import ipaddress
import logging
from typing import Tuple
from urllib.parse import urlparse

from async_service import Service, external_trio_api
import netifaces
import trio
import upnpclient

from ddht._utils import every
from ddht.typing import AnyIPAddress


#
# Exceptions used internally that should not ever propogate out of this
# service.
#
class NoInternalAddressMatchesDevice(Exception):
    pass


class PortMapFailed(Exception):
    pass


class WANServiceNotFound(Exception):
    pass


# UPnP discovery can take a long time, so use a loooong timeout here.
UPNP_DISCOVER_TIMEOUT_SECONDS = 30
UPNP_PORTMAP_DURATION = 30 * 60  # 30 minutes


logger = logging.getLogger("ddht.upnp")


class UPnPService(Service):
    logger = logger
    _internal_ip: AnyIPAddress
    _external_ip: AnyIPAddress

    def __init__(self, port: int) -> None:
        """
        :param port: The port that a server wants to bind to on this machine, and
        make publicly accessible.
        """
        self.port = port
        self._has_ip_addresses = trio.Event()
        self._ip_changed = trio.Condition()

    @external_trio_api
    async def get_ip_addresses(self) -> Tuple[AnyIPAddress, AnyIPAddress]:
        await self._has_ip_addresses.wait()
        return (self._internal_ip, self._external_ip)

    @external_trio_api
    async def wait_ip_changed(self) -> Tuple[AnyIPAddress, AnyIPAddress]:
        async with self._ip_changed:
            await self._ip_changed.wait()
        return (self._internal_ip, self._external_ip)

    async def run(self) -> None:
        """
        Run an infinite loop refreshing our NAT port mapping.

        On every iteration we configure the port mapping with a lifetime of 30 minutes and then
        sleep for that long as well.
        """
        while self.manager.is_running:
            async for _ in every(UPNP_PORTMAP_DURATION):
                with trio.move_on_after(UPNP_DISCOVER_TIMEOUT_SECONDS) as scope:
                    try:
                        internal_ip, external_ip = await trio.to_thread.run_sync(
                            setup_port_map, self.port, UPNP_PORTMAP_DURATION,
                        )
                        self._external_ip = external_ip
                        self._internal_ip = internal_ip
                        self._has_ip_addresses.set()
                        async with self._ip_changed:
                            self._ip_changed.notify_all()

                        self.logger.debug(
                            "NAT portmap created: internal=%s  external=%s",
                            internal_ip,
                            external_ip,
                        )
                    except PortMapFailed as err:
                        self.logger.error("Failed to setup NAT portmap: %s", err)
                    except Exception:
                        self.logger.exception("Error setuping NAT portmap")

                if scope.cancelled_caught:
                    self.logger.error("Timeout attempting to setup UPnP port map")


def setup_port_map(
    port: int, duration: int = UPNP_PORTMAP_DURATION
) -> Tuple[AnyIPAddress, AnyIPAddress]:
    """
    Set up the port mapping

    :return: the IP address of the new mapping (or None if failed)
    """
    devices = upnpclient.discover()
    if not devices:
        raise PortMapFailed("No UPnP devices available")

    for upnp_dev in devices:
        try:
            internal_ip, external_ip = setup_device_port_map(upnp_dev, port, duration)
            logger.info(
                "NAT port forwarding successfully set up: internal=%s:%d external=%s:%d",
                internal_ip,
                port,
                external_ip,
                port,
            )
            break
        except NoInternalAddressMatchesDevice:
            logger.debug(
                "No internal addresses were managed by the UPnP device at %s",
                upnp_dev.location,
            )
            continue
        except WANServiceNotFound:
            logger.debug(
                "No WAN services managed by the UPnP device at %s", upnp_dev.location,
            )
            continue
        except PortMapFailed:
            logger.debug(
                "Failed to setup portmap on UPnP divec at %s",
                upnp_dev.location,
                exc_info=True,
            )
            continue
    else:
        logger.info("Failed to setup NAT portmap.  Tried %d devices", len(devices))
        raise PortMapFailed(
            f"Failed to setup NAT portmap.  Tried {len(devices)} devices."
        )

    return ipaddress.ip_address(internal_ip), ipaddress.ip_address(external_ip)


def find_internal_ip_on_device_network(upnp_dev: upnpclient.upnp.Device) -> str:
    """
    For a given UPnP device, return the internal IP address of this host machine that can
    be used for a NAT mapping.
    """
    parsed_url = urlparse(upnp_dev.location)
    # Get an ipaddress.IPv4Network instance for the upnp device's network.
    upnp_dev_net = ipaddress.ip_network(parsed_url.hostname + "/24", strict=False)
    for iface in netifaces.interfaces():
        for family, addresses in netifaces.ifaddresses(iface).items():
            # TODO: Support IPv6 addresses as well.
            if family != netifaces.AF_INET:
                continue
            for item in addresses:
                if ipaddress.ip_address(item["addr"]) in upnp_dev_net:
                    return str(item["addr"])
    raise NoInternalAddressMatchesDevice(parsed_url.hostname)


WAN_SERVICE_NAMES = (
    "WANIPConn1",
    "WANIPConnection.1",  # Nighthawk C7800
    "WANPPPConnection.1",  # CenturyLink C1100Z
)


def get_wan_service(upnp_dev: upnpclient.upnp.Device) -> upnpclient.upnp.Service:
    for service_name in WAN_SERVICE_NAMES:
        try:
            return upnp_dev[service_name]
        except KeyError:
            continue
    else:
        raise WANServiceNotFound()


def setup_device_port_map(
    upnp_dev: upnpclient.upnp.Device, port: int, duration: int
) -> Tuple[str, str]:
    internal_ip = find_internal_ip_on_device_network(upnp_dev)
    wan_service = get_wan_service(upnp_dev)

    external_ip = wan_service.GetExternalIPAddress()["NewExternalIPAddress"]

    for protocol in ("UDP", "TCP"):
        try:
            wan_service.AddPortMapping(
                NewRemoteHost=external_ip,
                NewExternalPort=port,
                NewProtocol=protocol,
                NewInternalPort=port,
                NewInternalClient=internal_ip,
                NewEnabled="1",
                NewPortMappingDescription=f"ddht[{protocol}]",
                NewLeaseDuration=duration,
            )
        except upnpclient.soap.SOAPError as exc:
            if exc.args == (718, "ConflictInMappingEntry"):
                # An entry already exists with the parameters we specified. Maybe the router
                # didn't clean it up after it expired or it has been configured by other piece
                # of software, either way we should not override it.
                # https://tools.ietf.org/id/draft-ietf-pcp-upnp-igd-interworking-07.html#errors
                logger.debug("NAT port mapping already configured, not overriding it")
                continue
            else:
                logger.debug(
                    "Failed to setup NAT portmap on device: %s", upnp_dev.location,
                )
                raise PortMapFailed from exc

    return internal_ip, external_ip
