import logging
from typing import Tuple

from async_service import Service, external_trio_api
import trio
from upnp_port_forward import PortMapFailed, setup_port_map

from ddht._utils import every
from ddht.typing import AnyIPAddress

# UPnP discovery can take a long time, so use a loooong timeout here.
UPNP_DISCOVER_TIMEOUT_SECONDS = 30
UPNP_PORTMAP_DURATION = 30 * 60  # 30 minutes


class UPnPService(Service):
    logger = logging.getLogger("ddht.upnp")
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
