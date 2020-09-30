import logging
import os

from async_service import ServiceAPI, background_trio_service, as_service, Service
import trio

from ddht.boot_info import BootInfo
from ddht.constants import ProtocolVersion
from ddht.v5.app import Application as ApplicationV5
from ddht.v5_1.app import Application as ApplicationV5_1

from ddht.constants import DEFAULT_LISTEN
from ddht.datagram import send_datagram, InboundDatagram, DatagramReceiver
from ddht.v5.channel_services import InboundPacket, PacketDecoder
from ddht.v5.constants import DEFAULT_BOOTNODES
from ddht.v5.messages import PingMessage

logger = logging.getLogger("ddht")


async def do_main(boot_info: BootInfo) -> None:
    app: ServiceAPI
    if boot_info.protocol_version is ProtocolVersion.v5:
        app = ApplicationV5(boot_info)
    elif boot_info.protocol_version is ProtocolVersion.v5_1:
        app = ApplicationV5_1(boot_info)
    else:
        raise Exception(f"Unsupported protocol version: {boot_info.protocol_version}")

    logger.info("Started main process (pid=%d)", os.getpid())
    async with background_trio_service(app) as manager:
        await manager.wait_finished()


async def do_listen(boot_info: BootInfo) -> None:
    # TODO: respect boot_info.protocol_version

    sock = trio.socket.socket(
        family=trio.socket.AF_INET, type=trio.socket.SOCK_DGRAM
    )

    inbound_datagram_channels = trio.open_memory_channel[InboundDatagram](0)
    inbound_packet_channels = trio.open_memory_channel[InboundPacket](0)

    datagram_receiver = DatagramReceiver(  # type: ignore
        sock, inbound_datagram_channels[0]
    )

    packet_decoder = PacketDecoder(  # type: ignore
        inbound_datagram_channels[1], inbound_packet_channels[0]
    )

    listen_on = boot_info.listen_on or DEFAULT_LISTEN
    logger.info(f"About to listen. bind={listen_on}:{boot_info.port}")
    await sock.bind((str(listen_on), boot_info.port))

    async def log_received_packets():
        async for packet in inbound_packet_channels[1]:
            logger.info(f"Received packet from {packet.sender_endpoint}: {packet}")

    class ListenService(Service):
        async def run(self) -> None:
            logger.info("starting listen service")
            services = [
                datagram_receiver, packet_decoder, # as_service(log_received_packets)
            ]
            for service in services:
                self.manager.run_daemon_child_service(service)
            self.manager.run_daemon_task(log_received_packets)
            await self.manager.wait_finished()

    with sock:
        async with background_trio_service(ListenService()) as manager:
            await manager.wait_finished()


# TODO: This should probably delegate to either CrawlV5, or CrawlV5_1
async def do_crawl(boot_info: BootInfo) -> None:
    logger.info("Crawling!")

    # TODO: use these...
    boot_info.port: int
    boot_info.bootnodes: Tuple[ENRAPI]
    boot_info.private_key: Optional[keys.PrivateKey]
    boot_info.listen_on

    # TODO: Use v5 or v5.1 where appropriate
    # TODO: support UPNP?

    sock = trio.socket.socket(
        family=trio.socket.AF_INET, type=trio.socket.SOCK_DGRAM
    )
    await sock.bind(("0.0.0.0", 30306))

    # Bind to a socket and try to bond with a single bootnode

    test_bootnode = DEFAULT_BOOTNODES[0]
    logger.info(f"Attempting to bond with: {test_bootnode}")

    ping = PingMessage(request_id=0, enr_seq=0)

    # datagram = packet.to_wire_bytes()
    # send_datagram(sock, datagram, endpoint)

    # you need a Packer, to handle handshakes...
    # a Packer requires an ENRDatabaseAPI...
        # I might actually want to use a database, if you re-crawl it might be nice not to
        # have to handshake all over again?
    # also a message type registry?

    try:
        with trio.fail_after(10):
            pass
            # inbound_message = await self.message_dispatcher.request(node_id, ping)
    except trio.TooSlowError:
        logger.error("No response from bootnode")
