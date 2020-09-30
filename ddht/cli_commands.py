import logging
import os

from async_service import ServiceAPI, background_trio_service, as_service, Service
import trio

from ddht.boot_info import BootInfo
from ddht.constants import ProtocolVersion
from ddht.v5.app import Application as ApplicationV5
from ddht.v5_1.app import Application as ApplicationV5_1

from ddht.v5.app import get_local_private_key
from eth_enr import ENRDB, default_identity_scheme_registry, ENRManager, ENR
from ddht.base_message import AnyInboundMessage, AnyOutboundMessage
from ddht.constants import DEFAULT_LISTEN
from ddht.datagram import send_datagram, InboundDatagram, DatagramReceiver, OutboundDatagram, DatagramSender
from ddht.v5.channel_services import InboundPacket, PacketDecoder, OutboundPacket, PacketEncoder
from ddht.v5.constants import DEFAULT_BOOTNODES
from ddht.v5.messages import PingMessage, v5_registry
from ddht.v5.message_dispatcher import MessageDispatcher
from ddht.v5.packer import Packer

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

    enr_db = ENRDB(dict(), default_identity_scheme_registry)
    local_private_key = get_local_private_key(boot_info)
    enr_manager = ENRManager(private_key=local_private_key, enr_db=enr_db)
    enr_manager.update((b"udp", boot_info.port))
    enr_manager.update((b"ip", listen_on.packed))

    logger.info(f"About to listen. bind={listen_on}:{boot_info.port}")
    logger.info(f"current enr. enr={enr_manager.enr}")
    await sock.bind((str(listen_on), boot_info.port))

    async def log_received_packets():
        async for packet in inbound_packet_channels[1]:
            logger.info(f"Received packet from {packet.sender_endpoint}: {packet}")

    # TOOD: is there a way to do this which involves less ceremony?
    #       I think the core benefit async_service provides is that it's trio & asyncio?

    class ListenService(Service):
        async def run(self) -> None:
            logger.info("starting listen service")
            for service in (datagram_receiver, packet_decoder):
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

    # Bind to a socket and try to bond with a single bootnode

    test_bootnode = DEFAULT_BOOTNODES[0]
    test_bootnode = "enr:-IS4QHoI1sHKaPmcfDU3m0KXBeoWK8uidDvvnUOy01oigUs3a0HQjx2U701KvBG0Dg5OAsU3A5f6W2CvdZEIGFXcxhgDgmlkgnY0gmlwhAAAAACJc2VjcDI1NmsxoQLgn7ZgocyVfD0cUxGpfI_flyR-Oe8GNqw1V9MeWwxKC4N1ZHCCdl8"
    logger.info(f"Attempting to bond with: {test_bootnode}")
    bootnode_enr = ENR.from_repr(test_bootnode, default_identity_scheme_registry)

    # datagram = packet.to_wire_bytes()
    # send_datagram(sock, datagram, endpoint)

    # you need a Packer, to handle handshakes...
    # a Packer requires an ENRDatabaseAPI...
        # I might actually want to use a database, if you re-crawl it might be nice not to
        # have to handshake all over again?
    # also a message type registry?

    enr_db = ENRDB(dict(), default_identity_scheme_registry)
    enr_db.set_enr(bootnode_enr)

    outbound_datagram_channels = trio.open_memory_channel[OutboundDatagram](0)
    inbound_datagram_channels = trio.open_memory_channel[InboundDatagram](0)
    outbound_packet_channels = trio.open_memory_channel[OutboundPacket](0)
    inbound_packet_channels = trio.open_memory_channel[InboundPacket](0)
    outbound_message_channels = trio.open_memory_channel[AnyOutboundMessage](0)
    inbound_message_channels = trio.open_memory_channel[AnyInboundMessage](0)

    datagram_sender = DatagramSender(  # type: ignore
        outbound_datagram_channels[1], sock
    )
    datagram_receiver = DatagramReceiver(  # type: ignore
        sock, inbound_datagram_channels[0]
    )

    packet_encoder = PacketEncoder(  # type: ignore
        outbound_packet_channels[1], outbound_datagram_channels[0]
    )
    packet_decoder = PacketDecoder(  # type: ignore
        inbound_datagram_channels[1], inbound_packet_channels[0]
    )

    local_private_key = get_local_private_key(boot_info)
    enr_manager = ENRManager(private_key=local_private_key, enr_db=enr_db)
    enr_manager.update((b"udp", boot_info.port))
    packer = Packer(
        local_private_key=local_private_key.to_bytes(),
        local_node_id=enr_manager.enr.node_id,
        enr_db=enr_db,
        message_type_registry=v5_registry,
        inbound_packet_receive_channel=inbound_packet_channels[1],
        inbound_message_send_channel=inbound_message_channels[0],
        outbound_message_receive_channel=outbound_message_channels[1],
        outbound_packet_send_channel=outbound_packet_channels[0],
    )

    message_dispatcher = MessageDispatcher(
        enr_db=enr_db,
        inbound_message_receive_channel=inbound_message_channels[1],
        outbound_message_send_channel=outbound_message_channels[0],
    )

    services = (
        packet_encoder, datagram_sender,
        datagram_receiver, packet_decoder,
        packer,
        message_dispatcher,
    )

    async def ping_bootnode():
        ping = PingMessage(request_id=0, enr_seq=0)

        try:
            logger.info(f"Sending ping to remote node.")
            with trio.fail_after(10):
                resp = await message_dispatcher.request(
                    bootnode_enr.node_id,
                    ping
                )
                logger.info(f"Received response. resp={resp}")
        except trio.TooSlowError:
            logger.error("No response from bootnode")

    class CrawlService(Service):
        async def run(self) -> None:
            for service in services:
                self.manager.run_daemon_child_service(service)
            self.manager.run_daemon_task(ping_bootnode)
            await self.manager.wait_finished()

    listen_on = boot_info.listen_on or DEFAULT_LISTEN
    logger.info(f"About to listen. bind={listen_on}:{boot_info.port}")
    await sock.bind(("0.0.0.0", boot_info.port))

    with sock:
        async with background_trio_service(CrawlService()) as manager:
            await manager.wait_finished()
