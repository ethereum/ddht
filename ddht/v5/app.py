import logging

from async_service import Service, run_trio_service
from eth.db.backends.level import LevelDB
from eth_enr import ENRDB, ENRManager, default_identity_scheme_registry
from eth_enr.exceptions import OldSequenceNumber
from eth_keys import keys
from eth_utils import encode_hex
import trio

from ddht._utils import generate_node_key_file, read_node_key_file
from ddht.base_message import AnyInboundMessage, AnyOutboundMessage
from ddht.boot_info import BootInfo
from ddht.constants import (
    DEFAULT_LISTEN,
    IP_V4_ADDRESS_ENR_KEY,
    ROUTING_TABLE_BUCKET_SIZE,
)
from ddht.datagram import (
    DatagramReceiver,
    DatagramSender,
    InboundDatagram,
    OutboundDatagram,
)
from ddht.kademlia import KademliaRoutingTable
from ddht.typing import AnyIPAddress
from ddht.upnp import UPnPService
from ddht.v5.channel_services import (
    InboundPacket,
    OutboundPacket,
    PacketDecoder,
    PacketEncoder,
)
from ddht.v5.endpoint_tracker import EndpointTracker, EndpointVote
from ddht.v5.message_dispatcher import MessageDispatcher
from ddht.v5.messages import v5_registry
from ddht.v5.packer import Packer
from ddht.v5.routing_table_manager import RoutingTableManager

logger = logging.getLogger("ddht.DDHT")


ENR_DATABASE_DIR_NAME = "enr-db"


def get_local_private_key(boot_info: BootInfo) -> keys.PrivateKey:
    if boot_info.private_key is None:
        # load from disk or generate
        node_key_file_path = boot_info.base_dir / "nodekey"
        if not node_key_file_path.exists():
            generate_node_key_file(node_key_file_path)
        return read_node_key_file(node_key_file_path)
    else:
        return boot_info.private_key


class Application(Service):
    logger = logger
    _boot_info: BootInfo

    def __init__(self, boot_info: BootInfo) -> None:
        self._boot_info = boot_info

    async def run(self) -> None:
        identity_scheme_registry = default_identity_scheme_registry
        message_type_registry = v5_registry

        enr_database_dir = self._boot_info.base_dir / ENR_DATABASE_DIR_NAME
        enr_database_dir.mkdir(exist_ok=True)
        enr_db = ENRDB(LevelDB(enr_database_dir), identity_scheme_registry)

        local_private_key = get_local_private_key(self._boot_info)

        enr_manager = ENRManager(private_key=local_private_key, enr_db=enr_db,)

        port = self._boot_info.port

        if b"udp" not in enr_manager.enr:
            enr_manager.update((b"udp", port))

        listen_on: AnyIPAddress
        if self._boot_info.listen_on is None:
            listen_on = DEFAULT_LISTEN
        else:
            listen_on = self._boot_info.listen_on
            # Update the ENR if an explicit listening address was provided
            enr_manager.update((IP_V4_ADDRESS_ENR_KEY, listen_on.packed))

        if self._boot_info.is_upnp_enabled:
            upnp_service = UPnPService(port)
            self.manager.run_daemon_child_service(upnp_service)

        routing_table = KademliaRoutingTable(
            enr_manager.enr.node_id, ROUTING_TABLE_BUCKET_SIZE
        )

        for enr in self._boot_info.bootnodes:
            try:
                enr_db.set_enr(enr)
            except OldSequenceNumber:
                pass
            routing_table.update(enr.node_id)

        sock = trio.socket.socket(
            family=trio.socket.AF_INET, type=trio.socket.SOCK_DGRAM
        )
        outbound_datagram_channels = trio.open_memory_channel[OutboundDatagram](0)
        inbound_datagram_channels = trio.open_memory_channel[InboundDatagram](0)
        outbound_packet_channels = trio.open_memory_channel[OutboundPacket](0)
        inbound_packet_channels = trio.open_memory_channel[InboundPacket](0)
        outbound_message_channels = trio.open_memory_channel[AnyOutboundMessage](0)
        inbound_message_channels = trio.open_memory_channel[AnyInboundMessage](0)
        endpoint_vote_channels = trio.open_memory_channel[EndpointVote](0)

        # types ignored due to https://github.com/ethereum/async-service/issues/5
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

        packer = Packer(
            local_private_key=local_private_key.to_bytes(),
            local_node_id=enr_manager.enr.node_id,
            enr_db=enr_db,
            message_type_registry=message_type_registry,
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

        endpoint_tracker = EndpointTracker(
            local_private_key=local_private_key.to_bytes(),
            local_node_id=enr_manager.enr.node_id,
            enr_db=enr_db,
            identity_scheme_registry=identity_scheme_registry,
            vote_receive_channel=endpoint_vote_channels[1],
        )

        routing_table_manager = RoutingTableManager(
            local_node_id=enr_manager.enr.node_id,
            routing_table=routing_table,
            message_dispatcher=message_dispatcher,
            enr_db=enr_db,
            outbound_message_send_channel=outbound_message_channels[0],
            endpoint_vote_send_channel=endpoint_vote_channels[0],
        )

        logger.info(f"DDHT base dir: {self._boot_info.base_dir}")
        logger.info("Starting discovery service...")
        logger.info(f"Listening on {listen_on}:{port}")
        logger.info(f"Local Node ID: {encode_hex(enr_manager.enr.node_id)}")
        logger.info(f"Local ENR: {enr_manager.enr}")

        services = (
            datagram_sender,
            datagram_receiver,
            packet_encoder,
            packet_decoder,
            packer,
            message_dispatcher,
            endpoint_tracker,
            routing_table_manager,
        )
        await sock.bind((str(listen_on), port))
        with sock:
            async with trio.open_nursery() as nursery:
                for service in services:
                    nursery.start_soon(run_trio_service, service)
