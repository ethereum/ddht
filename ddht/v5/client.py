import logging

from async_service import Service
from eth_enr import ENRDatabaseAPI
from eth_keys import keys
import trio

from ddht.base_message import AnyInboundMessage, AnyOutboundMessage
from ddht.datagram import (
    DatagramReceiver,
    DatagramSender,
    InboundDatagram,
    OutboundDatagram,
)
from ddht.v5.channel_services import (
    InboundPacket,
    OutboundPacket,
    PacketDecoder,
    PacketEncoder,
)
from ddht.v5.message_dispatcher import MessageDispatcher
from ddht.v5.messages import v5_registry
from ddht.v5.packer import Packer


class Client(Service):
    logger = logging.getLogger("ddht.Client")

    def __init__(
        self, local_private_key: keys.PrivateKey, enr_db: ENRDatabaseAPI, node_id, sock
    ) -> None:

        self.enr_db = enr_db

        outbound_datagram_channels = trio.open_memory_channel[OutboundDatagram](0)
        inbound_datagram_channels = trio.open_memory_channel[InboundDatagram](0)
        outbound_packet_channels = trio.open_memory_channel[OutboundPacket](0)
        inbound_packet_channels = trio.open_memory_channel[InboundPacket](0)
        outbound_message_channels = trio.open_memory_channel[AnyOutboundMessage](0)
        inbound_message_channels = trio.open_memory_channel[AnyInboundMessage](0)

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

        self.packer = Packer(
            local_private_key=local_private_key.to_bytes(),
            local_node_id=node_id,
            enr_db=self.enr_db,
            message_type_registry=v5_registry,
            inbound_packet_receive_channel=inbound_packet_channels[1],
            inbound_message_send_channel=inbound_message_channels[0],
            outbound_message_receive_channel=outbound_message_channels[1],
            outbound_packet_send_channel=outbound_packet_channels[0],
        )

        self.message_dispatcher = MessageDispatcher(
            enr_db=self.enr_db,
            inbound_message_receive_channel=inbound_message_channels[1],
            outbound_message_send_channel=outbound_message_channels[0],
        )

        self.services = (
            packet_encoder,
            datagram_sender,
            datagram_receiver,
            packet_decoder,
            self.packer,
            self.message_dispatcher,
        )

        self.outbound_message_send_channel = outbound_message_channels[0]

    def discard_peer(self, remote_node_id) -> None:
        """
        Signals that we intend not to send any more messages to the remote peer, stops the
        service associated with that peer.
        """
        if remote_node_id in self.packer.managed_peer_packers:
            self.packer.managed_peer_packers[remote_node_id].manager.cancel()

    async def run(self) -> None:
        for service in self.services:
            self.manager.run_daemon_child_service(service)
        await self.manager.wait_finished()
