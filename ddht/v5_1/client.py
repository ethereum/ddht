from contextlib import contextmanager, nullcontext
import logging
import socket
from typing import Iterator, Optional, Sequence

from async_service import Service
from eth_keys import keys
import trio

from ddht.abc import NodeDBAPI
from ddht.base_message import AnyInboundMessage, AnyOutboundMessage
from ddht.datagram import (
    DatagramReceiver,
    DatagramSender,
    InboundDatagram,
    OutboundDatagram,
)
from ddht.endpoint import Endpoint
from ddht.enr import ENR, partition_enrs
from ddht.enr_manager import ENRManager
from ddht.message_registry import MessageTypeRegistry
from ddht.typing import NodeID
from ddht.v5_1.abc import ClientAPI, EventsAPI
from ddht.v5_1.constants import FOUND_NODES_MAX_PAYLOAD_SIZE
from ddht.v5_1.dispatcher import Dispatcher
from ddht.v5_1.envelope import (
    EnvelopeDecoder,
    EnvelopeEncoder,
    InboundEnvelope,
    OutboundEnvelope,
)
from ddht.v5_1.events import Events
from ddht.v5_1.messages import (
    FindNodeMessage,
    FoundNodesMessage,
    PingMessage,
    PongMessage,
    RegisterTopicMessage,
    RegistrationConfirmationMessage,
    TalkRequestMessage,
    TalkResponseMessage,
    TicketMessage,
    TopicQueryMessage,
    v51_registry,
)
from ddht.v5_1.pool import Pool


class Client(Service, ClientAPI):
    logger = logging.getLogger("ddht.Client")

    def __init__(
        self,
        local_private_key: keys.PrivateKey,
        listen_on: Endpoint,
        node_db: NodeDBAPI,
        events: EventsAPI = None,
        message_type_registry: MessageTypeRegistry = v51_registry,
    ) -> None:
        self._local_private_key = local_private_key

        self.listen_on = listen_on
        self._listening = trio.Event()

        self.enr_manager = ENRManager(node_db=node_db, private_key=local_private_key,)
        self._node_db = node_db
        self._registry = message_type_registry

        # Datagrams
        (
            self._outbound_datagram_send_channel,
            self._outbound_datagram_receive_channel,
        ) = trio.open_memory_channel[OutboundDatagram](256)
        (
            self._inbound_datagram_send_channel,
            self._inbound_datagram_receive_channel,
        ) = trio.open_memory_channel[InboundDatagram](256)

        # EnvelopePair
        (
            self._outbound_envelope_send_channel,
            self._outbound_envelope_receive_channel,
        ) = trio.open_memory_channel[OutboundEnvelope](256)
        (
            self._inbound_envelope_send_channel,
            self._inbound_envelope_receive_channel,
        ) = trio.open_memory_channel[InboundEnvelope](256)

        # Messages
        (
            self._outbound_message_send_channel,
            self._outbound_message_receive_channel,
        ) = trio.open_memory_channel[AnyOutboundMessage](256)
        (
            self._inbound_message_send_channel,
            self._inbound_message_receive_channel,
        ) = trio.open_memory_channel[AnyInboundMessage](256)

        if events is None:
            events = Events()
        self.events = events

        self.pool = Pool(
            local_private_key=self._local_private_key,
            local_node_id=self.enr_manager.enr.node_id,
            node_db=self._node_db,
            outbound_envelope_send_channel=self._outbound_envelope_send_channel,
            inbound_message_send_channel=self._inbound_message_send_channel,
            message_type_registry=self._registry,
            events=self.events,
        )

        self.message_dispatcher = Dispatcher(
            self._inbound_envelope_receive_channel,
            self._inbound_message_receive_channel,
            self.pool,
            self._node_db,
            self._registry,
            self.events,
        )
        self.envelope_decoder = EnvelopeEncoder(
            self._outbound_envelope_receive_channel,
            self._outbound_datagram_send_channel,
        )
        self.envelope_encoder = EnvelopeDecoder(
            self._inbound_datagram_receive_channel,
            self._inbound_envelope_send_channel,
            self.enr_manager.enr.node_id,
        )

        self._ready = trio.Event()

    async def run(self) -> None:
        self.manager.run_daemon_child_service(self.message_dispatcher)

        self.manager.run_daemon_child_service(self.envelope_decoder)
        self.manager.run_daemon_child_service(self.envelope_encoder)

        self.manager.run_daemon_task(self._do_listen, self.listen_on)

        await self.manager.wait_finished()

    async def wait_listening(self) -> None:
        await self._listening.wait()

    async def _do_listen(self, listen_on: Endpoint) -> None:
        sock = trio.socket.socket(
            family=trio.socket.AF_INET, type=trio.socket.SOCK_DGRAM,
        )
        ip_address, port = listen_on
        await sock.bind((socket.inet_ntoa(ip_address), port))

        self._listening.set()
        await self.events.listening.trigger(listen_on)

        self.logger.debug("Network connection listening on %s", listen_on)

        # TODO: the datagram services need to use the `EventsAPI`
        datagram_sender = DatagramSender(self._outbound_datagram_receive_channel, sock)  # type: ignore  # noqa: E501
        self.manager.run_daemon_child_service(datagram_sender)

        datagram_receiver = DatagramReceiver(sock, self._inbound_datagram_send_channel)  # type: ignore  # noqa: E501
        self.manager.run_daemon_child_service(datagram_receiver)

        await self.manager.wait_finished()

    #
    # Message API
    #
    @contextmanager
    def get_request_id(
        self, node_id: NodeID, request_id: Optional[int] = None
    ) -> Iterator[int]:
        if request_id is None:
            request_id_context = self.message_dispatcher.reserve_request_id(node_id)
        else:
            request_id_context = nullcontext(request_id)

        with request_id_context as message_request_id:
            yield message_request_id

    async def send_ping(
        self,
        dest_endpoint: Endpoint,
        dest_node_id: NodeID,
        *,
        request_id: Optional[int] = None,
    ) -> int:
        with self.get_request_id(dest_node_id, request_id) as message_request_id:
            message = AnyOutboundMessage(
                PingMessage(message_request_id, self.enr_manager.enr.sequence_number),
                dest_endpoint,
                dest_node_id,
            )
            await self.message_dispatcher.send_message(message)

        return message_request_id

    async def send_pong(
        self, dest_endpoint: Endpoint, dest_node_id: NodeID, *, request_id: int,
    ) -> None:
        message = AnyOutboundMessage(
            PongMessage(
                request_id,
                self.enr_manager.enr.sequence_number,
                dest_endpoint.ip_address,
                dest_endpoint.port,
            ),
            dest_endpoint,
            dest_node_id,
        )
        await self.message_dispatcher.send_message(message)

    async def send_find_nodes(
        self,
        dest_endpoint: Endpoint,
        dest_node_id: NodeID,
        *,
        distance: int,
        request_id: Optional[int] = None,
    ) -> int:
        with self.get_request_id(dest_node_id, request_id) as message_request_id:
            message = AnyOutboundMessage(
                FindNodeMessage(message_request_id, distance),
                dest_endpoint,
                dest_node_id,
            )
            await self.message_dispatcher.send_message(message)

        return message_request_id

    async def send_found_nodes(
        self,
        dest_endpoint: Endpoint,
        dest_node_id: NodeID,
        *,
        enrs: Sequence[ENR],
        request_id: int,
    ) -> int:
        enr_batches = partition_enrs(
            enrs, max_payload_size=FOUND_NODES_MAX_PAYLOAD_SIZE
        )
        num_batches = len(enr_batches)
        for batch in enr_batches:
            message = AnyOutboundMessage(
                FoundNodesMessage(request_id, num_batches, batch,),
                dest_endpoint,
                dest_node_id,
            )
            await self.message_dispatcher.send_message(message)

        return num_batches

    async def send_talk_request(
        self,
        dest_endpoint: Endpoint,
        dest_node_id: NodeID,
        *,
        protocol: bytes,
        request: bytes,
        request_id: Optional[int] = None,
    ) -> int:
        with self.get_request_id(dest_node_id, request_id) as message_request_id:
            message = AnyOutboundMessage(
                TalkRequestMessage(message_request_id, protocol, request),
                dest_endpoint,
                dest_node_id,
            )
            await self.message_dispatcher.send_message(message)

        return message_request_id

    async def send_talk_response(
        self,
        dest_endpoint: Endpoint,
        dest_node_id: NodeID,
        *,
        response: bytes,
        request_id: int,
    ) -> None:
        message = AnyOutboundMessage(
            TalkResponseMessage(request_id, response,), dest_endpoint, dest_node_id,
        )
        await self.message_dispatcher.send_message(message)

    async def send_register_topic(
        self,
        dest_endpoint: Endpoint,
        dest_node_id: NodeID,
        *,
        topic: bytes,
        enr: ENR,
        ticket: bytes = b"",
        request_id: Optional[int] = None,
    ) -> int:
        with self.get_request_id(dest_node_id, request_id) as message_request_id:
            message = AnyOutboundMessage(
                RegisterTopicMessage(message_request_id, topic, enr, ticket),
                dest_endpoint,
                dest_node_id,
            )
            await self.message_dispatcher.send_message(message)

        return message_request_id

    async def send_ticket(
        self,
        dest_endpoint: Endpoint,
        dest_node_id: NodeID,
        *,
        ticket: bytes,
        wait_time: int,
        request_id: int,
    ) -> None:
        message = AnyOutboundMessage(
            TicketMessage(request_id, ticket, wait_time), dest_endpoint, dest_node_id,
        )
        await self.message_dispatcher.send_message(message)

    async def send_registration_confirmation(
        self,
        dest_endpoint: Endpoint,
        dest_node_id: NodeID,
        *,
        topic: bytes,
        request_id: int,
    ) -> None:
        message = AnyOutboundMessage(
            RegistrationConfirmationMessage(request_id, topic),
            dest_endpoint,
            dest_node_id,
        )
        await self.message_dispatcher.send_message(message)

    async def send_topic_query(
        self,
        dest_endpoint: Endpoint,
        dest_node_id: NodeID,
        *,
        topic: bytes,
        request_id: int,
    ) -> None:
        message = AnyOutboundMessage(
            TopicQueryMessage(request_id, topic), dest_endpoint, dest_node_id,
        )
        await self.message_dispatcher.send_message(message)
