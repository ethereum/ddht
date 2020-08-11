from contextlib import contextmanager, nullcontext
import logging
import socket
from typing import Iterator, Optional

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
from ddht.enr_manager import ENRManager
from ddht.message_registry import MessageTypeRegistry
from ddht.typing import NodeID
from ddht.v5_1.abc import ClientAPI, EventsAPI
from ddht.v5_1.dispatcher import Dispatcher
from ddht.v5_1.envelope import (
    EnvelopeDecoder,
    EnvelopeEncoder,
    InboundEnvelope,
    OutboundEnvelope,
)
from ddht.v5_1.events import Events
from ddht.v5_1.messages import PingMessage, PongMessage, v51_registry
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
        if dest_node_id == self.enr_manager.enr.node_id:
            raise ValueError("Cannot send to self")

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
        if dest_node_id == self.enr_manager.enr.node_id:
            raise ValueError("Cannot send to self")

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
