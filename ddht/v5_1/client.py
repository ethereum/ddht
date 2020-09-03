from contextlib import contextmanager, nullcontext
import logging
import socket
from typing import Collection, ContextManager, Iterator, List, Optional, Sequence, Tuple

from async_service import Service
from eth_enr import ENRAPI, ENRDatabaseAPI, ENRManager
from eth_keys import keys
from eth_typing import NodeID
from eth_utils import ValidationError
import trio

from ddht.base_message import AnyInboundMessage, AnyOutboundMessage, InboundMessage
from ddht.datagram import (
    DatagramReceiver,
    DatagramSender,
    InboundDatagram,
    OutboundDatagram,
)
from ddht.endpoint import Endpoint
from ddht.enr import partition_enrs
from ddht.kademlia import compute_log_distance
from ddht.message_registry import MessageTypeRegistry
from ddht.v5_1.abc import ClientAPI, EventsAPI
from ddht.v5_1.constants import FOUND_NODES_MAX_PAYLOAD_SIZE, REQUEST_RESPONSE_TIMEOUT
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
        enr_db: ENRDatabaseAPI,
        events: EventsAPI = None,
        message_type_registry: MessageTypeRegistry = v51_registry,
    ) -> None:
        self._local_private_key = local_private_key

        self.listen_on = listen_on
        self._listening = trio.Event()

        self.enr_manager = ENRManager(private_key=local_private_key, enr_db=enr_db,)
        self.enr_db = enr_db
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
            enr_db=self.enr_db,
            outbound_envelope_send_channel=self._outbound_envelope_send_channel,
            inbound_message_send_channel=self._inbound_message_send_channel,
            message_type_registry=self._registry,
            events=self.events,
        )

        self.dispatcher = Dispatcher(
            self._inbound_envelope_receive_channel,
            self._inbound_message_receive_channel,
            self.pool,
            self.enr_db,
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

    @property
    def local_node_id(self) -> NodeID:
        return self.pool.local_node_id

    async def run(self) -> None:
        self.manager.run_daemon_child_service(self.dispatcher)

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
    def _get_request_id(
        self, node_id: NodeID, request_id: Optional[int] = None
    ) -> Iterator[int]:
        request_id_context: ContextManager[int]

        if request_id is None:
            request_id_context = self.dispatcher.reserve_request_id(node_id)
        else:
            request_id_context = nullcontext(request_id)

        with request_id_context as message_request_id:
            yield message_request_id

    async def send_ping(
        self, endpoint: Endpoint, node_id: NodeID, *, request_id: Optional[int] = None,
    ) -> int:
        with self._get_request_id(node_id, request_id) as message_request_id:
            message = AnyOutboundMessage(
                PingMessage(message_request_id, self.enr_manager.enr.sequence_number),
                endpoint,
                node_id,
            )
            await self.dispatcher.send_message(message)

        return message_request_id

    async def send_pong(
        self, endpoint: Endpoint, node_id: NodeID, *, request_id: int,
    ) -> None:
        message = AnyOutboundMessage(
            PongMessage(
                request_id,
                self.enr_manager.enr.sequence_number,
                endpoint.ip_address,
                endpoint.port,
            ),
            endpoint,
            node_id,
        )
        await self.dispatcher.send_message(message)

    async def send_find_nodes(
        self,
        endpoint: Endpoint,
        node_id: NodeID,
        *,
        distances: Collection[int],
        request_id: Optional[int] = None,
    ) -> int:
        with self._get_request_id(node_id, request_id) as message_request_id:
            message = AnyOutboundMessage(
                FindNodeMessage(message_request_id, tuple(distances)),
                endpoint,
                node_id,
            )
            await self.dispatcher.send_message(message)

        return message_request_id

    async def send_found_nodes(
        self,
        endpoint: Endpoint,
        node_id: NodeID,
        *,
        enrs: Sequence[ENRAPI],
        request_id: int,
    ) -> int:
        enr_batches = partition_enrs(
            enrs, max_payload_size=FOUND_NODES_MAX_PAYLOAD_SIZE
        )
        num_batches = len(enr_batches)
        for batch in enr_batches:
            message = AnyOutboundMessage(
                FoundNodesMessage(request_id, num_batches, batch,), endpoint, node_id,
            )
            await self.dispatcher.send_message(message)

        return num_batches

    async def send_talk_request(
        self,
        endpoint: Endpoint,
        node_id: NodeID,
        *,
        protocol: bytes,
        request: bytes,
        request_id: Optional[int] = None,
    ) -> int:
        with self._get_request_id(node_id, request_id) as message_request_id:
            message = AnyOutboundMessage(
                TalkRequestMessage(message_request_id, protocol, request),
                endpoint,
                node_id,
            )
            await self.dispatcher.send_message(message)

        return message_request_id

    async def send_talk_response(
        self, endpoint: Endpoint, node_id: NodeID, *, response: bytes, request_id: int,
    ) -> None:
        message = AnyOutboundMessage(
            TalkResponseMessage(request_id, response,), endpoint, node_id,
        )
        await self.dispatcher.send_message(message)

    async def send_register_topic(
        self,
        endpoint: Endpoint,
        node_id: NodeID,
        *,
        topic: bytes,
        enr: ENRAPI,
        ticket: bytes = b"",
        request_id: Optional[int] = None,
    ) -> int:
        with self._get_request_id(node_id, request_id) as message_request_id:
            message = AnyOutboundMessage(
                RegisterTopicMessage(message_request_id, topic, enr, ticket),
                endpoint,
                node_id,
            )
            await self.dispatcher.send_message(message)

        return message_request_id

    async def send_ticket(
        self,
        endpoint: Endpoint,
        node_id: NodeID,
        *,
        ticket: bytes,
        wait_time: int,
        request_id: int,
    ) -> None:
        message = AnyOutboundMessage(
            TicketMessage(request_id, ticket, wait_time), endpoint, node_id,
        )
        await self.dispatcher.send_message(message)

    async def send_registration_confirmation(
        self, endpoint: Endpoint, node_id: NodeID, *, topic: bytes, request_id: int,
    ) -> None:
        message = AnyOutboundMessage(
            RegistrationConfirmationMessage(request_id, topic), endpoint, node_id,
        )
        await self.dispatcher.send_message(message)

    async def send_topic_query(
        self,
        endpoint: Endpoint,
        node_id: NodeID,
        *,
        topic: bytes,
        request_id: Optional[int] = None,
    ) -> int:
        with self._get_request_id(node_id, request_id) as message_request_id:
            message = AnyOutboundMessage(
                TopicQueryMessage(message_request_id, topic), endpoint, node_id,
            )
            await self.dispatcher.send_message(message)
        return message_request_id

    #
    # Request/Response API
    #
    async def ping(
        self, endpoint: Endpoint, node_id: NodeID
    ) -> InboundMessage[PongMessage]:
        with self._get_request_id(node_id) as request_id:
            request = AnyOutboundMessage(
                PingMessage(request_id, self.enr_manager.enr.sequence_number),
                endpoint,
                node_id,
            )
            async with self.dispatcher.subscribe_request(
                request, PongMessage
            ) as subscription:
                with trio.fail_after(REQUEST_RESPONSE_TIMEOUT):
                    return await subscription.receive()

    async def find_nodes(
        self, endpoint: Endpoint, node_id: NodeID, distances: Collection[int],
    ) -> Tuple[InboundMessage[FoundNodesMessage], ...]:
        with self._get_request_id(node_id) as request_id:
            request = AnyOutboundMessage(
                FindNodeMessage(request_id, tuple(distances)), endpoint, node_id,
            )
            async with self.dispatcher.subscribe_request(
                request, FoundNodesMessage
            ) as subscription:
                with trio.fail_after(REQUEST_RESPONSE_TIMEOUT):
                    head_response = await subscription.receive()
                    total = head_response.message.total
                    responses: Tuple[InboundMessage[FoundNodesMessage], ...]
                    if total == 1:
                        responses = (head_response,)
                    elif total > 1:
                        tail_responses: List[InboundMessage[FoundNodesMessage]] = []
                        for _ in range(total - 1):
                            tail_responses.append(await subscription.receive())
                        responses = (head_response,) + tuple(tail_responses)
                    else:
                        # TODO: this code path needs to be excercised and
                        # probably replaced with some sort of
                        # `SessionTerminated` exception.
                        raise Exception("Invalid `total` counter in response")

                # Validate that all responses are indeed at one of the
                # specified distances.
                for response in responses:
                    for enr in response.message.enrs:
                        if enr.node_id == node_id:
                            if 0 not in distances:
                                raise ValidationError(
                                    f"Invalid response: distance=0  expected={distances}"
                                )
                        else:
                            distance = compute_log_distance(enr.node_id, node_id)
                            if distance not in distances:
                                raise ValidationError(
                                    f"Invalid response: distance={distance}  expected={distances}"
                                )

                return responses

    async def talk(
        self, endpoint: Endpoint, node_id: NodeID, protocol: bytes, request: bytes
    ) -> InboundMessage[TalkResponseMessage]:
        raise NotImplementedError

    async def register_topic(
        self,
        endpoint: Endpoint,
        node_id: NodeID,
        topic: bytes,
        ticket: Optional[bytes] = None,
    ) -> Tuple[
        InboundMessage[TicketMessage],
        Optional[InboundMessage[RegistrationConfirmationMessage]],
    ]:
        raise NotImplementedError

    async def topic_query(
        self, endpoint: Endpoint, node_id: NodeID, topic: bytes
    ) -> InboundMessage[FoundNodesMessage]:
        raise NotImplementedError
