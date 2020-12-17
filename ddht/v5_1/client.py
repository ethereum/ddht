import logging
import socket
from typing import (
    AsyncContextManager,
    AsyncIterator,
    Collection,
    List,
    Optional,
    Sequence,
    Tuple,
    Type,
)

from async_generator import asynccontextmanager
from async_service import Service, background_trio_service
from eth_enr import ENRAPI, ENRManager, QueryableENRDatabaseAPI
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
from ddht.message_registry import MessageTypeRegistry
from ddht.request_tracker import RequestTracker
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
    BaseMessage,
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
from ddht.validation import validate_found_nodes_distances


class Client(Service, ClientAPI):
    logger = logging.getLogger("ddht.Client")

    def __init__(
        self,
        local_private_key: keys.PrivateKey,
        listen_on: Endpoint,
        enr_db: QueryableENRDatabaseAPI,
        session_cache_size: int,
        events: EventsAPI = None,
        message_type_registry: MessageTypeRegistry = v51_registry,
    ) -> None:
        self.local_private_key = local_private_key

        self.listen_on = listen_on
        self._listening = trio.Event()

        self.enr_manager = ENRManager(private_key=local_private_key, enr_db=enr_db,)
        self.enr_db = enr_db
        self._registry = message_type_registry

        self.request_tracker = RequestTracker()

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
            local_private_key=self.local_private_key,
            local_node_id=self.enr_manager.enr.node_id,
            enr_db=self.enr_db,
            outbound_envelope_send_channel=self._outbound_envelope_send_channel,
            inbound_message_send_channel=self._inbound_message_send_channel,
            session_cache_size=session_cache_size,
            message_type_registry=self._registry,
            events=self.events,
        )

        self.dispatcher = Dispatcher(
            self._inbound_envelope_receive_channel,
            self._inbound_message_receive_channel,
            self.pool,
            self.enr_db,
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
        self.manager.run_daemon_task(self._run_envelope_and_dispatcher_services)
        self.manager.run_daemon_task(self._do_listen, self.listen_on)

        await self.manager.wait_finished()

    async def _run_envelope_and_dispatcher_services(self) -> None:
        """
        Ensure that in the task hierarchy the envelope encode will be shut down
        *after* the dispatcher.

        run()
          |
          ---EnvelopeEncoder
                |
                ---EnvelopeDecoder
                      |
                      ---Dispatcher
        """
        async with background_trio_service(self.envelope_encoder):
            async with background_trio_service(self.envelope_decoder):
                async with background_trio_service(self.dispatcher):
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
    async def send_ping(
        self,
        node_id: NodeID,
        endpoint: Endpoint,
        *,
        enr_seq: Optional[int] = None,
        request_id: Optional[bytes] = None,
    ) -> bytes:
        if enr_seq is None:
            enr_seq = self.enr_manager.enr.sequence_number

        with self.request_tracker.reserve_request_id(
            node_id, request_id
        ) as message_request_id:
            message = AnyOutboundMessage(
                PingMessage(message_request_id, enr_seq), endpoint, node_id,
            )
            await self.dispatcher.send_message(message)

        return message_request_id

    async def send_pong(
        self,
        node_id: NodeID,
        endpoint: Endpoint,
        *,
        enr_seq: Optional[int] = None,
        request_id: bytes,
    ) -> None:
        if enr_seq is None:
            enr_seq = self.enr_manager.enr.sequence_number

        message = AnyOutboundMessage(
            PongMessage(request_id, enr_seq, endpoint.ip_address, endpoint.port,),
            endpoint,
            node_id,
        )
        await self.dispatcher.send_message(message)

    async def send_find_nodes(
        self,
        node_id: NodeID,
        endpoint: Endpoint,
        *,
        distances: Collection[int],
        request_id: Optional[bytes] = None,
    ) -> bytes:
        with self.request_tracker.reserve_request_id(
            node_id, request_id
        ) as message_request_id:
            message = AnyOutboundMessage(
                FindNodeMessage(message_request_id, tuple(distances)),
                endpoint,
                node_id,
            )
            await self.dispatcher.send_message(message)

        return message_request_id

    async def send_found_nodes(
        self,
        node_id: NodeID,
        endpoint: Endpoint,
        *,
        enrs: Sequence[ENRAPI],
        request_id: bytes,
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
        node_id: NodeID,
        endpoint: Endpoint,
        *,
        protocol: bytes,
        payload: bytes,
        request_id: Optional[bytes] = None,
    ) -> bytes:
        with self.request_tracker.reserve_request_id(
            node_id, request_id
        ) as message_request_id:
            message = AnyOutboundMessage(
                TalkRequestMessage(message_request_id, protocol, payload),
                endpoint,
                node_id,
            )
            await self.dispatcher.send_message(message)

        return message_request_id

    async def send_talk_response(
        self, node_id: NodeID, endpoint: Endpoint, *, payload: bytes, request_id: bytes,
    ) -> None:
        message = AnyOutboundMessage(
            TalkResponseMessage(request_id, payload), endpoint, node_id,
        )
        await self.dispatcher.send_message(message)

    async def send_register_topic(
        self,
        node_id: NodeID,
        endpoint: Endpoint,
        *,
        topic: bytes,
        enr: ENRAPI,
        ticket: bytes = b"",
        request_id: Optional[bytes] = None,
    ) -> bytes:
        with self.request_tracker.reserve_request_id(
            node_id, request_id
        ) as message_request_id:
            message = AnyOutboundMessage(
                RegisterTopicMessage(message_request_id, topic, enr, ticket),
                endpoint,
                node_id,
            )
            await self.dispatcher.send_message(message)

        return message_request_id

    async def send_ticket(
        self,
        node_id: NodeID,
        endpoint: Endpoint,
        *,
        ticket: bytes,
        wait_time: int,
        request_id: bytes,
    ) -> None:
        message = AnyOutboundMessage(
            TicketMessage(request_id, ticket, wait_time), endpoint, node_id,
        )
        await self.dispatcher.send_message(message)

    async def send_registration_confirmation(
        self, node_id: NodeID, endpoint: Endpoint, *, topic: bytes, request_id: bytes,
    ) -> None:
        message = AnyOutboundMessage(
            RegistrationConfirmationMessage(request_id, topic), endpoint, node_id,
        )
        await self.dispatcher.send_message(message)

    async def send_topic_query(
        self,
        node_id: NodeID,
        endpoint: Endpoint,
        *,
        topic: bytes,
        request_id: Optional[bytes] = None,
    ) -> bytes:
        with self.request_tracker.reserve_request_id(
            node_id, request_id
        ) as message_request_id:
            message = AnyOutboundMessage(
                TopicQueryMessage(message_request_id, topic), endpoint, node_id,
            )
            await self.dispatcher.send_message(message)
        return message_request_id

    #
    # Request/Response API
    #
    async def ping(
        self,
        node_id: NodeID,
        endpoint: Endpoint,
        *,
        request_id: Optional[bytes] = None,
    ) -> InboundMessage[PongMessage]:
        with self.request_tracker.reserve_request_id(node_id, request_id) as request_id:
            request = AnyOutboundMessage(
                PingMessage(request_id, self.enr_manager.enr.sequence_number),
                endpoint,
                node_id,
            )
            async with self.dispatcher.subscribe_request(
                request, PongMessage
            ) as subscription:
                return await subscription.receive()

    async def find_nodes(
        self,
        node_id: NodeID,
        endpoint: Endpoint,
        distances: Collection[int],
        *,
        request_id: Optional[bytes] = None,
    ) -> Tuple[InboundMessage[FoundNodesMessage], ...]:
        with self.request_tracker.reserve_request_id(node_id, request_id) as request_id:
            request = AnyOutboundMessage(
                FindNodeMessage(request_id, tuple(distances)), endpoint, node_id,
            )
            async with self.dispatcher.subscribe_request(
                request, FoundNodesMessage
            ) as subscription:
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
                    raise ValidationError(
                        f"Invalid `total` counter in response: total={total}"
                    )

                return responses

    def stream_find_nodes(
        self,
        node_id: NodeID,
        endpoint: Endpoint,
        distances: Collection[int],
        *,
        request_id: Optional[bytes] = None,
    ) -> AsyncContextManager[
        trio.abc.ReceiveChannel[InboundMessage[FoundNodesMessage]]
    ]:
        return common_client_stream_find_nodes(
            self, node_id, endpoint, distances, request_id=request_id
        )

    async def talk(
        self,
        node_id: NodeID,
        endpoint: Endpoint,
        protocol: bytes,
        payload: bytes,
        *,
        request_id: Optional[bytes] = None,
    ) -> InboundMessage[TalkResponseMessage]:
        with self.request_tracker.reserve_request_id(node_id, request_id) as request_id:
            request = AnyOutboundMessage(
                TalkRequestMessage(request_id, protocol, payload), endpoint, node_id,
            )
            async with self.dispatcher.subscribe_request(
                request, TalkResponseMessage
            ) as subscription:
                return await subscription.receive()

    async def register_topic(
        self,
        node_id: NodeID,
        endpoint: Endpoint,
        topic: bytes,
        ticket: Optional[bytes] = None,
        *,
        request_id: Optional[bytes] = None,
    ) -> Tuple[
        InboundMessage[TicketMessage],
        Optional[InboundMessage[RegistrationConfirmationMessage]],
    ]:
        raise NotImplementedError

    async def topic_query(
        self,
        node_id: NodeID,
        endpoint: Endpoint,
        topic: bytes,
        *,
        request_id: Optional[bytes] = None,
    ) -> InboundMessage[FoundNodesMessage]:
        raise NotImplementedError


@asynccontextmanager
async def common_client_stream_find_nodes(
    client: Client,
    node_id: NodeID,
    endpoint: Endpoint,
    distances: Collection[int],
    *,
    request_id: Optional[bytes] = None,
) -> AsyncIterator[trio.abc.ReceiveChannel[InboundMessage[FoundNodesMessage]]]:
    async def _stream_find_nodes_response(
        response_message_type: Type[BaseMessage],
        send_channel: trio.abc.SendChannel[InboundMessage[FoundNodesMessage]],
    ) -> None:

        with trio.move_on_after(REQUEST_RESPONSE_TIMEOUT) as scope:
            async with send_channel:
                with client.request_tracker.reserve_request_id(
                    node_id, request_id
                ) as reserved_request_id:
                    request = AnyOutboundMessage(
                        FindNodeMessage(reserved_request_id, tuple(distances)),
                        endpoint,
                        node_id,
                    )

                    async with client.dispatcher.subscribe_request(
                        request, response_message_type
                    ) as subscription:
                        head_response = await subscription.receive()
                        expected_total = head_response.message.total
                        validate_found_nodes_response(
                            head_response.message, request, expected_total,
                        )
                        await send_channel.send(head_response)

                        for _ in range(expected_total - 1):
                            response = await subscription.receive()
                            validate_found_nodes_response(
                                response.message, request, expected_total
                            )

                            await send_channel.send(response)

        if scope.cancelled_caught:
            client.logger.debug(
                "Stream find nodes request disconnected: request=%s message_type=%s",
                request,
                reserved_request_id,
            )
            raise trio.TooSlowError("Timeout in stream_find_nodes")

    async with trio.open_nursery() as nursery:
        send_channel, receive_channel = trio.open_memory_channel[
            InboundMessage[FoundNodesMessage]
        ](4)
        nursery.start_soon(
            _stream_find_nodes_response, FoundNodesMessage, send_channel,
        )

        async with receive_channel:
            try:
                yield receive_channel
            except trio.EndOfChannel as err:
                raise trio.TooSlowError("Timeout in stream_find_nodes") from err
            except (trio.ClosedResourceError, trio.ClosedResourceError):
                pass

        nursery.cancel_scope.cancel()


def validate_found_nodes_response(
    message: FoundNodesMessage, request: AnyOutboundMessage, expected_total: int
) -> None:
    if message.total == 0:
        raise ValidationError(
            f"Invalid `total` counter in response: total={message.total}"
        )

    if expected_total != message.total:
        raise ValidationError(
            "Inconsistent message total. Received a FoundNodesMessage with "
            f"a total of {message.total}, expected a total of {expected_total}"
        )

    validate_found_nodes_distances(
        message.enrs, request.receiver_node_id, request.message.distances
    )
