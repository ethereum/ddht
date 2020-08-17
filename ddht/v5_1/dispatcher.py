import collections
from contextlib import contextmanager
import logging
import secrets
from typing import (
    AsyncIterator,
    DefaultDict,
    Iterator,
    NamedTuple,
    Optional,
    Set,
    Tuple,
    Type,
)

from async_generator import asynccontextmanager
from async_service import Service
import trio

from ddht.abc import NodeDBAPI
from ddht.base_message import (
    AnyInboundMessage,
    AnyOutboundMessage,
    InboundMessage,
    TMessage,
)
from ddht.endpoint import Endpoint
from ddht.message_registry import MessageTypeRegistry
from ddht.typing import NodeID
from ddht.v5_1.abc import DispatcherAPI, EventsAPI, PoolAPI, SessionAPI
from ddht.v5_1.constants import REQUEST_RESPONSE_TIMEOUT
from ddht.v5_1.envelope import InboundEnvelope
from ddht.v5_1.events import Events
from ddht.v5_1.messages import PingMessage, PongMessage, v51_registry


class _Subcription(NamedTuple):
    send_channel: trio.abc.SendChannel[AnyInboundMessage]
    filter_by_endpoint: Optional[Endpoint]
    filter_by_node_id: Optional[NodeID]


def get_random_request_id() -> int:
    return secrets.randbits(32)


MAX_REQUEST_ID_ATTEMPTS = 3


class Dispatcher(Service, DispatcherAPI):
    logger = logging.getLogger("ddht.Dispatcher")

    _subscriptions: DefaultDict[int, Set[_Subcription]]

    _reserved_request_ids: Set[Tuple[NodeID, int]]
    _active_request_ids: Set[Tuple[NodeID, int]]

    def __init__(
        self,
        inbound_envelope_receive_channel: trio.abc.ReceiveChannel[InboundEnvelope],
        inbound_message_receive_channel: trio.abc.ReceiveChannel[AnyInboundMessage],
        pool: PoolAPI,
        node_db: NodeDBAPI,
        message_registry: MessageTypeRegistry = v51_registry,
        events: EventsAPI = None,
    ) -> None:
        self._registry = message_registry

        self._pool = pool
        self._node_db = node_db

        if events is None:
            events = Events()
        self._events = events

        self._inbound_envelope_receive_channel = inbound_envelope_receive_channel
        self._inbound_message_receive_channel = inbound_message_receive_channel

        (
            self._outbound_message_send_channel,
            self._outbound_message_receive_channel,
        ) = trio.open_memory_channel[AnyOutboundMessage](256)

        self._subscriptions = collections.defaultdict(set)

        self._reserved_request_ids = set()
        self._active_request_ids = set()

    async def run(self) -> None:
        self.manager.run_daemon_task(
            self._handle_inbound_envelopes, self._inbound_envelope_receive_channel,
        )
        self.manager.run_daemon_task(
            self._handle_inbound_messages, self._inbound_message_receive_channel,
        )
        self.manager.run_daemon_task(
            self._handle_outbound_messages, self._outbound_message_receive_channel,
        )
        await self.manager.wait_finished()

    async def _handle_inbound_envelopes(
        self, receive_channel: trio.abc.ReceiveChannel[InboundEnvelope],
    ) -> None:
        async with trio.open_nursery() as nursery:
            async with receive_channel:
                async for envelope in receive_channel:
                    for session in self._get_sessions_for_inbound_envelope(envelope):
                        nursery.start_soon(session.handle_inbound_envelope, envelope)
                        self.logger.debug(
                            "inbound envelope %s dispatched to %s", envelope, session,
                        )

    async def trigger_event_for_outbound_message(
        self, message: AnyOutboundMessage
    ) -> None:
        message_type = type(message.message)
        if message_type is PingMessage:
            await self._events.ping_sent.trigger(message)
        elif message_type is PongMessage:
            await self._events.pong_sent.trigger(message)
        else:
            raise Exception(f"Unhandled message type: {message_type}")

    async def _handle_outbound_messages(
        self, receive_channel: trio.abc.ReceiveChannel[AnyOutboundMessage],
    ) -> None:
        async with trio.open_nursery() as nursery:
            async with receive_channel:
                async for message in receive_channel:
                    # trigger Event
                    await self.trigger_event_for_outbound_message(message)

                    # feed sessions
                    sessions = self._get_sessions_for_outbound_message(message)
                    for session in sessions:
                        nursery.start_soon(session.handle_outbound_message, message)
                        self.logger.debug(
                            "outbound message %s dispatched to %s", message, session
                        )

    async def trigger_event_for_inbound_message(
        self, message: AnyInboundMessage
    ) -> None:
        message_type = type(message.message)
        if message_type is PingMessage:
            await self._events.ping_received.trigger(message)
        elif message_type is PongMessage:
            await self._events.pong_received.trigger(message)
        else:
            raise Exception(f"Unhandled message type: {message_type}")

    async def _handle_inbound_messages(
        self, receive_channel: trio.abc.ReceiveChannel[AnyInboundMessage],
    ) -> None:
        async with receive_channel:
            async for message in receive_channel:
                # trigger Event
                await self.trigger_event_for_inbound_message(message)

                # feed subscriptions
                message_id = self._registry.get_message_id(type(message.message))
                subscriptions = tuple(self._subscriptions[message_id])
                self.logger.debug(
                    "Handling %d subscriptions for message: %s",
                    len(subscriptions),
                    message,
                )
                for subscription in subscriptions:
                    if subscription.filter_by_endpoint is not None:
                        if message.sender_endpoint != subscription.filter_by_endpoint:
                            continue
                    if subscription.filter_by_node_id is not None:
                        if message.sender_node_id != subscription.filter_by_node_id:
                            continue

                    try:
                        subscription.send_channel.send_nowait(message)  # type: ignore
                    except trio.WouldBlock:
                        self.logger.debug(
                            "Discarding message for subscription %s due to full channel: %s",
                            subscription,
                            message,
                        )
                    except trio.BrokenResourceError:
                        pass

    #
    # Session Retrieval
    #
    def _get_sessions_for_inbound_envelope(
        self, envelope: InboundEnvelope
    ) -> Tuple[SessionAPI, ...]:
        sessions = tuple(
            session
            for session in self._pool.get_sessions_for_endpoint(
                envelope.sender_endpoint
            )
            if (
                not session.is_after_handshake
                or session.remote_node_id == envelope.packet.header.source_node_id
            )
        )

        if not sessions:
            session = self._pool.receive_session(envelope.sender_endpoint)
            self.logger.debug(
                "Inbound envelope %s initiated new session: %s", envelope, session
            )
            sessions = (session,)

        return sessions

    def _get_sessions_for_outbound_message(
        self, message: AnyOutboundMessage
    ) -> Tuple[SessionAPI, ...]:
        sessions = tuple(
            session
            for session in self._pool.get_sessions_for_endpoint(
                message.receiver_endpoint
            )
            if (
                not session.is_after_handshake
                or session.remote_node_id == message.receiver_node_id
            )
        )

        if not sessions:
            session = self._pool.initiate_session(
                message.receiver_endpoint, message.receiver_node_id,
            )
            self.logger.debug(
                "Outbound message %s initiated new session: %s", message, session
            )
            sessions = (session,)

        return sessions

    #
    # Utility
    #
    def get_free_request_id(self, node_id: NodeID) -> int:
        for _ in range(MAX_REQUEST_ID_ATTEMPTS):
            request_id = get_random_request_id()
            if (node_id, request_id) in self._reserved_request_ids:
                continue
            elif (node_id, request_id) in self._active_request_ids:
                continue
            else:
                return request_id
        else:
            # The improbability of picking three already used request ids in a
            # row is sufficiently improbable that we can generally assume it
            # just will not ever happen (< 1/2**96)
            raise ValueError(
                f"Failed to get free request id ({len(self._reserved_request_ids)} "
                f"handlers added right now)"
            )

    @contextmanager
    def reserve_request_id(self, node_id: NodeID) -> Iterator[int]:
        request_id = self.get_free_request_id(node_id)
        try:
            self._reserved_request_ids.add((node_id, request_id))
            yield request_id
        finally:
            self._reserved_request_ids.remove((node_id, request_id))

    #
    # Message Sending
    #
    async def send_message(self, message: AnyOutboundMessage) -> None:
        await self._outbound_message_send_channel.send(message)

    #
    # Request Response
    #
    @asynccontextmanager
    async def subscribe(
        self,
        message_type: Type[TMessage],
        endpoint: Optional[Endpoint] = None,
        node_id: Optional[NodeID] = None,
    ) -> AsyncIterator[trio.abc.ReceiveChannel[InboundMessage[TMessage]]]:
        message_id = self._registry.get_message_id(message_type)
        send_channel, receive_channel = trio.open_memory_channel[
            InboundMessage[TMessage]
        ](256)
        subscription = _Subcription(send_channel, endpoint, node_id)
        self._subscriptions[message_id].add(subscription)
        try:
            async with receive_channel:
                yield receive_channel
        finally:
            self._subscriptions[message_id].remove(subscription)

    @asynccontextmanager
    async def subscribe_request(
        self, request: AnyOutboundMessage, response_message_type: Type[TMessage],
    ) -> AsyncIterator[trio.abc.ReceiveChannel[InboundMessage[TMessage]]]:  # noqa: E501
        node_id = request.receiver_node_id
        request_id = request.message.request_id

        self.logger.debug(
            "Sending request: %s with request id %d", request, request_id,
        )

        send_channel, receive_channel = trio.open_memory_channel[TMessage](256)
        key = (node_id, request_id)
        if key in self._active_request_ids:
            raise Exception("Invariant")
        self._active_request_ids.add(key)

        async with trio.open_nursery() as nursery:
            nursery.start_soon(
                self._manage_request_response,
                request,
                response_message_type,
                send_channel,
            )
            try:
                async with receive_channel:
                    yield receive_channel
            finally:
                self._active_request_ids.remove(key)
                nursery.cancel_scope.cancel()

    async def _manage_request_response(
        self,
        request: AnyOutboundMessage,
        response_message_type: Type[TMessage],
        send_channel: trio.abc.SendChannel[InboundMessage[TMessage]],
    ) -> None:
        request_id = request.message.request_id

        with trio.move_on_after(REQUEST_RESPONSE_TIMEOUT) as scope:
            subscription_ctx = self.subscribe(
                response_message_type,
                request.receiver_endpoint,
                request.receiver_node_id,
            )
            async with subscription_ctx as subscription:
                self.logger.debug(
                    "Sending request with request id %d", request_id,
                )
                # Send the request
                await self.send_message(request)

                # Wait for the response
                async with send_channel:
                    async for response in subscription:
                        if response.message.request_id != request_id:
                            continue
                        else:
                            await send_channel.send(response)
        if scope.cancelled_caught:
            self.logger.warning(
                "Abandoned request response monitor: request=%s message_type=%s",
                request,
                response_message_type,
            )
