import functools
import logging
from typing import AsyncContextManager, AsyncIterator, Optional, Set, Tuple, Type

from async_generator import asynccontextmanager
from async_service import Service
from eth_enr import ENRDatabaseAPI
from eth_typing import NodeID
import trio

from ddht._utils import humanize_node_id
from ddht.abc import EventAPI
from ddht.base_message import (
    AnyInboundMessage,
    AnyOutboundMessage,
    InboundMessage,
    OutboundMessage,
    TBaseMessage,
)
from ddht.endpoint import Endpoint
from ddht.subscription_manager import SubscriptionManager
from ddht.v5_1.abc import DispatcherAPI, EventsAPI, PoolAPI, SessionAPI
from ddht.v5_1.constants import REQUEST_RESPONSE_TIMEOUT
from ddht.v5_1.envelope import InboundEnvelope
from ddht.v5_1.events import Events
from ddht.v5_1.exceptions import SessionNotFound
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
)


def _get_event_for_outbound_message(
    events: EventsAPI, message: OutboundMessage[TBaseMessage],
) -> EventAPI[OutboundMessage[TBaseMessage]]:
    message_type = type(message.message)

    if message_type is PingMessage:
        return events.ping_sent
    elif message_type is PongMessage:
        return events.pong_sent
    elif message_type is FindNodeMessage:
        return events.find_nodes_sent
    elif message_type is FoundNodesMessage:
        return events.found_nodes_sent
    elif message_type is TalkRequestMessage:
        return events.talk_request_sent
    elif message_type is TalkResponseMessage:
        return events.talk_response_sent
    elif message_type is RegisterTopicMessage:
        return events.register_topic_sent
    elif message_type is TicketMessage:
        return events.ticket_sent
    elif message_type is RegistrationConfirmationMessage:
        return events.registration_confirmation_sent
    elif message_type is TopicQueryMessage:
        return events.topic_query_sent
    else:
        raise Exception(f"Unhandled message type: {message_type}")


def _get_event_for_inbound_message(
    events: EventsAPI, message: InboundMessage[TBaseMessage]
) -> EventAPI[InboundMessage[TBaseMessage]]:
    message_type = type(message.message)

    if message_type is PingMessage:
        return events.ping_received
    elif message_type is PongMessage:
        return events.pong_received
    elif message_type is FindNodeMessage:
        return events.find_nodes_received
    elif message_type is FoundNodesMessage:
        return events.found_nodes_received
    elif message_type is TalkRequestMessage:
        return events.talk_request_received
    elif message_type is TalkResponseMessage:
        return events.talk_response_received
    elif message_type is RegisterTopicMessage:
        return events.register_topic_received
    elif message_type is TicketMessage:
        return events.ticket_received
    elif message_type is RegistrationConfirmationMessage:
        return events.registration_confirmation_received
    elif message_type is TopicQueryMessage:
        return events.topic_query_received
    else:
        raise Exception(f"Unhandled message type: {message_type}")


class Dispatcher(Service, DispatcherAPI):
    logger = logging.getLogger("ddht.Dispatcher")

    _reserved_request_ids: Set[Tuple[NodeID, bytes]]
    _active_request_ids: Set[Tuple[NodeID, bytes]]

    def __init__(
        self,
        inbound_envelope_receive_channel: trio.abc.ReceiveChannel[InboundEnvelope],
        inbound_message_receive_channel: trio.abc.ReceiveChannel[AnyInboundMessage],
        pool: PoolAPI,
        enr_db: ENRDatabaseAPI,
        events: EventsAPI = None,
    ) -> None:
        self._pool = pool
        self._enr_db = enr_db

        if events is None:
            events = Events()
        self._events = events

        self._inbound_envelope_receive_channel = inbound_envelope_receive_channel
        self._inbound_message_receive_channel = inbound_message_receive_channel

        (
            self._outbound_message_send_channel,
            self._outbound_message_receive_channel,
        ) = trio.open_memory_channel[AnyOutboundMessage](256)

        self.subscription_manager = SubscriptionManager()

        self._reserved_request_ids = set()
        self._active_request_ids = set()

    def subscribe(
        self,
        message_type: Type[TBaseMessage],
        endpoint: Optional[Endpoint] = None,
        node_id: Optional[NodeID] = None,
    ) -> AsyncContextManager[trio.abc.ReceiveChannel[InboundMessage[TBaseMessage]]]:
        return self.subscription_manager.subscribe(message_type, endpoint, node_id)

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
        self.manager.run_daemon_task(self._monitor_hanshake_completions)
        await self.manager.wait_finished()

    async def _handle_inbound_envelopes(
        self, receive_channel: trio.abc.ReceiveChannel[InboundEnvelope],
    ) -> None:
        async with receive_channel:
            async for envelope in receive_channel:
                was_handled = False
                for session in self._get_sessions_for_inbound_envelope(envelope):
                    try:
                        was_handled |= await session.handle_inbound_envelope(envelope)
                    except trio.BrokenResourceError:
                        self.logger.debug(
                            "Dispatcher exiting due to trio.BrokenResourceError"
                        )
                        self.manager.cancel()
                        return
                    else:
                        self.logger.debug(
                            "inbound envelope %s dispatched to %s", envelope, session,
                        )
                if was_handled is False:
                    if envelope.packet.is_message:
                        session = self._pool.receive_session(envelope.sender_endpoint)
                        await session.handle_inbound_envelope(envelope)
                        self.logger.debug(
                            "inbound envelope %s initiated new session: %s",
                            envelope,
                            session,
                        )
                    else:
                        self.logger.debug(
                            "discarding unhandled inbound envelope %s", envelope,
                        )

    async def _handle_outbound_messages(
        self, receive_channel: trio.abc.ReceiveChannel[AnyOutboundMessage],
    ) -> None:
        @functools.lru_cache(16)
        def get_event(
            message: OutboundMessage[TBaseMessage],
        ) -> EventAPI[OutboundMessage[TBaseMessage]]:
            return _get_event_for_outbound_message(self._events, message)

        async with receive_channel:
            async for message in receive_channel:
                # trigger Event
                event = get_event(message)
                await event.trigger(message)

                # feed sessions
                sessions = self._get_sessions_for_outbound_message(message)
                for session in sessions:
                    try:
                        await session.handle_outbound_message(message)
                    except trio.BrokenResourceError:
                        self.logger.debug(
                            "Dispatcher exiting due to trio.BrokenResourceError"
                        )
                        self.manager.cancel()
                    else:
                        self.logger.debug(
                            "outbound message %s dispatched to %s", message, session
                        )

    async def _handle_inbound_messages(
        self, receive_channel: trio.abc.ReceiveChannel[AnyInboundMessage],
    ) -> None:
        @functools.lru_cache(16)
        def get_event(
            message: InboundMessage[TBaseMessage],
        ) -> EventAPI[InboundMessage[TBaseMessage]]:
            return _get_event_for_inbound_message(self._events, message)

        async with receive_channel:
            async for message in receive_channel:
                # trigger Event
                event = get_event(message)
                await event.trigger(message)

                # feed subscriptions
                self.subscription_manager.feed_subscriptions(message)

    #
    # Session Management
    #
    async def _monitor_hanshake_completions(self) -> None:
        """
        Ensure that we only ever have one fully handshaked session for any
        given endpoint/node-id.  Anytime we find a duplicate sessions exists we
        discard them, preferring the newly handshaked session.
        """
        async with self._events.session_handshake_complete.subscribe() as subscription:
            async for session in subscription:
                self.logger.info(
                    "Session established: %s@%s (%s) id=%s",
                    humanize_node_id(session.remote_node_id),
                    session.remote_endpoint,
                    "outbound" if session.is_initiator else "inbound",
                    session.id,
                )

                for other in self._pool.get_sessions_for_endpoint(
                    session.remote_endpoint
                ):
                    if not other.is_after_handshake:
                        continue
                    elif other.id == session.id:
                        continue
                    elif other.remote_node_id != session.remote_node_id:
                        continue
                    else:
                        self.logger.debug(
                            "Newly handshaked session %s triggered discard of previous session %s",
                            session,
                            other,
                        )
                        self._pool.remove_session(other.id)

    async def _monitor_session_timeout(self, session: SessionAPI) -> None:
        """
        Monitor for the session to timeout, removing it from the pool.
        """
        while self.manager.is_running:
            await trio.sleep_until(session.timeout_at)

            if session.is_timed_out:
                try:
                    self._pool.remove_session(session.id)
                except SessionNotFound:
                    break
                else:
                    await self._events.session_timeout.trigger(session)
                    break

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
                or (
                    (
                        envelope.packet.is_message
                        and session.remote_node_id
                        == envelope.packet.auth_data.source_node_id  # type: ignore  # noqa: E501
                    )
                    or (
                        envelope.packet.is_handshake
                        and session.remote_node_id
                        == envelope.packet.auth_data.auth_data_head.source_node_id  # type: ignore
                    )
                )
            )
        )

        if not sessions:
            session = self._pool.receive_session(envelope.sender_endpoint)
            self.logger.debug(
                "Inbound envelope %s initiated new session: %s", envelope, session
            )
            self.manager.run_task(self._monitor_session_timeout, session)
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
            and not (session.is_before_handshake and session.is_recipient)
        )

        if not sessions:
            session = self._pool.initiate_session(
                message.receiver_endpoint, message.receiver_node_id,
            )
            self.logger.debug(
                "Outbound message %s initiated new session: %s", message, session
            )
            self.manager.run_task(self._monitor_session_timeout, session)
            sessions = (session,)

        return sessions

    #
    # Message Sending
    #
    async def send_message(self, message: AnyOutboundMessage) -> None:
        if message.receiver_node_id == self._pool.local_node_id:
            raise Exception("Cannot send message to self")
        try:
            await self._outbound_message_send_channel.send(message)
        except trio.BrokenResourceError:
            if self.manager.is_cancelled:
                await trio.sleep(1)
            raise

    #
    # Request Response
    #
    @asynccontextmanager
    async def subscribe_request(
        self, request: AnyOutboundMessage, response_message_type: Type[TBaseMessage],
    ) -> AsyncIterator[trio.abc.ReceiveChannel[InboundMessage[TBaseMessage]]]:
        request_id = request.message.request_id

        self.logger.debug(
            "Sending request: %s with request id %s", request, request_id.hex(),
        )

        send_channel, receive_channel = trio.open_memory_channel[TBaseMessage](256)

        async with trio.open_nursery() as nursery:
            nursery.start_soon(
                self._manage_request_response,
                request,
                response_message_type,
                send_channel,
            )
            try:
                async with receive_channel:
                    try:
                        yield receive_channel
                    # Wrap EOC error with TSE to make the timeouts obvious
                    except trio.EndOfChannel as err:
                        raise trio.TooSlowError from err
            finally:
                nursery.cancel_scope.cancel()

    async def _manage_request_response(
        self,
        request: AnyOutboundMessage,
        response_message_type: Type[TBaseMessage],
        send_channel: trio.abc.SendChannel[InboundMessage[TBaseMessage]],
    ) -> None:
        request_id = request.message.request_id

        with trio.move_on_after(REQUEST_RESPONSE_TIMEOUT) as scope:
            subscription_ctx = self.subscription_manager.subscribe(
                response_message_type,
                request.receiver_endpoint,
                request.receiver_node_id,
            )
            async with subscription_ctx as subscription:
                self.logger.debug(
                    "Sending request with request id %s", request_id.hex(),
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
