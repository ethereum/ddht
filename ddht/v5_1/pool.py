import collections
import logging
from typing import DefaultDict, Dict, Set, Tuple
import uuid

from eth_keys import keys
import trio

from ddht.abc import NodeDBAPI
from ddht.base_message import AnyInboundMessage
from ddht.endpoint import Endpoint
from ddht.message_registry import MessageTypeRegistry
from ddht.typing import NodeID
from ddht.v5_1.abc import EventsAPI, PoolAPI, SessionAPI
from ddht.v5_1.constants import SESSION_IDLE_TIMEOUT
from ddht.v5_1.envelope import OutboundEnvelope
from ddht.v5_1.events import Events
from ddht.v5_1.messages import v51_registry
from ddht.v5_1.session import SessionInitiator, SessionRecipient


class Pool(PoolAPI):
    logger = logging.getLogger("ddht.Pool")

    _sessions: Dict[uuid.UUID, SessionAPI]
    _sessions_by_endpoint: DefaultDict[Endpoint, Set[SessionAPI]]

    def __init__(
        self,
        local_private_key: keys.PrivateKey,
        local_node_id: NodeID,
        node_db: NodeDBAPI,
        outbound_envelope_send_channel: trio.abc.SendChannel[OutboundEnvelope],
        inbound_message_send_channel: trio.abc.SendChannel[AnyInboundMessage],
        message_type_registry: MessageTypeRegistry = v51_registry,
        events: EventsAPI = None,
    ) -> None:
        self.local_private_key = local_private_key
        self.local_node_id = local_node_id

        self._node_db = node_db
        self._message_type_registry = message_type_registry

        if events is None:
            events = Events()
        self._events = events

        self._sessions = {}
        self._sessions_by_endpoint = collections.defaultdict(set)

        self._outbound_packet_send_channel = outbound_envelope_send_channel
        self._inbound_message_send_channel = inbound_message_send_channel

    def remove_session(self, session_id: uuid.UUID) -> SessionAPI:
        session = self._sessions.pop(session_id)
        self._sessions_by_endpoint[session.remote_endpoint].remove(session)
        return session

    def get_idle_sesssions(self) -> Tuple[SessionAPI, ...]:
        idle_at = trio.current_time() - SESSION_IDLE_TIMEOUT
        return tuple(
            session
            for session in self._sessions.values()
            if (
                (
                    session.is_after_handshake
                    and session.last_message_received_at <= idle_at
                )
                or (session.created_at <= idle_at)
            )
        )

    def get_sessions_for_endpoint(
        self, remote_endpoint: Endpoint
    ) -> Tuple[SessionAPI, ...]:
        if remote_endpoint in self._sessions_by_endpoint:
            return tuple(self._sessions_by_endpoint[remote_endpoint])
        else:
            return ()

    def initiate_session(
        self, remote_endpoint: Endpoint, remote_node_id: NodeID
    ) -> SessionAPI:

        session = SessionInitiator(
            local_private_key=self.local_private_key.to_bytes(),
            local_node_id=self.local_node_id,
            remote_endpoint=remote_endpoint,
            remote_node_id=remote_node_id,
            node_db=self._node_db,
            inbound_message_send_channel=self._inbound_message_send_channel.clone(),  # type: ignore  # noqa: E501
            outbound_envelope_send_channel=self._outbound_packet_send_channel.clone(),  # type: ignore  # noqa: E501
            message_type_registry=self._message_type_registry,
            events=self._events,
        )

        self._sessions[session.id] = session
        self._sessions_by_endpoint[remote_endpoint].add(session)
        self._events.session_created.trigger_nowait(session)

        return session

    def receive_session(self, remote_endpoint: Endpoint) -> SessionAPI:
        # TODO: I think we should have the remote node id at this stage since
        # it is part of the packet header, however it isn't verifiable until we
        # receive the signature.
        session = SessionRecipient(
            local_private_key=self.local_private_key.to_bytes(),
            local_node_id=self.local_node_id,
            remote_endpoint=remote_endpoint,
            node_db=self._node_db,
            inbound_message_send_channel=self._inbound_message_send_channel.clone(),  # type: ignore  # noqa: E501
            outbound_envelope_send_channel=self._outbound_packet_send_channel.clone(),  # type: ignore  # noqa: E501
            message_type_registry=self._message_type_registry,
            events=self._events,
        )

        self._sessions[session.id] = session
        self._sessions_by_endpoint[remote_endpoint].add(session)
        self._events.session_created.trigger_nowait(session)

        return session
