from abc import ABC, abstractmethod
import logging
from typing import Tuple
import uuid

from eth_keys import keys

from ddht.abc import EventAPI
from ddht.base_message import AnyOutboundMessage
from ddht.endpoint import Endpoint
from ddht.typing import NodeID, SessionKeys
from ddht.v5_1.envelope import InboundEnvelope


class SessionAPI(ABC):
    logger: logging.Logger

    id: uuid.UUID
    remote_endpoint: Endpoint
    events: "EventsAPI"

    is_initiator: bool
    created_at: float

    @property
    @abstractmethod
    def remote_node_id(self) -> NodeID:
        ...

    @property
    @abstractmethod
    def keys(self) -> SessionKeys:
        ...

    @property
    @abstractmethod
    def last_message_received_at(self) -> float:
        ...

    @property
    @abstractmethod
    def is_before_handshake(self) -> bool:
        ...

    @property
    @abstractmethod
    def is_during_handshake(self) -> bool:
        ...

    @property
    @abstractmethod
    def is_after_handshake(self) -> bool:
        ...

    @abstractmethod
    async def handle_outbound_message(self, message: AnyOutboundMessage) -> None:
        ...

    @abstractmethod
    async def handle_inbound_envelope(self, envelope: InboundEnvelope) -> None:
        ...


class EventsAPI(ABC):
    session_created: EventAPI[SessionAPI]
    session_handshake_complete: EventAPI[SessionAPI]
    session_timeout: EventAPI[SessionAPI]

    packet_discarded: EventAPI[Tuple[SessionAPI, InboundEnvelope]]

    listening: EventAPI[Endpoint]

    external_endpoint_updated: EventAPI[Endpoint]


class PoolAPI(ABC):
    local_private_key: keys.PrivateKey
    local_node_id: NodeID

    @abstractmethod
    def remove_session(self, session_id: uuid.UUID) -> SessionAPI:
        ...

    @abstractmethod
    def get_idle_sesssions(self) -> Tuple[SessionAPI, ...]:
        ...

    @abstractmethod
    def get_sessions_for_endpoint(
        self, remote_endpoint: Endpoint
    ) -> Tuple[SessionAPI, ...]:
        ...

    @abstractmethod
    def initiate_session(
        self, remote_endpoint: Endpoint, remote_node_id: NodeID
    ) -> SessionAPI:
        ...

    @abstractmethod
    def receive_session(self, remote_endpoint: Endpoint) -> SessionAPI:
        ...
