from abc import ABC, abstractmethod
import logging
from typing import Tuple
import uuid

from ddht.abc import EventAPI
from ddht.base_message import OutboundMessage
from ddht.endpoint import Endpoint
from ddht.typing import NodeID, SessionKeys
from ddht.v5_1.envelope import InboundEnvelope


class SessionAPI(ABC):
    id: uuid.UUID
    remote_endpoint: Endpoint
    events: "EventsAPI"
    logger: logging.Logger
    is_initiator: bool

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
    async def handle_outbound_message(self, message: OutboundMessage) -> None:
        ...

    @abstractmethod
    async def handle_inbound_envelope(self, envelope: InboundEnvelope) -> None:
        ...


class EventsAPI(ABC):
    session_created: EventAPI[SessionAPI]
    session_handshake_complete: EventAPI[SessionAPI]
    packet_discarded: EventAPI[Tuple[SessionAPI, InboundEnvelope]]
