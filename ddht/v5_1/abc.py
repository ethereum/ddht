from abc import ABC, abstractmethod
import uuid

from ddht.abc import EventAPI
from ddht.base_message import OutgoingMessage
from ddht.endpoint import Endpoint
from ddht.typing import NodeID, SessionKeys
from ddht.v5_1.envelope import IncomingEnvelope


class SessionAPI(ABC):
    id: uuid.UUID
    remote_endpoint: Endpoint

    @property
    @abstractmethod
    def remote_node_id(self) -> NodeID:
        ...

    @property
    @abstractmethod
    def keys(self) -> SessionKeys:
        ...

    @abstractmethod
    async def handle_outgoing_message(self, message: OutgoingMessage) -> None:
        ...

    @abstractmethod
    async def handle_incoming_envelope(self, envelope: IncomingEnvelope) -> None:
        ...


class EventsAPI(ABC):
    session_created: EventAPI[SessionAPI]
