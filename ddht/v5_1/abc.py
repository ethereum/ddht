from abc import ABC, abstractmethod
import logging
from typing import AsyncContextManager, ContextManager, Optional, Tuple, Type
import uuid

from async_service import ServiceAPI
from eth_keys import keys
import trio

from ddht.abc import EventAPI
from ddht.base_message import (
    AnyOutboundMessage,
    InboundMessage,
    OutboundMessage,
    TMessage,
)
from ddht.endpoint import Endpoint
from ddht.typing import NodeID, SessionKeys
from ddht.v5_1.envelope import InboundEnvelope
from ddht.v5_1.messages import PingMessage, PongMessage


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

    ping_sent: EventAPI[OutboundMessage[PingMessage]]
    ping_received: EventAPI[InboundMessage[PingMessage]]

    pong_sent: EventAPI[OutboundMessage[PongMessage]]
    pong_received: EventAPI[InboundMessage[PongMessage]]


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


class DispatcherAPI(ServiceAPI):
    @abstractmethod
    async def send_message(self, message: AnyOutboundMessage) -> None:
        ...

    @abstractmethod
    def get_free_request_id(self, node_id: NodeID) -> int:
        ...

    @abstractmethod
    def reserve_request_id(self, node_id: NodeID) -> ContextManager[int]:
        ...

    @abstractmethod
    def subscribe(
        self,
        payload_type: Type[TMessage],
        endpoint: Optional[Endpoint],
        node_id: Optional[NodeID],
    ) -> AsyncContextManager[trio.abc.ReceiveChannel[InboundMessage[TMessage]]]:
        ...

    def subscribe_request(
        self, request: AnyOutboundMessage, response_payload_type: Type[TMessage],
    ) -> AsyncContextManager[
        trio.abc.ReceiveChannel[InboundMessage[TMessage]]
    ]:  # noqa: E501
        ...
