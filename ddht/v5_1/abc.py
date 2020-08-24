from abc import ABC, abstractmethod
import logging
from typing import AsyncContextManager, ContextManager, Optional, Sequence, Tuple, Type
import uuid

from async_service import ServiceAPI
from eth_keys import keys
import trio

from ddht.abc import ENRManagerAPI, EventAPI
from ddht.base_message import (
    AnyOutboundMessage,
    InboundMessage,
    OutboundMessage,
    TMessage,
)
from ddht.endpoint import Endpoint
from ddht.enr import ENR
from ddht.typing import NodeID, SessionKeys
from ddht.v5_1.envelope import InboundEnvelope
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

    find_nodes_sent: EventAPI[OutboundMessage[FindNodeMessage]]
    find_nodes_received: EventAPI[InboundMessage[FindNodeMessage]]

    found_nodes_sent: EventAPI[OutboundMessage[FoundNodesMessage]]
    found_nodes_received: EventAPI[InboundMessage[FoundNodesMessage]]

    talk_request_sent: EventAPI[OutboundMessage[TalkRequestMessage]]
    talk_request_received: EventAPI[InboundMessage[TalkRequestMessage]]

    talk_response_sent: EventAPI[OutboundMessage[TalkResponseMessage]]
    talk_response_received: EventAPI[InboundMessage[TalkResponseMessage]]

    register_topic_sent: EventAPI[OutboundMessage[RegisterTopicMessage]]
    register_topic_received: EventAPI[InboundMessage[RegisterTopicMessage]]

    ticket_sent: EventAPI[OutboundMessage[TicketMessage]]
    ticket_received: EventAPI[InboundMessage[TicketMessage]]

    registration_confirmation_sent: EventAPI[
        OutboundMessage[RegistrationConfirmationMessage]
    ]
    registration_confirmation_received: EventAPI[
        InboundMessage[RegistrationConfirmationMessage]
    ]

    topic_query_sent: EventAPI[OutboundMessage[TopicQueryMessage]]
    topic_query_received: EventAPI[InboundMessage[TopicQueryMessage]]


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


class ClientAPI(ServiceAPI):
    enr_manager: ENRManagerAPI
    events: EventsAPI
    message_dispatcher: DispatcherAPI
    pool: PoolAPI

    @abstractmethod
    async def wait_listening(self) -> None:
        ...

    @abstractmethod
    async def send_ping(
        self,
        dest_endpoint: Endpoint,
        dest_node_id: NodeID,
        *,
        request_id: Optional[int] = None,
    ) -> int:
        ...

    async def send_pong(
        self, dest_endpoint: Endpoint, dest_node_id: NodeID, *, request_id: int,
    ) -> None:
        ...

    async def send_find_nodes(
        self,
        dest_endpoint: Endpoint,
        dest_node_id: NodeID,
        *,
        distance: int,
        request_id: Optional[int] = None,
    ) -> int:
        ...

    async def send_found_nodes(
        self,
        dest_endpoint: Endpoint,
        dest_node_id: NodeID,
        *,
        enrs: Sequence[ENR],
        request_id: int,
    ) -> int:
        ...

    async def send_talk_request(
        self,
        dest_endpoint: Endpoint,
        dest_node_id: NodeID,
        *,
        protocol: bytes,
        request: bytes,
        request_id: Optional[int] = None,
    ) -> int:
        ...

    async def send_talk_response(
        self,
        dest_endpoint: Endpoint,
        dest_node_id: NodeID,
        *,
        response: bytes,
        request_id: int,
    ) -> None:
        ...

    async def send_register_topic(
        self,
        dest_endpoint: Endpoint,
        dest_node_id: NodeID,
        *,
        topic: bytes,
        enr: ENR,
        ticket: bytes = b"",
        request_id: Optional[int] = None,
    ) -> int:
        ...

    async def send_ticket(
        self,
        dest_endpoint: Endpoint,
        dest_node_id: NodeID,
        *,
        ticket: bytes,
        wait_time: int,
        request_id: int,
    ) -> None:
        ...

    async def send_registration_confirmation(
        self,
        dest_endpoint: Endpoint,
        dest_node_id: NodeID,
        *,
        topic: bytes,
        request_id: int,
    ) -> None:
        ...

    async def send_topic_query(
        self,
        dest_endpoint: Endpoint,
        dest_node_id: NodeID,
        *,
        topic: bytes,
        request_id: int,
    ) -> None:
        ...
