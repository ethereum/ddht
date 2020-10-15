from abc import ABC, abstractmethod
import logging
from typing import Any, AsyncContextManager, Collection, Optional, Sequence, Tuple, Type
import uuid

from async_service import ServiceAPI
from eth_enr import ENRAPI, ENRDatabaseAPI, ENRManagerAPI, IdentitySchemeAPI
from eth_keys import keys
from eth_typing import NodeID
import trio

from ddht.abc import (
    EventAPI,
    HandshakeSchemeAPI,
    RequestTrackerAPI,
    RoutingTableAPI,
    SubscriptionManagerAPI,
)
from ddht.base_message import (
    AnyOutboundMessage,
    BaseMessage,
    InboundMessage,
    OutboundMessage,
    TBaseMessage,
)
from ddht.endpoint import Endpoint
from ddht.typing import SessionKeys
from ddht.v5_1.envelope import InboundEnvelope, OutboundEnvelope
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

    @property
    @abstractmethod
    def is_recipient(self) -> bool:
        ...

    created_at: float

    @property
    @abstractmethod
    def remote_node_id(self) -> NodeID:
        ...

    @property
    @abstractmethod
    def identity_scheme(self) -> Type[IdentitySchemeAPI]:
        ...

    @property
    @abstractmethod
    def handshake_scheme(self) -> Type[HandshakeSchemeAPI[Any]]:
        ...

    @property
    @abstractmethod
    def keys(self) -> SessionKeys:
        ...

    #
    # Timeouts
    #
    @property
    @abstractmethod
    def is_timed_out(self) -> bool:
        ...

    @property
    @abstractmethod
    def timeout_at(self) -> float:
        ...

    @property
    @abstractmethod
    def last_message_received_at(self) -> float:
        ...

    #
    # Handshake Status
    #
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

    #
    # Message and Envelope handlers
    #
    @abstractmethod
    async def handle_outbound_message(self, message: AnyOutboundMessage) -> None:
        ...

    @abstractmethod
    async def handle_inbound_envelope(self, envelope: InboundEnvelope) -> bool:
        ...


class EventsAPI(ABC):
    session_created: EventAPI[SessionAPI]
    session_handshake_complete: EventAPI[SessionAPI]
    session_timeout: EventAPI[SessionAPI]

    packet_sent: EventAPI[Tuple[SessionAPI, OutboundEnvelope]]
    packet_received: EventAPI[Tuple[SessionAPI, InboundEnvelope]]
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
    subscription_manager: SubscriptionManagerAPI[BaseMessage]

    @abstractmethod
    async def send_message(self, message: AnyOutboundMessage) -> None:
        ...

    @abstractmethod
    def subscribe(
        self,
        message_type: Type[TBaseMessage],
        endpoint: Optional[Endpoint] = None,
        node_id: Optional[NodeID] = None,
    ) -> AsyncContextManager[trio.abc.ReceiveChannel[InboundMessage[TBaseMessage]]]:
        ...

    @abstractmethod
    def subscribe_request(
        self, request: AnyOutboundMessage, response_message_type: Type[TBaseMessage],
    ) -> AsyncContextManager[trio.abc.ReceiveChannel[InboundMessage[TBaseMessage]]]:
        ...


class ClientAPI(ServiceAPI):
    enr_manager: ENRManagerAPI
    events: EventsAPI
    dispatcher: DispatcherAPI
    pool: PoolAPI
    enr_db: ENRDatabaseAPI
    request_tracker: RequestTrackerAPI

    @property
    @abstractmethod
    def local_node_id(self) -> NodeID:
        ...

    @abstractmethod
    async def wait_listening(self) -> None:
        ...

    #
    # Message Sending API
    #
    @abstractmethod
    async def send_ping(
        self,
        node_id: NodeID,
        endpoint: Endpoint,
        *,
        request_id: Optional[bytes] = None,
    ) -> bytes:
        ...

    @abstractmethod
    async def send_pong(
        self, node_id: NodeID, endpoint: Endpoint, *, request_id: bytes,
    ) -> None:
        ...

    @abstractmethod
    async def send_find_nodes(
        self,
        node_id: NodeID,
        endpoint: Endpoint,
        *,
        distances: Collection[int],
        request_id: Optional[bytes] = None,
    ) -> bytes:
        ...

    @abstractmethod
    async def send_found_nodes(
        self,
        node_id: NodeID,
        endpoint: Endpoint,
        *,
        enrs: Sequence[ENRAPI],
        request_id: bytes,
    ) -> int:
        ...

    @abstractmethod
    async def send_talk_request(
        self,
        node_id: NodeID,
        endpoint: Endpoint,
        *,
        protocol: bytes,
        payload: bytes,
        request_id: Optional[bytes] = None,
    ) -> bytes:
        ...

    @abstractmethod
    async def send_talk_response(
        self, node_id: NodeID, endpoint: Endpoint, *, payload: bytes, request_id: bytes,
    ) -> None:
        ...

    @abstractmethod
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
        ...

    @abstractmethod
    async def send_ticket(
        self,
        node_id: NodeID,
        endpoint: Endpoint,
        *,
        ticket: bytes,
        wait_time: int,
        request_id: bytes,
    ) -> None:
        ...

    @abstractmethod
    async def send_registration_confirmation(
        self, node_id: NodeID, endpoint: Endpoint, *, topic: bytes, request_id: bytes,
    ) -> None:
        ...

    @abstractmethod
    async def send_topic_query(
        self,
        node_id: NodeID,
        endpoint: Endpoint,
        *,
        topic: bytes,
        request_id: Optional[bytes] = None,
    ) -> bytes:
        ...

    #
    # Request/Response API
    #
    @abstractmethod
    async def ping(
        self,
        node_id: NodeID,
        endpoint: Endpoint,
        *,
        request_id: Optional[bytes] = None,
    ) -> InboundMessage[PongMessage]:
        ...

    @abstractmethod
    async def find_nodes(
        self,
        node_id: NodeID,
        endpoint: Endpoint,
        distances: Collection[int],
        *,
        request_id: Optional[bytes] = None,
    ) -> Tuple[InboundMessage[FoundNodesMessage], ...]:
        ...

    @abstractmethod
    async def talk(
        self,
        node_id: NodeID,
        endpoint: Endpoint,
        protocol: bytes,
        payload: bytes,
        *,
        request_id: Optional[bytes] = None,
    ) -> InboundMessage[TalkResponseMessage]:
        ...

    @abstractmethod
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
        ...

    @abstractmethod
    async def topic_query(
        self,
        node_id: NodeID,
        endpoint: Endpoint,
        topic: bytes,
        *,
        request_id: Optional[bytes] = None,
    ) -> InboundMessage[FoundNodesMessage]:
        ...


class TalkProtocolAPI(ABC):
    protocol_id: bytes


class NetworkAPI(ServiceAPI):
    client: ClientAPI
    routing_table: RoutingTableAPI

    #
    # Proxied ClientAPI properties
    #
    @property
    @abstractmethod
    def local_node_id(self) -> NodeID:
        ...

    @property
    @abstractmethod
    def events(self) -> EventsAPI:
        ...

    @property
    @abstractmethod
    def dispatcher(self) -> DispatcherAPI:
        ...

    @property
    @abstractmethod
    def enr_manager(self) -> ENRManagerAPI:
        ...

    @property
    @abstractmethod
    def pool(self) -> PoolAPI:
        ...

    @property
    @abstractmethod
    def enr_db(self) -> ENRDatabaseAPI:
        ...

    #
    # TALK API
    #
    @abstractmethod
    def add_talk_protocol(self, protocol: TalkProtocolAPI) -> None:
        ...

    #
    # High Level API
    #
    @abstractmethod
    async def bond(
        self, node_id: NodeID, *, endpoint: Optional[Endpoint] = None,
    ) -> bool:
        ...

    @abstractmethod
    async def ping(
        self,
        node_id: NodeID,
        *,
        endpoint: Optional[Endpoint] = None,
        request_id: Optional[bytes] = None,
    ) -> PongMessage:
        ...

    @abstractmethod
    async def find_nodes(
        self,
        node_id: NodeID,
        *distances: int,
        endpoint: Optional[Endpoint] = None,
        request_id: Optional[bytes] = None,
    ) -> Tuple[ENRAPI, ...]:
        ...

    @abstractmethod
    async def talk(
        self,
        node_id: NodeID,
        *,
        protocol: bytes,
        payload: bytes,
        endpoint: Optional[Endpoint] = None,
        request_id: Optional[bytes] = None,
    ) -> bytes:
        ...

    @abstractmethod
    async def lookup_enr(
        self, node_id: NodeID, *, enr_seq: int = 0, endpoint: Optional[Endpoint] = None
    ) -> ENRAPI:
        ...

    @abstractmethod
    async def recursive_find_nodes(self, target: NodeID) -> Tuple[ENRAPI, ...]:
        ...
