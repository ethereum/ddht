from abc import ABC, abstractmethod
from typing import (
    Any,
    AsyncContextManager,
    Collection,
    Iterable,
    Optional,
    Sequence,
    Tuple,
    Type,
)

from async_service import ServiceAPI
from eth_enr import ENRAPI, ENRManagerAPI, QueryableENRDatabaseAPI
from eth_typing import Hash32, NodeID
import trio

from ddht.abc import RequestTrackerAPI, RoutingTableAPI, SubscriptionManagerAPI
from ddht.base_message import InboundMessage
from ddht.endpoint import Endpoint
from ddht.v5_1.abc import NetworkAPI, TalkProtocolAPI
from ddht.v5_1.alexandria.advertisements import Advertisement
from ddht.v5_1.alexandria.constants import ONE_HOUR
from ddht.v5_1.alexandria.messages import (
    AckMessage,
    AlexandriaMessage,
    ContentMessage,
    FoundNodesMessage,
    PongMessage,
    TAlexandriaMessage,
)
from ddht.v5_1.alexandria.partials.proof import Proof
from ddht.v5_1.alexandria.payloads import AckPayload, PongPayload
from ddht.v5_1.alexandria.typing import ContentKey


class ContentStorageAPI(ABC):
    @abstractmethod
    def has_content(self, content_key: ContentKey) -> bool:
        ...

    @abstractmethod
    def get_content(self, content_key: ContentKey) -> bytes:
        ...

    @abstractmethod
    def set_content(self, content_key: ContentKey, content: bytes) -> None:
        ...

    @abstractmethod
    def delete_content(self, content_key: ContentKey) -> None:
        ...

    @abstractmethod
    def enumerate_keys(
        self,
        start_key: Optional[ContentKey] = None,
        end_key: Optional[ContentKey] = None,
    ) -> Iterable[ContentKey]:
        ...


class BroadcastLogAPI(ABC):
    @abstractmethod
    def log(self, node_id: NodeID, advertisement: Advertisement) -> None:
        ...

    @abstractmethod
    def was_logged(
        self, node_id: NodeID, advertisement: Advertisement, max_age: int = ONE_HOUR
    ) -> bool:
        ...


class ContentProviderAPI(ServiceAPI):
    @abstractmethod
    async def ready(self) -> None:
        ...


class AlexandriaClientAPI(ServiceAPI, TalkProtocolAPI):
    network: NetworkAPI
    request_tracker: RequestTrackerAPI
    subscription_manager: SubscriptionManagerAPI[AlexandriaMessage[Any]]

    #
    # Proxy API for subscriptions
    #
    @abstractmethod
    def subscribe(
        self,
        message_type: Type[TAlexandriaMessage],
        endpoint: Optional[Endpoint] = None,
        node_id: Optional[NodeID] = None,
    ) -> AsyncContextManager[
        trio.abc.ReceiveChannel[InboundMessage[TAlexandriaMessage]]
    ]:
        ...

    #
    # Low Level Message Sending
    #
    @abstractmethod
    async def send_ping(
        self,
        node_id: NodeID,
        endpoint: Endpoint,
        *,
        enr_seq: int,
        request_id: Optional[bytes] = None,
    ) -> bytes:
        ...

    @abstractmethod
    async def send_pong(
        self, node_id: NodeID, endpoint: Endpoint, *, enr_seq: int, request_id: bytes,
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
    async def send_get_content(
        self,
        node_id: NodeID,
        endpoint: Endpoint,
        *,
        content_key: ContentKey,
        start_chunk_index: int,
        max_chunks: int,
        request_id: Optional[bytes] = None,
    ) -> bytes:
        ...

    @abstractmethod
    async def send_content(
        self,
        node_id: NodeID,
        endpoint: Endpoint,
        *,
        is_proof: bool,
        payload: bytes,
        request_id: bytes,
    ) -> None:
        ...

    @abstractmethod
    async def send_advertisements(
        self,
        node_id: NodeID,
        endpoint: Endpoint,
        *,
        advertisements: Sequence[Advertisement],
        request_id: Optional[bytes] = None,
    ) -> bytes:
        ...

    @abstractmethod
    async def send_ack(
        self,
        node_id: NodeID,
        endpoint: Endpoint,
        *,
        advertisement_radius: int,
        request_id: bytes,
    ) -> None:
        ...

    #
    # High Level Request/Response
    #
    @abstractmethod
    async def ping(self, node_id: NodeID, endpoint: Endpoint) -> PongMessage:
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
    async def get_content(
        self,
        node_id: NodeID,
        endpoint: Endpoint,
        *,
        content_key: ContentKey,
        start_chunk_index: int,
        max_chunks: int,
        request_id: Optional[bytes] = None,
    ) -> ContentMessage:
        ...

    @abstractmethod
    async def advertise(
        self,
        node_id: NodeID,
        endpoint: Endpoint,
        *,
        advertisements: Collection[Advertisement],
    ) -> Tuple[AckMessage, ...]:
        ...


class AlexandriaNetworkAPI(ServiceAPI, TalkProtocolAPI):
    client: AlexandriaClientAPI
    routing_table: RoutingTableAPI

    content_storage: ContentStorageAPI
    content_provider: ContentProviderAPI

    @property
    @abstractmethod
    def network(self) -> NetworkAPI:
        ...

    @property
    @abstractmethod
    def local_node_id(self) -> NodeID:
        ...

    @property
    @abstractmethod
    def enr_manager(self) -> ENRManagerAPI:
        ...

    @property
    @abstractmethod
    def enr_db(self) -> QueryableENRDatabaseAPI:
        ...

    #
    # High Level Request/Response
    #
    @abstractmethod
    async def bond(
        self, node_id: NodeID, *, endpoint: Optional[Endpoint] = None,
    ) -> bool:
        ...

    @abstractmethod
    async def ping(
        self, node_id: NodeID, *, endpoint: Optional[Endpoint] = None,
    ) -> PongPayload:
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

    async def get_content_proof(
        self,
        node_id: NodeID,
        *,
        hash_tree_root: Hash32,
        content_key: ContentKey,
        start_chunk_index: int,
        max_chunks: int,
        endpoint: Optional[Endpoint] = None,
        request_id: Optional[bytes] = None,
    ) -> Proof:
        ...

    @abstractmethod
    async def get_content_from_nodes(
        self,
        nodes: Collection[Tuple[NodeID, Optional[Endpoint]]],
        *,
        hash_tree_root: Hash32,
        content_key: ContentKey,
        concurrency: int = 4,
    ) -> Proof:
        ...

    @abstractmethod
    async def advertise(
        self,
        node_id: NodeID,
        *,
        advertisements: Collection[Advertisement],
        endpoint: Optional[Endpoint] = None,
    ) -> Tuple[AckPayload, ...]:
        ...

    @abstractmethod
    def recursive_find_nodes(
        self, target: NodeID
    ) -> AsyncContextManager[trio.abc.ReceiveChannel[ENRAPI]]:
        ...
