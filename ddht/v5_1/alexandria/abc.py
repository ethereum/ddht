from abc import ABC, abstractmethod
from typing import (
    Any,
    AsyncContextManager,
    Collection,
    ContextManager,
    Iterable,
    Optional,
    Sequence,
    Sized,
    Tuple,
    Type,
    Union,
)

from async_service import ServiceAPI
from eth_enr import ENRAPI, ENRManagerAPI, QueryableENRDatabaseAPI
from eth_keys import keys
from eth_typing import Hash32, NodeID
import trio

from ddht.abc import (
    EventAPI,
    RequestTrackerAPI,
    ResourceQueueAPI,
    RoutingTableAPI,
    SubscriptionManagerAPI,
)
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
    LocationsMessage,
    PongMessage,
    TAlexandriaMessage,
)
from ddht.v5_1.alexandria.partials.chunking import MissingSegment
from ddht.v5_1.alexandria.partials.proof import Proof
from ddht.v5_1.alexandria.payloads import AckPayload, PongPayload
from ddht.v5_1.alexandria.typing import ContentID, ContentKey


class ContentStorageAPI(Sized):
    @abstractmethod
    def total_size(self) -> int:
        ...

    @abstractmethod
    def has_content(self, content_key: ContentKey) -> bool:
        ...

    @abstractmethod
    def get_content(self, content_key: ContentKey) -> bytes:
        ...

    @abstractmethod
    def set_content(
        self, content_key: ContentKey, content: bytes, exists_ok: bool = False
    ) -> None:
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

    @abstractmethod
    def atomic(self) -> ContextManager["ContentStorageAPI"]:
        ...

    @abstractmethod
    def iter_furthest(self, target: NodeID) -> Iterable[ContentKey]:
        ...

    @abstractmethod
    def iter_closest(self, target: NodeID) -> Iterable[ContentKey]:
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


class ContentValidatorAPI(ABC):
    @abstractmethod
    async def validate_content(self, content_key: ContentKey, content: bytes) -> None:
        ...

    async def validate_header(self, content_key: ContentKey, content: bytes) -> None:
        ...


class ContentProviderAPI(ServiceAPI):
    @abstractmethod
    async def ready(self) -> None:
        ...


class AdvertisementProviderAPI(ServiceAPI):
    @abstractmethod
    async def ready(self) -> None:
        ...


class RadiusTrackerAPI(ServiceAPI):
    @abstractmethod
    async def ready(self) -> None:
        ...

    @abstractmethod
    async def get_advertisement_radius(self, node_id: NodeID) -> int:
        ...


class AdvertisementDatabaseAPI(ABC):
    @abstractmethod
    def exists(self, advertisement: Advertisement) -> bool:
        ...

    @abstractmethod
    def add(self, advertisement: Advertisement) -> None:
        ...

    @abstractmethod
    def remove(self, advertisement: Advertisement) -> bool:
        ...

    @abstractmethod
    def query(
        self,
        node_id: Optional[NodeID] = None,
        content_id: Optional[ContentID] = None,
        content_key: Optional[bytes] = None,
        hash_tree_root: Optional[Hash32] = None,
    ) -> Iterable[Advertisement]:
        ...

    @abstractmethod
    def closest(self, node_id: NodeID) -> Iterable[Advertisement]:
        ...

    @abstractmethod
    def furthest(self, node_id: NodeID) -> Iterable[Advertisement]:
        ...

    @abstractmethod
    def get_hash_tree_roots_for_content_id(
        self, content_id: ContentID
    ) -> Iterable[Hash32]:
        ...

    @abstractmethod
    def count(self) -> int:
        ...

    @abstractmethod
    def expired(self) -> Iterable[Advertisement]:
        ...


class ContentManagerAPI(ServiceAPI):
    content_storage: ContentStorageAPI
    max_size: Optional[int]

    @property
    @abstractmethod
    def content_radius(self) -> int:
        ...

    @property
    @abstractmethod
    def is_full(self) -> bool:
        ...

    @abstractmethod
    async def process_content(self, content_key: ContentKey, content: bytes) -> None:
        ...


class AdvertisementCollectorAPI(ServiceAPI):
    new_advertisement: EventAPI[Advertisement]

    @abstractmethod
    async def ready(self) -> None:
        ...

    @abstractmethod
    def check_interest(self, advertisement: Advertisement) -> bool:
        ...

    @abstractmethod
    async def validate_advertisement(self, advertisement: Advertisement) -> None:
        ...

    @abstractmethod
    async def handle_advertisement(self, advertisement: Advertisement) -> bool:
        ...


class AdvertisementManagerAPI(ServiceAPI):
    advertisement_db: AdvertisementDatabaseAPI
    max_advertisement_count: Optional[int]

    @property
    @abstractmethod
    def advertisement_radius(self) -> int:
        ...

    @abstractmethod
    async def purge_expired_ads(self) -> None:
        ...

    @abstractmethod
    async def purge_distant_ads(self) -> None:
        ...

    @abstractmethod
    async def ready(self) -> None:
        ...


class ContentCollectorAPI(ServiceAPI):
    content_manager: ContentManagerAPI

    @property
    @abstractmethod
    def content_storage(self) -> ContentStorageAPI:
        ...

    @abstractmethod
    async def ready(self) -> None:
        ...


class AlexandriaClientAPI(ServiceAPI, TalkProtocolAPI):
    network: NetworkAPI
    request_tracker: RequestTrackerAPI
    subscription_manager: SubscriptionManagerAPI[AlexandriaMessage[Any]]

    @property
    @abstractmethod
    def local_private_key(self) -> keys.PrivateKey:
        ...

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
        advertisement_radius: int,
        request_id: Optional[bytes] = None,
    ) -> bytes:
        ...

    @abstractmethod
    async def send_pong(
        self,
        node_id: NodeID,
        endpoint: Endpoint,
        *,
        enr_seq: int,
        advertisement_radius: int,
        request_id: bytes,
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
        acked: Tuple[bool, ...],
        request_id: bytes,
    ) -> None:
        ...

    @abstractmethod
    async def send_locate(
        self,
        node_id: NodeID,
        endpoint: Endpoint,
        *,
        content_key: ContentKey,
        request_id: bytes,
    ) -> bytes:
        ...

    @abstractmethod
    async def send_locations(
        self,
        node_id: NodeID,
        endpoint: Endpoint,
        *,
        advertisements: Sequence[Advertisement],
        request_id: bytes,
    ) -> int:
        ...

    #
    # High Level Request/Response
    #
    @abstractmethod
    async def ping(
        self,
        node_id: NodeID,
        endpoint: Endpoint,
        enr_seq: int,
        advertisement_radius: int,
        request_id: Optional[bytes] = None,
    ) -> PongMessage:
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
    ) -> AckMessage:
        ...

    @abstractmethod
    async def locate(
        self,
        node_id: NodeID,
        endpoint: Endpoint,
        *,
        content_key: ContentKey,
        request_id: Optional[bytes] = None,
    ) -> Tuple[InboundMessage[LocationsMessage], ...]:
        ...

    @abstractmethod
    def stream_locate(
        self,
        node_id: NodeID,
        endpoint: Endpoint,
        *,
        content_key: ContentKey,
        request_id: Optional[bytes] = None,
    ) -> AsyncContextManager[trio.abc.ReceiveChannel[InboundMessage[LocationsMessage]]]:
        ...


class ContentRetrievalAPI(ServiceAPI):
    content_key: ContentKey
    content_id: ContentID
    hash_tree_root: Hash32

    node_queue: ResourceQueueAPI[NodeID]
    segment_queue: ResourceQueueAPI[MissingSegment]

    @abstractmethod
    async def wait_content_proof(self) -> Proof:
        ...


class AlexandriaNetworkAPI(ServiceAPI, TalkProtocolAPI):
    client: AlexandriaClientAPI
    routing_table: RoutingTableAPI

    max_advertisement_count: int

    radius_tracker: RadiusTrackerAPI
    broadcast_log: BroadcastLogAPI

    local_advertisement_db: AdvertisementDatabaseAPI
    local_advertisement_manager: AdvertisementManagerAPI

    remote_advertisement_db: AdvertisementDatabaseAPI
    remote_advertisement_manager: AdvertisementManagerAPI

    advertisement_provider: AdvertisementProviderAPI
    advertisement_collector: AdvertisementCollectorAPI

    content_validator: ContentValidatorAPI

    content_storage: ContentStorageAPI
    content_provider: ContentProviderAPI

    commons_content_storage: ContentStorageAPI
    commons_content_manager: ContentManagerAPI

    pinned_content_storage: ContentStorageAPI
    pinned_content_manager: ContentManagerAPI

    @abstractmethod
    async def routing_table_ready(self) -> None:
        ...

    @abstractmethod
    async def ready(self) -> None:
        ...

    #
    # Properties from base network
    #
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
    # Local properties
    #
    @property
    @abstractmethod
    def local_advertisement_radius(self) -> int:
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
    async def lookup_enr(
        self, node_id: NodeID, *, enr_seq: int = 0, endpoint: Optional[Endpoint] = None
    ) -> ENRAPI:
        ...

    @abstractmethod
    async def ping(
        self,
        node_id: NodeID,
        *,
        enr_seq: Optional[int] = None,
        advertisement_radius: Optional[int] = None,
        endpoint: Optional[Endpoint] = None,
        request_id: Optional[bytes] = None,
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
    def retrieve_content(
        self, content_key: ContentKey, hash_tree_root: Hash32
    ) -> AsyncContextManager[ContentRetrievalAPI]:
        ...

    @abstractmethod
    async def get_content(
        self, content_key: ContentKey, hash_tree_root: Hash32, *, concurrency: int = 3,
    ) -> Proof:
        ...

    @abstractmethod
    async def advertise(
        self,
        node_id: NodeID,
        *,
        advertisements: Collection[Advertisement],
        endpoint: Optional[Endpoint] = None,
    ) -> AckPayload:
        ...

    @abstractmethod
    async def locate(
        self,
        node_id: NodeID,
        *,
        content_key: ContentKey,
        endpoint: Optional[Endpoint] = None,
        request_id: Optional[bytes] = None,
    ) -> Tuple[Advertisement, ...]:
        ...

    @abstractmethod
    def stream_locate(
        self,
        node_id: NodeID,
        *,
        content_key: ContentKey,
        endpoint: Optional[Endpoint] = None,
        request_id: Optional[bytes] = None,
    ) -> AsyncContextManager[trio.abc.ReceiveChannel[Advertisement]]:
        ...

    @abstractmethod
    def stream_locations(
        self,
        content_key: ContentKey,
        *,
        hash_tree_root: Optional[Hash32] = None,
        concurrency: int = 3,
    ) -> AsyncContextManager[trio.abc.ReceiveChannel[Advertisement]]:
        ...

    @abstractmethod
    def recursive_find_nodes(
        self, target: Union[NodeID, ContentID],
    ) -> AsyncContextManager[trio.abc.ReceiveChannel[ENRAPI]]:
        ...

    @abstractmethod
    def explore(
        self, target: Union[NodeID, ContentID], concurrency: int = 3,
    ) -> AsyncContextManager[trio.abc.ReceiveChannel[ENRAPI]]:
        ...

    @abstractmethod
    async def broadcast(
        self, advertisement: Advertisement, redundancy_factor: int = 3
    ) -> Tuple[NodeID, ...]:
        ...
