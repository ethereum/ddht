from abc import abstractmethod
from typing import (
    Any,
    AsyncContextManager,
    Collection,
    Optional,
    Sequence,
    Tuple,
    Type,
    Union,
)

from async_service import ServiceAPI
from eth_enr import ENRAPI, ENRManagerAPI, QueryableENRDatabaseAPI
from eth_keys import keys
from eth_typing import NodeID
import trio

from ddht.abc import RequestTrackerAPI, RoutingTableAPI, SubscriptionManagerAPI
from ddht.base_message import InboundMessage
from ddht.endpoint import Endpoint
from ddht.v5_1.abc import NetworkAPI, TalkProtocolAPI
from ddht.v5_1.alexandria.messages import (
    AlexandriaMessage,
    FoundContentMessage,
    FoundNodesMessage,
    PongMessage,
    TAlexandriaMessage,
)
from ddht.v5_1.alexandria.payloads import FoundContentPayload, PongPayload
from ddht.v5_1.alexandria.typing import ContentID, ContentKey


class RadiusTrackerAPI(ServiceAPI):
    @abstractmethod
    async def ready(self) -> None:
        ...

    @abstractmethod
    async def get_advertisement_radius(self, node_id: NodeID) -> int:
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
    async def send_find_content(
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
    async def send_found_content(
        self,
        node_id: NodeID,
        endpoint: Endpoint,
        *,
        enrs: Optional[Sequence[ENRAPI]] = None,
        content: Optional[bytes] = None,
        request_id: bytes,
    ) -> None:
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
    async def find_content(
        self,
        node_id: NodeID,
        endpoint: Endpoint,
        *,
        content_key: ContentKey,
        request_id: Optional[bytes] = None,
    ) -> FoundContentMessage:
        ...


class AlexandriaNetworkAPI(ServiceAPI, TalkProtocolAPI):
    client: AlexandriaClientAPI
    routing_table: RoutingTableAPI

    max_advertisement_count: int

    radius_tracker: RadiusTrackerAPI

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
    async def find_content(
        self,
        node_id: NodeID,
        *,
        content_key: ContentKey,
        endpoint: Optional[Endpoint] = None,
        request_id: Optional[bytes] = None,
    ) -> FoundContentPayload:
        ...

    @abstractmethod
    async def recursive_find_content(self, *, content_key: ContentKey,) -> bytes:
        ...
