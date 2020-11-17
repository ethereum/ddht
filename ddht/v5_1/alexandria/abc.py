from abc import abstractmethod
from typing import Any, AsyncContextManager, Collection, Optional, Sequence, Tuple, Type

from async_service import ServiceAPI
from eth_enr import ENRAPI, ENRDatabaseAPI, ENRManagerAPI
from eth_typing import NodeID
import trio

from ddht.abc import RequestTrackerAPI, RoutingTableAPI, SubscriptionManagerAPI
from ddht.base_message import InboundMessage
from ddht.endpoint import Endpoint
from ddht.v5_1.abc import NetworkAPI, TalkProtocolAPI
from ddht.v5_1.alexandria.messages import (
    AlexandriaMessage,
    ContentMessage,
    FoundNodesMessage,
    PongMessage,
    TAlexandriaMessage,
)
from ddht.v5_1.alexandria.payloads import PongPayload
from ddht.v5_1.alexandria.typing import ContentID


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
        content_id: ContentID,
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
        content_id: ContentID,
        start_chunk_index: int,
        max_chunks: int,
        request_id: Optional[bytes] = None,
    ) -> ContentMessage:
        ...


class AlexandriaNetworkAPI(ServiceAPI, TalkProtocolAPI):
    client: AlexandriaClientAPI
    routing_table: RoutingTableAPI

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
    def enr_db(self) -> ENRDatabaseAPI:
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

    @abstractmethod
    async def recursive_find_nodes(self, target: NodeID) -> Tuple[ENRAPI, ...]:
        ...
