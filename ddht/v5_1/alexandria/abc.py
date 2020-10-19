from abc import abstractmethod
from typing import Any, Optional, Tuple

from async_service import ServiceAPI
from eth_enr import ENRAPI, ENRDatabaseAPI, ENRManagerAPI
from eth_typing import NodeID

from ddht.abc import RequestTrackerAPI, RoutingTableAPI, SubscriptionManagerAPI
from ddht.endpoint import Endpoint
from ddht.v5_1.abc import (
    FindNodesClientAPI,
    NetworkAPI,
    PingPongClientAPI,
    TalkProtocolAPI,
)
from ddht.v5_1.alexandria.messages import (
    AlexandriaMessage,
    FoundNodesMessage,
    PongMessage,
)
from ddht.v5_1.alexandria.payloads import PongPayload


class AlexandriaClientAPI(
    ServiceAPI,
    TalkProtocolAPI,
    PingPongClientAPI[PongMessage],
    FindNodesClientAPI[FoundNodesMessage],
):
    network: NetworkAPI
    request_tracker: RequestTrackerAPI
    subscription_manager: SubscriptionManagerAPI[AlexandriaMessage[Any]]


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
    async def get_enr(
        self, node_id: NodeID, *, enr_seq: int = 0, endpoint: Optional[Endpoint] = None
    ) -> ENRAPI:
        ...

    @abstractmethod
    async def recursive_find_nodes(self, target: NodeID) -> Tuple[ENRAPI, ...]:
        ...
