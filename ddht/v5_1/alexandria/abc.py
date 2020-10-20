from abc import abstractmethod
from typing import Any, AsyncContextManager, Optional, Type

from async_service import ServiceAPI
from eth_enr import ENRDatabaseAPI, ENRManagerAPI
from eth_typing import NodeID
import trio

from ddht.abc import RequestTrackerAPI, SubscriptionManagerAPI
from ddht.base_message import InboundMessage
from ddht.endpoint import Endpoint
from ddht.v5_1.abc import NetworkAPI, TalkProtocolAPI
from ddht.v5_1.alexandria.messages import (
    AlexandriaMessage,
    PongMessage,
    TAlexandriaMessage,
)
from ddht.v5_1.alexandria.payloads import PongPayload


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

    #
    # High Level Request/Response
    #
    @abstractmethod
    async def ping(self, node_id: NodeID, endpoint: Endpoint,) -> PongMessage:
        ...


class AlexandriaNetworkAPI(ServiceAPI, TalkProtocolAPI):
    client: AlexandriaClientAPI

    @property
    @abstractmethod
    def network(self) -> NetworkAPI:
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
