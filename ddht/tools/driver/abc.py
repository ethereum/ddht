from abc import ABC, abstractmethod
from typing import AsyncContextManager, Collection, NamedTuple, Optional, Tuple

from eth_enr import ENRAPI, QueryableENRDatabaseAPI
from eth_keys import keys
from eth_typing import NodeID

from ddht.base_message import AnyInboundMessage, BaseMessage
from ddht.endpoint import Endpoint
from ddht.tools.factories.v5_1 import SessionChannels
from ddht.v5_1.abc import (
    ClientAPI,
    DispatcherAPI,
    EventsAPI,
    NetworkAPI,
    PoolAPI,
    SessionAPI,
)
from ddht.v5_1.alexandria.abc import (
    AdvertisementDatabaseAPI,
    AlexandriaClientAPI,
    AlexandriaNetworkAPI,
    ContentStorageAPI,
)
from ddht.v5_1.envelope import InboundEnvelope, OutboundEnvelope
from ddht.v5_1.messages import PingMessage, PongMessage
from ddht.v5_1.packets import AnyPacket


class AlexandriaNodeAPI(ABC):
    commons_content_storage: ContentStorageAPI
    pinned_content_storage: ContentStorageAPI
    advertisement_db: AdvertisementDatabaseAPI

    @property
    @abstractmethod
    def enr(self) -> ENRAPI:
        ...

    @abstractmethod
    def client(
        self, network: Optional[NetworkAPI] = None
    ) -> AsyncContextManager[AlexandriaClientAPI]:
        ...

    @abstractmethod
    def network(
        self,
        network: Optional[NetworkAPI] = None,
        bootnodes: Optional[Collection[ENRAPI]] = None,
    ) -> AsyncContextManager[AlexandriaNetworkAPI]:
        ...


class NodeAPI(ABC):
    private_key: keys.PrivateKey
    enr: ENRAPI
    enr_db: QueryableENRDatabaseAPI
    events: EventsAPI
    alexandria: AlexandriaNodeAPI

    @property
    @abstractmethod
    def endpoint(self) -> Endpoint:
        ...

    @property
    @abstractmethod
    def node_id(self) -> NodeID:
        ...

    @abstractmethod
    def client(self) -> AsyncContextManager[ClientAPI]:
        ...

    @abstractmethod
    def network(
        self, bootnodes: Collection[ENRAPI] = ()
    ) -> AsyncContextManager[NetworkAPI]:
        ...


class SessionDriverAPI(ABC):
    session: SessionAPI
    node: NodeAPI
    remote: NodeAPI
    channels: SessionChannels

    @property
    @abstractmethod
    def events(self) -> EventsAPI:
        ...

    @abstractmethod
    async def send_message(self, message: BaseMessage) -> None:
        ...

    @abstractmethod
    async def next_message(self) -> AnyInboundMessage:
        ...

    @abstractmethod
    async def send_ping(self, request_id: Optional[bytes] = None) -> PingMessage:
        ...

    @abstractmethod
    async def send_pong(self, request_id: Optional[bytes] = None) -> PongMessage:
        ...


class EnvelopePair(NamedTuple):
    outbound: OutboundEnvelope
    inbound: InboundEnvelope

    @property
    def packet(self) -> AnyPacket:
        return self.outbound.packet


class SessionPairAPI(ABC):
    initiator: SessionDriverAPI
    recipient: SessionDriverAPI

    @abstractmethod
    async def transmit_one(self, source: SessionDriverAPI) -> EnvelopePair:
        ...

    @abstractmethod
    def transmit(self) -> AsyncContextManager[None]:
        ...

    @abstractmethod
    async def handshake(self) -> None:
        ...


class AlexandriaTesterAPI(ABC):
    @abstractmethod
    def node(self) -> AlexandriaNodeAPI:
        ...

    @abstractmethod
    def network_group(
        self, num_networks: int, bootnodes: Collection[ENRAPI] = (),
    ) -> AsyncContextManager[Tuple[AlexandriaNetworkAPI, ...]]:
        ...


class TesterAPI(ABC):
    alexandria: AlexandriaTesterAPI

    @abstractmethod
    def register_pool(self, pool: PoolAPI, channels: SessionChannels) -> None:
        ...

    @abstractmethod
    def node(self) -> NodeAPI:
        ...

    @abstractmethod
    def network_group(
        self, num_networks: int, bootnodes: Collection[ENRAPI] = (),
    ) -> AsyncContextManager[Tuple[NetworkAPI, ...]]:
        ...

    @abstractmethod
    def session_pair(
        self,
        initiator: Optional[NodeAPI] = None,
        recipient: Optional[NodeAPI] = None,
        initiator_session: Optional[SessionAPI] = None,
        recipient_session: Optional[SessionAPI] = None,
    ) -> SessionPairAPI:
        ...

    def dispatcher_pair(
        self, node_a: NodeAPI, node_b: NodeAPI,
    ) -> AsyncContextManager[Tuple[DispatcherAPI, DispatcherAPI]]:
        ...
