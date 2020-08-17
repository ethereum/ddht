from abc import ABC, abstractmethod
from typing import AsyncContextManager, NamedTuple, Optional, Tuple

from eth_keys import keys

from ddht.abc import NodeDBAPI
from ddht.base_message import AnyInboundMessage, BaseMessage
from ddht.endpoint import Endpoint
from ddht.enr import ENR
from ddht.tools.factories.v5_1 import SessionChannels
from ddht.typing import NodeID
from ddht.v5_1.abc import DispatcherAPI, EventsAPI, PoolAPI, SessionAPI
from ddht.v5_1.envelope import InboundEnvelope, OutboundEnvelope
from ddht.v5_1.messages import PingMessage, PongMessage
from ddht.v5_1.packets import AnyPacket


class NodeAPI(ABC):
    enr: ENR
    node_db: NodeDBAPI
    events: EventsAPI
    pool: PoolAPI
    channels: SessionChannels

    @property
    @abstractmethod
    def private_key(self) -> keys.PrivateKey:
        ...

    @property
    @abstractmethod
    def endpoint(self) -> Endpoint:
        ...

    @property
    @abstractmethod
    def node_id(self) -> NodeID:
        ...


class SessionDriverAPI(ABC):
    session: SessionAPI
    node: NodeAPI
    remote: NodeAPI

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
    async def send_ping(self, request_id: Optional[int] = None) -> PingMessage:
        ...

    @abstractmethod
    async def send_pong(self, request_id: Optional[int] = None) -> PongMessage:
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


class NetworkAPI(ABC):
    @abstractmethod
    def node(self) -> NodeAPI:
        ...

    @abstractmethod
    def session_pair(self, initiator: NodeAPI, recipient: NodeAPI) -> SessionPairAPI:
        ...

    def dispatcher_pair(
        self, node_a: NodeAPI, node_b: NodeAPI,
    ) -> AsyncContextManager[Tuple[DispatcherAPI, DispatcherAPI]]:
        ...
