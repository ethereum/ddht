from abc import ABC, abstractmethod
from typing import AsyncContextManager, NamedTuple, Optional

from eth_keys import keys
import trio

from ddht.abc import NodeDBAPI
from ddht.base_message import AnyInboundMessage, BaseMessage
from ddht.endpoint import Endpoint
from ddht.enr import ENR
from ddht.typing import NodeID
from ddht.v5_1.abc import SessionAPI
from ddht.v5_1.envelope import InboundEnvelope, OutboundEnvelope
from ddht.v5_1.messages import PingMessage, PongMessage
from ddht.v5_1.packets import AnyPacket


class NodeAPI(ABC):
    enr: ENR
    private_key: keys.PrivateKey
    node_db: NodeDBAPI

    @property
    @abstractmethod
    def endpoint(self) -> Endpoint:
        ...

    @property
    @abstractmethod
    def node_id(self) -> NodeID:
        ...


class SessionChannels(NamedTuple):
    inbound_message_send_channel: trio.abc.SendChannel[AnyInboundMessage]
    inbound_message_receive_channel: trio.abc.ReceiveChannel[AnyInboundMessage]
    outbound_envelope_send_channel: trio.abc.SendChannel[OutboundEnvelope]
    outbound_envelope_receive_channel: trio.abc.ReceiveChannel[OutboundEnvelope]

    @classmethod
    def init(cls) -> "SessionChannels":
        (
            inbound_message_send_channel,
            inbound_message_receive_channel,
        ) = trio.open_memory_channel[AnyInboundMessage](256)
        (
            outbound_envelope_send_channel,
            outbound_envelope_receive_channel,
        ) = trio.open_memory_channel[OutboundEnvelope](256)
        return cls(
            inbound_message_send_channel,
            inbound_message_receive_channel,
            outbound_envelope_send_channel,
            outbound_envelope_receive_channel,
        )


class SessionDriverAPI(ABC):
    session: SessionAPI
    channels: SessionChannels
    node: NodeAPI

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
