from abc import ABC, abstractmethod
from typing import NamedTuple

from eth_keys import keys
import trio

from ddht.abc import NodeDBAPI
from ddht.base_message import BaseMessage, IncomingMessage
from ddht.endpoint import Endpoint
from ddht.enr import ENR
from ddht.typing import NodeID
from ddht.v5_1.abc import SessionAPI
from ddht.v5_1.envelope import IncomingEnvelope, OutgoingEnvelope
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
    incoming_message_send_channel: trio.abc.SendChannel[IncomingMessage]
    incoming_message_receive_channel: trio.abc.ReceiveChannel[IncomingMessage]
    outgoing_envelope_send_channel: trio.abc.SendChannel[OutgoingEnvelope]
    outgoing_envelope_receive_channel: trio.abc.ReceiveChannel[OutgoingEnvelope]

    @classmethod
    def init(cls) -> "SessionChannels":
        (
            incoming_message_send_channel,
            incoming_message_receive_channel,
        ) = trio.open_memory_channel[IncomingMessage](256)
        (
            outgoing_envelope_send_channel,
            outgoing_envelope_receive_channel,
        ) = trio.open_memory_channel[OutgoingEnvelope](256)
        return cls(
            incoming_message_send_channel,
            incoming_message_receive_channel,
            outgoing_envelope_send_channel,
            outgoing_envelope_receive_channel,
        )


class SessionDriverAPI(ABC):
    session: SessionAPI
    channels: SessionChannels

    @abstractmethod
    async def send_message(self, message: BaseMessage) -> None:
        ...

    @abstractmethod
    async def next_message(self) -> IncomingMessage:
        ...


class EnvelopePair(NamedTuple):
    outgoing: OutgoingEnvelope
    incoming: IncomingEnvelope

    @property
    def packet(self) -> AnyPacket:
        return self.outgoing.packet


class SessionPairAPI(ABC):
    initiator: SessionDriverAPI
    recipient: SessionDriverAPI

    @abstractmethod
    async def transmit_one(self, source: SessionDriverAPI) -> EnvelopePair:
        ...


class NetworkAPI(ABC):
    @abstractmethod
    def node(self) -> NodeAPI:
        ...

    @abstractmethod
    def session_pair(self, initiator: NodeAPI, recipient: NodeAPI) -> SessionPairAPI:
        ...
