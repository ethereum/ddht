from dataclasses import dataclass
from typing import Generic, TypeVar

from eth_typing import NodeID
from eth_utils import int_to_big_endian
import rlp

from ddht.endpoint import Endpoint


class BaseMessage(rlp.Serializable):  # type: ignore
    message_type: int

    def to_bytes(self) -> bytes:
        return b"".join((int_to_big_endian(self.message_type), rlp.encode(self)))


TMessage = TypeVar("TMessage", bound=BaseMessage)
TResponseMessage = TypeVar("TResponseMessage", bound=BaseMessage)


@dataclass(frozen=True)
class OutboundMessage(Generic[TMessage]):
    message: BaseMessage
    receiver_endpoint: Endpoint
    receiver_node_id: NodeID

    def __str__(self) -> str:
        return f"{self.__class__.__name__}[{self.message.__class__.__name__}]"


@dataclass(frozen=True)
class InboundMessage(Generic[TMessage]):
    message: TMessage
    sender_endpoint: Endpoint
    sender_node_id: NodeID

    def __str__(self) -> str:
        return f"{self.__class__.__name__}[{self.message.__class__.__name__}]"

    def to_response(
        self, response_message: TResponseMessage
    ) -> OutboundMessage[TResponseMessage]:
        return OutboundMessage(
            message=response_message,
            receiver_endpoint=self.sender_endpoint,
            receiver_node_id=self.sender_node_id,
        )


AnyInboundMessage = InboundMessage[BaseMessage]
AnyOutboundMessage = OutboundMessage[BaseMessage]
