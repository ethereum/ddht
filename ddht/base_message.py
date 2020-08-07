from typing import NamedTuple

from eth_utils import int_to_big_endian
import rlp

from ddht.endpoint import Endpoint
from ddht.typing import NodeID


class BaseMessage(rlp.Serializable):  # type: ignore
    message_type: int

    def to_bytes(self) -> bytes:
        return b"".join((int_to_big_endian(self.message_type), rlp.encode(self)))


class IncomingMessage(NamedTuple):
    message: BaseMessage
    sender_endpoint: Endpoint
    sender_node_id: NodeID

    def __str__(self) -> str:
        return f"{self.__class__.__name__}[{self.message.__class__.__name__}]"

    def to_response(self, response_message: BaseMessage) -> "OutgoingMessage":
        return OutgoingMessage(
            message=response_message,
            receiver_endpoint=self.sender_endpoint,
            receiver_node_id=self.sender_node_id,
        )


class OutgoingMessage(NamedTuple):
    message: BaseMessage
    receiver_endpoint: Endpoint
    receiver_node_id: NodeID

    def __str__(self) -> str:
        return f"{self.__class__.__name__}[{self.message.__class__.__name__}]"
