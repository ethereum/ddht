from dataclasses import dataclass
from typing import Any, Generic, Optional, TypeVar

from eth_typing import NodeID
from eth_utils import int_to_big_endian
import rlp

from ddht.endpoint import Endpoint


class BaseMessage(rlp.Serializable):  # type: ignore
    message_type: int

    def to_bytes(self) -> bytes:
        return b"".join((int_to_big_endian(self.message_type), rlp.encode(self)))


TMessage = TypeVar("TMessage")
TBaseMessage = TypeVar("TBaseMessage", bound=BaseMessage)
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
    explicit_request_id: Optional[bytes] = None

    @property
    def request_id(self) -> bytes:
        """
        Return the ``request_id`` for this message.

        This API exists to allow this class to be used with both base-protocol
        messages which contain the ``request_id`` as well as with
        TALKREQ/TALKRESP based sub-protocol messages.  In the TALKREQ/TALKRESP
        case the ``request_id`` is only present on the base protocol message
        and needs to be duplicated into the sub-protocol message.  This is
        integral to being able to use the ``SubscriptionManagerAPI`` with both
        the base protocol and sub-protocols.
        """
        if self.explicit_request_id is not None:
            return self.explicit_request_id
        elif hasattr(self.message, "request_id"):
            return self.message.request_id  # type: ignore
        else:
            raise AttributeError(
                f"No explicit request_id and message does not have a "
                f"`request_id` property: {self.message}"
            )

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


AnyInboundMessage = InboundMessage[Any]
AnyOutboundMessage = OutboundMessage[Any]
