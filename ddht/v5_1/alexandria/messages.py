from typing import Any, Dict, Generic, Type, TypeVar

import ssz
from ssz import BaseSedes

from ddht.constants import UINT8_TO_BYTES
from ddht.exceptions import DecodingError
from ddht.v5_1.alexandria.payloads import PingPayload, PongPayload
from ddht.v5_1.alexandria.sedes import PingSedes, PongSedes

TPayload = TypeVar("TPayload")


TAlexandriaMessage = TypeVar("TAlexandriaMessage", bound="AlexandriaMessage[Any]")


class AlexandriaMessage(Generic[TPayload]):
    message_id: int
    sedes: BaseSedes
    payload_type: Type[TPayload]

    payload: TPayload

    def __init__(self, payload: TPayload) -> None:
        self.payload = payload

    def __eq__(self, other: Any) -> bool:
        if type(self) is not type(other):
            return False
        return self.payload == other.payload  # type: ignore

    def to_wire_bytes(self) -> bytes:
        return b"".join(
            (
                UINT8_TO_BYTES[self.message_id],
                ssz.encode(self.payload, sedes=self.sedes),
            )
        )

    @classmethod
    def from_payload_args(
        cls: Type[TAlexandriaMessage], payload_args: Any
    ) -> TAlexandriaMessage:
        payload = cls.payload_type(*payload_args)
        return cls(payload)


MESSAGE_REGISTRY: Dict[int, Type[AlexandriaMessage[Any]]] = {}


def register(message_class: Type[TAlexandriaMessage]) -> Type[TAlexandriaMessage]:
    message_id = message_class.message_id

    if message_id in MESSAGE_REGISTRY:
        raise ValueError(
            f"Message id already in registry: id={message_id} "
            f"class={MESSAGE_REGISTRY[message_id]}"
        )

    MESSAGE_REGISTRY[message_id] = message_class
    return message_class


@register
class PingMessage(AlexandriaMessage[PingPayload]):
    message_id = 1
    sedes = PingSedes
    payload_type = PingPayload

    payload: PingPayload


@register
class PongMessage(AlexandriaMessage[PongPayload]):
    message_id = 2
    sedes = PongSedes
    payload_type = PongPayload

    payload: PongPayload


def decode_message(data: bytes) -> AlexandriaMessage[Any]:
    message_id = data[0]
    try:
        message_class = MESSAGE_REGISTRY[message_id]
    except KeyError:
        raise DecodingError(f"Unknown message type: id={message_id}")

    payload_args = ssz.decode(data[1:], sedes=message_class.sedes)
    return message_class.from_payload_args(payload_args)
