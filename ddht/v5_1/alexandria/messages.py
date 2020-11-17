from typing import Any, Dict, Generic, Type, TypeVar

import ssz
from ssz import BaseSedes

from ddht.constants import UINT8_TO_BYTES
from ddht.exceptions import DecodingError
from ddht.v5_1.alexandria.payloads import (
    ContentPayload,
    FindNodesPayload,
    FoundNodesPayload,
    GetContentPayload,
    PingPayload,
    PongPayload,
)
from ddht.v5_1.alexandria.sedes import (
    ContentSedes,
    FindNodesSedes,
    FoundNodesSedes,
    GetContentSedes,
    PingSedes,
    PongSedes,
)

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
                ssz.encode(self.get_payload_for_encoding(), sedes=self.sedes),
            )
        )

    def get_payload_for_encoding(self) -> Any:
        return self.payload

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


@register
class FindNodesMessage(AlexandriaMessage[FindNodesPayload]):
    message_id = 3
    sedes = FindNodesSedes
    payload_type = FindNodesPayload

    payload: FindNodesPayload

    @classmethod
    def from_payload_args(
        cls: Type[TAlexandriaMessage], payload_args: Any
    ) -> TAlexandriaMessage:
        # py-ssz uses an internal type for decoded `ssz.sedes.List` types that
        # we don't need or want so we force it to a normal tuple type here.
        distances = tuple(payload_args[0])
        payload = cls.payload_type(distances)
        return cls(payload)


@register
class FoundNodesMessage(AlexandriaMessage[FoundNodesPayload]):
    message_id = 4
    sedes = FoundNodesSedes
    payload_type = FoundNodesPayload

    payload: FoundNodesPayload

    @classmethod
    def from_payload_args(
        cls: Type[TAlexandriaMessage], payload_args: Any
    ) -> TAlexandriaMessage:
        # py-ssz uses an internal type for decoded `ssz.sedes.List` types that
        # we don't need or want so we force it to a normal tuple type here.
        total, ssz_wrapped_enrs = payload_args
        enrs = tuple(ssz_wrapped_enrs)
        payload = cls.payload_type(total, enrs)
        return cls(payload)


@register
class GetContentMessage(AlexandriaMessage[GetContentPayload]):
    message_id = 5
    sedes = GetContentSedes
    payload_type = GetContentPayload

    payload: GetContentPayload


@register
class ContentMessage(AlexandriaMessage[ContentPayload]):
    message_id = 6
    sedes = ContentSedes
    payload_type = ContentPayload

    payload: ContentPayload


def decode_message(data: bytes) -> AlexandriaMessage[Any]:
    message_id = data[0]
    try:
        message_class = MESSAGE_REGISTRY[message_id]
    except KeyError:
        raise DecodingError(f"Unknown message type: id={message_id}")

    payload_args = ssz.decode(data[1:], sedes=message_class.sedes)
    return message_class.from_payload_args(payload_args)
