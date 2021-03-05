from typing import Any, Tuple, Iterable, Sequence

from eth_utils import to_tuple
from eth_utils.toolz import sliding_window, partition

from ddht._utils import caboose
from ddht.v5_1.utp.abc import ExtensionAPI


@to_tuple
def _bytes_to_bitfield(data: bytes) -> Iterable[bool]:
    for char in data:
        for _ in range(8):
            yield bool(char & 1)
            char >>= 1


def _bitfield_to_bytes(bitfield: Tuple[bool, ...]) -> bytes:
    return bytes(
        sum(2**idx for idx, bit in enumerate(segment) if bit)
        for segment in partition(8, bitfield, False)
    )


class SelectiveAck(ExtensionAPI):
    id: int = 1

    def __init__(self, data: bytes) -> None:
        self.data = data

    @property
    def length(self) -> int:
        return len(self.data)

    def __eq__(self, other: Any) -> bool:
        if type(self) is not type(other):
            return False
        return self.data == other.data

    @classmethod
    def from_unacked(cls, ack_nr: int, acked: Sequence[int]) -> 'SelectiveAck':
        min_acked = min(acked)
        max_acked = max(acked)
        assert min_acked >= ack_nr + 2

        bitfield = tuple(
            seq_nr in acked
            for seq_nr
            in range(ack_nr + 2, max_acked + 1)
        )
        return cls(_bitfield_to_bytes(bitfield))

    def get_acks(self, ack_nr: int) -> Tuple[int]:
        bitfield = _bytes_to_bitfield(self.data)

        for idx, bit in enumerate(bitfield, 2):
            if bit:
                yield ack_nr + idx


class UnknownExtension(ExtensionAPI):
    id: int

    def __init__(self, extension_id: int, data: bytes) -> None:
        self.id = extension_id
        self.data = data

    @property
    def length(self) -> int:
        return len(self.data)

    def __eq__(self, other: Any) -> bool:
        if type(self) is not type(other):
            return False
        return self.data == other.data and self.id == other.id


def decode_extensions(first_extension_type: int,
                      payload: bytes,
                      ) -> Tuple[Tuple[ExtensionAPI, ...], int]:
    extensions = _decode_extensions(first_extension_type, payload)
    offset = sum(2 + extension.length for extension in extensions)
    return extensions, offset


def encode_extensions(extensions: Sequence[ExtensionAPI]) -> bytes:
    return b''.join(_encode_extensions(extensions))


def _encode_extensions(extensions: Sequence[ExtensionAPI]) -> Iterable[bytes]:
    if not extensions:
        return

    for extension, next_extension in sliding_window(2, caboose(extensions, None)):
        if next_extension is None:
            next_extension_id = 0
        else:
            next_extension_id = next_extension.id

        yield b''.join((
            next_extension_id.to_bytes(1, 'big'),
            extension.length.to_bytes(1, 'big'),
            extension.data,
        ))


@to_tuple
def _decode_extensions(first_extension_type: int, payload: bytes) -> Iterable[ExtensionAPI]:
    extension_type = first_extension_type
    offset = 0

    while extension_type != 0:
        next_extension_type = payload[offset]
        extension_length = payload[offset + 1]
        extension_data = payload[offset + 2:offset + 2 + extension_length]

        if extension_type == 1:
            yield SelectiveAck(extension_data)
        else:
            yield UnknownExtension(extension_type, extension_data)

        extension_type = next_extension_type
        offset += (2 + extension_length)
