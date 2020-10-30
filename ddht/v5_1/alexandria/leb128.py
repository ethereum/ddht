import functools
import itertools
import math
import operator
from typing import Iterable, Iterator, Tuple

from eth_utils import to_tuple

from ddht.exceptions import ParseError

LOW_MASK = 2 ** 7 - 1
HIGH_MASK = 2 ** 7


def encode_leb128(value: int) -> bytes:
    return bytes(bytearray(_encode_leb128(value)))


def _encode_leb128(value: int) -> Iterable[int]:
    while True:
        byte = value & LOW_MASK

        value >>= 7

        if value == 0:
            yield byte
            break
        else:
            yield byte | HIGH_MASK


def decode_leb128(data: bytes) -> int:
    """
    https://en.wikipedia.org/wiki/LEB128
    """
    value, remainder = parse_leb128(data)
    if remainder:
        raise ParseError(f"Dangling bytes: data={data.hex()}  extra={remainder.hex()}")
    return value


def parse_leb128(data: bytes) -> Tuple[int, bytes]:
    data_iter = iter(data)
    value = functools.reduce(operator.or_, _parse_leb128(data_iter), 0,)
    remainder = bytes(data_iter)
    return value, remainder


@to_tuple
def partition_leb128(data: bytes) -> Iterable[bytes]:
    if not data:
        return
    last_idx = 0
    for idx, byte in enumerate(data):
        if not byte & HIGH_MASK:
            yield data[last_idx : idx + 1]
            last_idx = idx + 1
    if last_idx != len(data):
        raise ParseError(
            f"Dangling bytes: data={data.hex()}  extra={data[last_idx:].hex()}"
        )


# The maximum shift width for a 64 bit integer.  We shouldn't have to decode
# integers larger than this.
SHIFT_64_BIT_MAX = int(math.ceil(64 / 7)) * 7


def _parse_leb128(data_iter: Iterator[int]) -> Iterable[int]:
    for shift in itertools.count(0, 7):
        if shift > SHIFT_64_BIT_MAX:
            raise ParseError("Decoded integer exceeds maximum decodable size...")

        try:
            value = next(data_iter)
        except StopIteration:
            raise ParseError(
                "Unexpected end of stream while parsing LEB128 encoded integer"
            )

        yield (value & LOW_MASK) << shift

        if not value & HIGH_MASK:
            break
