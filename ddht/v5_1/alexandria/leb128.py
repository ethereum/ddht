import functools
import itertools
import math
import operator
from typing import IO, Iterable

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


def parse_leb128(stream: IO[bytes]) -> int:
    """
    https://en.wikipedia.org/wiki/LEB128
    """
    return functools.reduce(operator.or_, _parse_leb128(stream), 0,)


# The maximum shift width for a 64 bit integer.  We shouldn't have to decode
# integers larger than this.
SHIFT_64_BIT_MAX = int(math.ceil(64 / 7)) * 7


def _parse_leb128(stream: IO[bytes]) -> Iterable[int]:
    for shift in itertools.count(0, 7):
        if shift > SHIFT_64_BIT_MAX:
            raise ParseError("Decoded integer exceeds maximum decodable size...")

        byte = stream.read(1)

        try:
            value = byte[0]
        except IndexError:
            raise ParseError(
                "Unexpected end of stream while parsing LEB128 encoded integer"
            )

        yield (value & LOW_MASK) << shift

        if not value & HIGH_MASK:
            break
