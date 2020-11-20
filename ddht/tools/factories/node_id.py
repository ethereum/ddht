from typing import Tuple

from eth_typing import NodeID
from eth_utils import big_endian_to_int, int_to_big_endian
from eth_utils.toolz import reduce
import factory


def bytes_to_bits(input_bytes: bytes) -> Tuple[bool, ...]:
    num_bits = len(input_bytes) * 8
    as_int = big_endian_to_int(input_bytes)
    as_bits = tuple(bool(as_int & (1 << index)) for index in range(num_bits))[::-1]
    return as_bits


def bits_to_bytes(input_bits: Tuple[bool, ...]) -> bytes:
    if len(input_bits) % 8 != 0:
        raise ValueError("Number of input bits must be a multiple of 8")
    num_bytes = len(input_bits) // 8

    as_int = reduce(lambda rest, bit: rest * 2 + bit, input_bits)
    as_bytes_unpadded = int_to_big_endian(as_int)
    padding = b"\x00" * (num_bytes - len(as_bytes_unpadded))
    return padding + as_bytes_unpadded


class NodeIDFactory(factory.Factory):  # type: ignore
    class Meta:
        model = NodeID
        inline_args = ("node_id",)

    node_id = factory.Faker("binary", length=32)

    @classmethod
    def at_log_distance(cls, reference: NodeID, log_distance: int) -> NodeID:
        from ddht.kademlia import at_log_distance as _at_log_distance

        return _at_log_distance(reference, log_distance)
