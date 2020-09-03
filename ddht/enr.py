from typing import Iterable, Sequence, Tuple

from eth_enr import ENRAPI
from eth_enr.constants import MAX_ENR_SIZE
from eth_enr.sedes import ENRSedes
import rlp
from rlp.sedes import CountableList


def _partition_enrs(
    enrs: Sequence[ENRAPI], max_payload_size: int
) -> Iterable[Tuple[ENRAPI, ...]]:
    num_records = len(enrs)
    min_records = max_payload_size // MAX_ENR_SIZE
    left = 0
    right = min_records

    while right < num_records:
        encoded = rlp.encode(enrs[left : right + 1], CountableList(ENRSedes))
        if len(encoded) > max_payload_size:
            yield tuple(enrs[left:right])
            left, right = right, right + min_records
            continue
        else:
            right += 1

    yield tuple(enrs[left:right])


def partition_enrs(
    enrs: Sequence[ENRAPI], max_payload_size: int
) -> Tuple[Tuple[ENRAPI, ...], ...]:
    """
    Partition a list of ENRs to groups to be sent in separate NODES messages.

    The goal is to send as few messages as possible, but each message must not exceed the maximum
    allowed size.

    If a single ENR exceeds the maximum payload size, it will be dropped.
    """
    if max_payload_size < MAX_ENR_SIZE:
        raise ValueError(
            "Cannot parition ENR records under the max singular ENR record size"
        )

    return tuple(_partition_enrs(enrs, max_payload_size))
