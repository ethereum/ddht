from typing import Iterable

from eth_utils import to_tuple
from eth_utils.toolz import sliding_window
from ssz.constants import CHUNK_SIZE

from ddht.v5_1.alexandria.constants import POWERS_OF_TWO

from .typing import TreePath


def display_path(path: TreePath) -> str:
    """
    Converts a tree path to a string of 1s and 0s for more legible display.
    """
    return "".join((str(int(bit)) for bit in path))


@to_tuple
def decompose_into_powers_of_two(value: int) -> Iterable[int]:
    for i in range(value.bit_length()):
        power_of_two = POWERS_OF_TWO[i]
        if value & power_of_two:
            yield power_of_two


@to_tuple
def get_longest_common_path(*paths: TreePath) -> Iterable[bool]:
    """
    Return the longs common prefix for the provided paths.
    """
    if not paths:
        return
    elif len(paths) == 1:
        yield from paths[0]
        return
    elif not any(paths):
        return

    for crumbs in zip(*paths):
        if all(crumbs) or not any(crumbs):
            yield crumbs[0]
        else:
            break


def get_chunk_count_for_data_length(length: int) -> int:
    if length == 0:
        return 0
    return (length + CHUNK_SIZE - 1) // CHUNK_SIZE  # type: ignore


@to_tuple
def filter_overlapping_paths(*paths: TreePath) -> Iterable[TreePath]:
    """
    Filter out any paths that are a prefix of another path.
    """
    if not paths:
        return
    sorted_paths = sorted(paths)
    for left, right in sliding_window(2, sorted_paths):
        if right[: len(left)] == left:
            continue
        else:
            yield left

    # Because of the use of `sliding_window` we need to manually yield the last
    # path
    yield sorted_paths[-1]
