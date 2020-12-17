import bisect
from typing import Tuple, Sequence, Iterable

from eth_utils import to_tuple
from eth_utils.toolz import sliding_window

TreePath = Tuple[bool, ...]


def leaf_to_path(value: int, num_bits: int) -> TreePath:
    return tuple(bool(value >> width) & 1 for width in range(num_bits))


@to_tuple
def reduce_unexplored(unexplored: Sequence[TreePath]) -> Iterable[TreePath]:
    assert tuple(sorted(unexplored)) == tuple(unexplored)
    assert len(set(unexplored)) == len(unexplored)

    for left, right in sliding_window(2, unexplored):
        if left[:-1] == right[:-1]:
            yield right[:-1]
        else:
            yield left

    if unexplored:
        yield unexplored[-1]


def explore_path(unexplored: Sequence[TreePath], path: TreePath) -> Tuple[TreePath, ...]:
    idx = bisect.bisect_left(unexplored, path) - 1
    candidate = unexplored[idx]

    left_parts = unexplored[:idx]
    right_parts = unexplored[idx + 2:]

    if path == candidate:
        # exploring a full section that is un-explored
        return left_parts + right_parts
    elif path[:len(candidate)] == candidate:
        # exploring part of an existing unexplored section
        new_part = candidate + (not path[len(candidate)],)
        return left_parts + new_part + right_parts
    else:
        # exploring a previously explored section
        return unexplored


class Fog:
    def __init__(self, bits: int) -> None:
        self.unexplored = ((),)

    def explore_leaf(self, value: int) -> None:
        self.explore_path(leaf_to_path(value))

    def explore_path(self, path: TreePath) -> None:
        self

    def is_path_explored(self, path: TreePath) -> bool:
        ...

    def is_leaf_explored(self, value: int) -> bool:
        ...
