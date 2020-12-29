from typing import Tuple, Sequence, Iterable

from eth_utils import to_tuple

from ddht.v5_1.alexandria.partials._utils import get_longest_common_path, filter_overlapping_paths
from ddht.v5_1.alexandria.partials.chunking import get_subtree_slices, chunk_index_to_path
from ddht.v5_1.alexandria.partials.typing import TreePath


@to_tuple
def _new_unexplored_parts(base: TreePath, path: TreePath) -> Iterable[TreePath]:
    tail = path[len(base):]
    for depth in range(len(tail) - 1, -1, -1):
        yield base + tail[:depth] + (not tail[depth],)


def is_subpath_of(path: TreePath, other: TreePath) -> bool:
    return path == other[:len(path)]


def path_to_leaf(path: TreePath) -> int:
    return sum(
        crumb << shift if crumb else 0
        for shift, crumb
        in enumerate(reversed(path))
    )


def _path_to_slice(path: TreePath, depth: int) -> slice:
    left = path + (False,) * (depth - len(path))
    right = path + (True,) * (depth - len(path))
    return slice(path_to_leaf(left), path_to_leaf(right) + 1)


def explore_path(unexplored: Sequence[TreePath], to_explore: TreePath) -> Tuple[TreePath, ...]:
    if not unexplored:
        return unexplored
    return filter_overlapping_paths(*_explore_path(unexplored, to_explore))


def _explore_path(unexplored: Sequence[TreePath], to_explore: TreePath) -> Iterable[TreePath]:
    for unexplored_path in unexplored:
        # Exploring the child of an unexplored path:
        #   - discard the unexplored path
        #   - mark all of the intermediate forks of the new path as unexplored
        if is_subpath_of(unexplored_path, to_explore):
            yield from _new_unexplored_parts(unexplored_path, to_explore)
        # Exploring the parent of an unexplored_path
        #   - discard the unexplored path
        elif is_subpath_of(to_explore, unexplored_path):
            continue
        else:
            yield unexplored_path


class Fog:
    def __init__(self, depth: int) -> None:
        self.depth = depth
        self.unexplored = ((),)

    def explore_leaf(self, value: int) -> None:
        self.explore_path(chunk_index_to_path(value, self.depth))

    def explore_path(self, path: TreePath) -> None:
        self.unexplored = explore_path(self.unexplored, path)

    def is_leaf_explored(self, value: int) -> bool:
        return self.is_path_explored(chunk_index_to_path(value, self.depth))

    def is_path_unexplored(self, path: TreePath) -> bool:
        if not self.unexplored:
            return False
        elif path in self.unexplored:
            return True

        for candidate in self.unexplored:
            if is_subpath_of(candidate, path):
                return True
            elif is_subpath_of(path, candidate):
                return True
        else:
            return False

    def is_path_explored(self, path: TreePath) -> bool:
        return not self.is_path_unexplored(path)

    def explore_from(self, from_leaf: int, num_leaves: int) -> None:
        slices = get_subtree_slices(from_leaf, num_leaves)
        for slice in slices:
            path = get_longest_common_path(
                chunk_index_to_path(slice.start, self.depth),
                chunk_index_to_path(slice.stop - 1, self.depth),
            )
            self.explore_path(path)

    def iter_unexplored(self) -> Iterable[slice]:
        if not self.unexplored:
            return

        for path in self.unexplored:
            yield _path_to_slice(path, self.depth)
