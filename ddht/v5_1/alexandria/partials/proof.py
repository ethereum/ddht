import bisect
from dataclasses import dataclass
import functools
import operator
from typing import Any, Collection, Iterable, Optional, Sequence, Tuple

from eth_typing import Hash32
from eth_utils import ValidationError, to_tuple
from eth_utils.toolz import groupby, sliding_window
from ssz.constants import CHUNK_SIZE, ZERO_HASHES
from ssz.hash import hash_eth2
from ssz.sedes import List as ListSedes

from ddht.v5_1.alexandria.partials._utils import (
    decompose_into_powers_of_two,
    display_path,
    get_chunk_count_for_data_length,
)
from ddht.v5_1.alexandria.partials.chunking import chunk_index_to_path, compute_chunks
from ddht.v5_1.alexandria.partials.typing import TreePath


class BrokenTree(Exception):
    """
    Exception signaling that there is something wrong with the merkle tree.
    """

    ...


@dataclass(frozen=True, eq=True, order=True)
class ProofElement:
    path: TreePath
    value: Hash32

    @property
    def depth(self) -> int:
        return len(self.path)

    def __str__(self) -> str:
        return f"{display_path(self.path)}: {self.value.hex()}"


class Proof:
    """
    Representation of a merkle proof for an SSZ byte string (aka List[uint8,
    max_length=...]).
    """

    # TODO: look at where `self.sedes` is actually used and figure out if it belongs in this class.
    sedes: ListSedes
    # TODO: footgun.  Without performing verification (validate_proof(proof))
    # we don't know this value is actually valid.
    elements: Tuple[ProofElement, ...]

    def __init__(self, elements: Collection[ProofElement], sedes: ListSedes,) -> None:
        self.elements = tuple(sorted(elements))
        self._paths = tuple(el.path for el in self.elements)
        self.sedes = sedes

    def __eq__(self, other: Any) -> bool:
        if type(self) is not type(other):
            return False
        else:
            return self.elements == other.elements  # type: ignore

    def __hash__(self) -> int:
        return hash((self.elements, self.sedes))

    @functools.lru_cache
    def get_hash_tree_root(self) -> Hash32:
        return merklize_elements(self.elements)

    @functools.lru_cache
    def get_element(self, path: TreePath) -> ProofElement:
        """
        Retrieve a single element by its path.
        """
        candidate_index = bisect.bisect_left(self._paths, path)
        candidate = self.elements[candidate_index]
        if candidate.path == path:
            return candidate
        else:
            raise IndexError(f"No proof element for path: {display_path(path)}")

    @functools.lru_cache
    def get_content_length(self) -> int:
        """
        Retrieve the content length
        """
        length_element = self.get_element((True,))
        return int.from_bytes(length_element.value, "little")

    @functools.lru_cache
    def get_last_data_chunk_index(self) -> int:
        """
        Compute the index of the last data chunck.
        """
        length = self.get_content_length()
        num_data_chunks = get_chunk_count_for_data_length(length)
        return max(0, num_data_chunks - 1)

    @functools.lru_cache
    def get_last_data_chunk_path(self) -> TreePath:
        """
        Compute the `TreePath` of the last data chunck.
        """
        return chunk_index_to_path(
            self.get_last_data_chunk_index(), self.path_bit_length
        )

    def get_data_elements(self) -> Tuple[ProofElement, ...]:
        """
        Return all of the proof elements that are part of the section of the
        merkle tree that houses the actual content data.
        """
        return self.get_elements(
            right=self.get_last_data_chunk_path(), right_inclusive=True
        )

    @functools.lru_cache
    def get_first_padding_chunk_index(self) -> int:
        """
        Return the index of the first padding chunk.

        Raise `IndexError` if the tree has no padding.
        """
        last_data_chunk_index = self.get_last_data_chunk_index()
        if last_data_chunk_index == self.sedes.chunk_count - 1:
            raise IndexError("Full tree.  No padding")
        return last_data_chunk_index + 1

    @functools.lru_cache
    def get_first_padding_chunk_path(self) -> TreePath:
        """
        Return the path of the first padding chunk.

        Raise `IndexError` if the tree has no padding.
        """
        return chunk_index_to_path(
            self.get_first_padding_chunk_index(), self.path_bit_length
        )

    def get_padding_elements(self) -> Tuple[ProofElement, ...]:
        """
        Return all of the elements from the tree that are purely padding.
        """
        return self.get_elements(
            self.get_last_data_chunk_path(), (True,), left_inclusive=False
        )

    def get_elements(
        self,
        left: Optional[TreePath] = None,
        right: Optional[TreePath] = None,
        left_inclusive: bool = True,
        right_inclusive: bool = False,
    ) -> Tuple[ProofElement, ...]:
        """
        Return the elements from the proof bounded by the `left` and `right`
        paths.

        The `left_inclusive` and `right_inclusive` dictate whether the
        node at the given `left/right` path should be included in the results.
        """
        if left is None and right is None:
            return self.elements
        elif left is None:
            if right_inclusive:
                right_index = bisect.bisect_right(self._paths, right)
            else:
                right_index = bisect.bisect_left(self._paths, right)
            return self.elements[:right_index]
        elif right is None:
            if left_inclusive:
                left_index = bisect.bisect_left(self._paths, left)
            else:
                left_index = bisect.bisect_right(self._paths, left)

            return self.elements[left_index:]
        else:
            if right_inclusive:
                right_index = bisect.bisect_right(self._paths, right)
            else:
                right_index = bisect.bisect_left(self._paths, right)

            if left_inclusive:
                left_index = bisect.bisect_left(self._paths, left)
            else:
                left_index = bisect.bisect_right(self._paths, left)

            return self.elements[left_index:right_index]

    @functools.lru_cache
    @to_tuple
    def get_elements_under(self, path: TreePath) -> Iterable[ProofElement]:
        """
        Return all of the proof elements whos path begins with the given path.
        """
        start_index = bisect.bisect_left(self._paths, path)
        path_depth = len(path)
        for el in self.elements[start_index:]:
            if el.path[:path_depth] == path:
                yield el
            else:
                break

    @property
    def path_bit_length(self) -> int:
        """
        The maximum valid length for any path in this proof.
        """
        return self.sedes.chunk_count.bit_length()  # type: ignore


def merklize_elements(elements: Sequence[ProofElement]) -> Hash32:
    """
    Given a set of `ProofElement` compute the `hash_tree_root`.
    """
    elements_by_depth = groupby(operator.attrgetter("depth"), elements)
    max_depth = max(elements_by_depth.keys())

    for depth in range(max_depth, 0, -1):
        try:
            elements_at_depth = sorted(elements_by_depth.pop(depth))
        except KeyError:
            continue

        sibling_pairs = tuple(
            (left, right)
            for left, right in sliding_window(2, elements_at_depth)
            if left.path[:-1] == right.path[:-1]
        )
        parents = tuple(
            ProofElement(path=left.path[:-1], value=hash_eth2(left.value + right.value))
            for left, right in sibling_pairs
        )

        if not elements_by_depth:
            if len(parents) == 1:
                return parents[0].value
            else:
                raise BrokenTree("Multiple root nodes")
        else:
            elements_by_depth.setdefault(depth - 1, [])
            elements_by_depth[depth - 1].extend(parents)
    else:
        raise BrokenTree("Unable to fully collapse tree within 32 rounds")


def validate_proof(proof: Proof) -> None:
    try:
        proof.get_hash_tree_root()
    except BrokenTree as err:
        raise ValidationError(str(err)) from err


def is_proof_valid(proof: Proof) -> bool:
    try:
        proof.get_hash_tree_root()
    except BrokenTree:
        return False
    else:
        return True


@to_tuple
def compute_proof_elements(
    chunks: Sequence[Hash32], chunk_count: int
) -> Iterable[ProofElement]:
    """
    Compute all of the proof elements, including the right hand padding
    elements for a proof over the given chunks.
    """
    # By using the full bit-length here we leave room for the length which gets
    # mixed in at the root of the tree.
    path_bit_length = chunk_count.bit_length()

    for idx, chunk in enumerate(chunks):
        path = chunk_index_to_path(idx, path_bit_length)
        yield ProofElement(path, chunk)

    start_index = len(chunks) - 1
    num_padding_chunks = chunk_count - len(chunks)

    yield from get_padding_elements(start_index, num_padding_chunks, path_bit_length)


def compute_proof(data: bytes, sedes: ListSedes) -> Proof:
    """
    Compute the full proof, including the mixed-in length value.
    """
    chunks = compute_chunks(data)

    chunk_count = sedes.chunk_count

    proof_elements = compute_proof_elements(chunks, chunk_count)
    length_element = ProofElement(
        path=(True,), value=Hash32(len(data).to_bytes(CHUNK_SIZE, "little")),
    )
    all_elements = proof_elements + (length_element,)

    return Proof(all_elements, sedes)


@to_tuple
def get_padding_elements(
    start_index: int, num_padding_chunks: int, path_bit_length: int
) -> Iterable[ProofElement]:
    r"""
    Get the padding elements for a proof.


    0:                           X
                                / \
                              /     \
                            /         \
                          /             \
                        /                 \
                      /                     \
                    /                         \
    1:             0                           1
                 /   \                       /   \
               /       \                   /       \
             /           \               /           \
    2:      0             1             0             1
           / \           / \           / \           / \
          /   \         /   \         /   \         /   \
    3:   0     1       0     1       0     1       0     1
        / \   / \     / \   / \     / \   / \     / \   / \
    4: 0   1 0   1   0   1 0   1   0   1 0   1   0   1 0   1

                                                      |<--->|
                                                      [  2  ]
                                                    |<----->|
                                                    [1][  2 ]
                                                |<-PADDING->|
                                                [    4      ]
                                            |<---PADDING--->|
                                            [1] [    4      ]
                                        |<-----PADDING----->|
                                        [  2  ] [    4      ]
                                      |<------PADDING------>|
                                      [1][  2 ] [    4      ]
                                  |<--------PADDING-------->|
                                  [            8            ]

    Padding is always on the right-hand-side of the tree.

    One nice property of the binary tree is that we can very efficiently
    determine the minimal set of subtree nodes needed to represent the full
    padding.

    We do this by computing the total number of padding nodes, and then
    decomposing that into the powers of two needed to make up that number.

    These determine our subtrees.  The bit-length of each number tells us how
    many levels of zero hashes we will need, and we use a pre-computed set of
    these hashes for efficiency sake.
    """
    for power_of_two in decompose_into_powers_of_two(num_padding_chunks):
        depth = power_of_two.bit_length() - 1
        left_index = start_index + power_of_two
        left_path = chunk_index_to_path(left_index, path_bit_length)
        padding_hash_tree_root = ZERO_HASHES[depth]
        yield ProofElement(left_path[: path_bit_length - depth], padding_hash_tree_root)
