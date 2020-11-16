import bisect
from dataclasses import dataclass
import functools
import itertools
import operator
from typing import (
    Any,
    Collection,
    Iterable,
    NamedTuple,
    Optional,
    Sequence,
    Tuple,
    Union,
    overload,
)

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
from ddht.v5_1.alexandria.partials.chunking import (
    chunk_index_to_path,
    compute_chunks,
    group_by_subtree,
    path_to_left_chunk_index,
)
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


class DataSegment(NamedTuple):
    start_index: int
    data: bytes

    @property
    def end_index(self) -> int:
        return self.start_index + len(self.data)


class DataPartial:
    """
    A wrapper around a partial proof which facilitates easy access to the proven data.

    The `Proof.get_proven_data` API returns an instance of this class, which
    wraps the various segments of proven data and allows you to treat them as
    if they were a single byte string with missing sections.  Attempts to
    access a section of the data that is missing will raise an `IndexError`.

    Data can be accessed by slice or index in the same manner as a normal byte
    string, with the exception that it does not allow for slices that extend
    beyond the end of the data, nor does it allow slices that define a `step`
    value.
    """

    def __init__(self, length: int, segments: Tuple[DataSegment, ...]) -> None:
        self._length = length
        self._segments = segments

    def __len__(self) -> int:
        return self._length

    @overload
    def __getitem__(self, index_or_slice: int) -> int:
        ...

    @overload
    def __getitem__(self, index_or_slice: slice) -> bytes:
        ...

    def __getitem__(self, index_or_slice: Union[int, slice]) -> Union[int, bytes]:
        if isinstance(index_or_slice, slice):
            if index_or_slice.step is not None:
                raise Exception("step values not supported")
            start_at = index_or_slice.start or 0
            end_at = index_or_slice.stop
        elif isinstance(index_or_slice, int):
            start_at = index_or_slice
            end_at = index_or_slice + 1
        else:
            raise TypeError(f"Unsupported type: {type(index_or_slice)}")

        data_length = end_at - start_at

        candidate_index = max(0, bisect.bisect_left(self._segments, (start_at,)) - 1)
        segment = self._segments[candidate_index]
        is_within_bounds = (
            segment.start_index <= start_at <= segment.end_index
            and data_length <= len(segment.data)
        )
        if not is_within_bounds:
            raise IndexError(
                f"Requested data is out of bounds: segment=({segment.start_index} - "
                f"{segment.end_index}) slice=({start_at} - {end_at})"
            )

        if isinstance(index_or_slice, slice):
            offset_slice = slice(
                start_at - segment.start_index, end_at - segment.start_index
            )
            return segment.data[offset_slice]
        elif isinstance(index_or_slice, int):
            offset_index = index_or_slice - segment.start_index
            return segment.data[offset_index]
        else:
            raise TypeError(f"Unsupported type: {type(index_or_slice)}")


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

    def to_partial(self, start_at: int, partial_data_length: int) -> "Proof":
        """
        Return another proof with the minimal number of tree elements necessary
        to prove the slice of the underlying bytestring denoted by the
        `start_at` and `partial_data_length` parameters.
        """
        # First retrieve the overall content length from the proof.  The `length`
        # should always be found on the path `(True,)` which should always be
        # present in the tree.
        length = self.get_content_length()

        # Ensure that we aren't requesting data that exceeds the overall length
        # of the actual content.
        end_at = start_at + partial_data_length
        if end_at > length:
            raise Exception(
                f"Cannot create partial that exceeds the data length: {end_at} > {length}"
            )

        # Compute the chunk indices and corresponding paths for the locations
        # in the tree where the partial data starts and ends.
        first_partial_chunk_index = start_at // CHUNK_SIZE

        if partial_data_length == 0:
            last_partial_chunk_index = first_partial_chunk_index
        else:
            last_partial_chunk_index = (end_at - 1) // CHUNK_SIZE

        first_partial_chunk_path = chunk_index_to_path(
            first_partial_chunk_index, self.path_bit_length
        )
        last_partial_chunk_path = chunk_index_to_path(
            last_partial_chunk_index, self.path_bit_length
        )

        # Get all of the leaf nodes for the section of the tree where the
        # partial data is located.  Ensure that we have a contiguous section of
        # leaf nodes for this part of the tree.
        partial_elements = self.get_elements(
            left=first_partial_chunk_path,
            right=last_partial_chunk_path,
            right_inclusive=True,
        )
        expected_partial_chunk_count = (
            last_partial_chunk_index - first_partial_chunk_index
        ) + 1
        if len(partial_elements) != expected_partial_chunk_count:
            raise Exception(
                "Proof is missing leaf nodes required for partial construction."
            )

        minimal_data_elements_left_of_partial: Tuple[ProofElement, ...]
        if first_partial_chunk_index == 0:
            minimal_data_elements_left_of_partial = ()
        else:
            minimal_data_elements_left_of_partial = self.get_minimal_proof_elements(
                0, first_partial_chunk_index,
            )

        last_data_chunk_index = self.get_last_data_chunk_index()

        minimal_data_elements_right_of_partial: Tuple[ProofElement, ...]
        if last_partial_chunk_index == last_data_chunk_index:
            minimal_data_elements_right_of_partial = ()
        else:
            minimal_data_elements_right_of_partial = self.get_minimal_proof_elements(
                last_partial_chunk_index + 1,
                last_data_chunk_index - (last_partial_chunk_index + 1) + 1,
            )

        padding_elements = self.get_padding_elements()

        length_element = self.get_element((True,))

        # Now re-assembly the sections of the tree for the minimal proof for
        # the partial data.
        partial_elements = sum(
            (
                minimal_data_elements_left_of_partial,
                partial_elements,
                minimal_data_elements_right_of_partial,
                padding_elements,
                (length_element,),
            ),
            (),
        )

        return Proof(partial_elements, self.sedes)

    def get_proven_data(self) -> DataPartial:
        """
        Returns a view over the proven data which can be accessed similar to a
        bytestring by either indexing or slicing.
        """
        segments = self.get_proven_data_segments()
        return DataPartial(self.get_content_length(), segments)

    @to_tuple
    def get_proven_data_segments(self) -> Iterable[DataSegment]:
        length = self.get_content_length()

        last_data_chunk_data_size = length % CHUNK_SIZE

        next_chunk_index = 0
        segment_start_index = 0
        data_segment = b""

        # Walk over the data chunks merging contigious chunks into a single
        # segment.
        last_data_chunk_path = self.get_last_data_chunk_path()
        data_elements = self.get_elements(
            right=last_data_chunk_path, right_inclusive=True,
        )
        for el in data_elements:
            if el.depth != self.path_bit_length:
                continue

            if el.path == last_data_chunk_path:
                if last_data_chunk_data_size:
                    chunk_data = el.value[:last_data_chunk_data_size]
                else:
                    chunk_data = el.value
            else:
                chunk_data = el.value

            chunk_index = path_to_left_chunk_index(el.path, self.path_bit_length)

            if chunk_index == next_chunk_index:
                data_segment += chunk_data
            else:
                if data_segment:
                    yield DataSegment(segment_start_index, data_segment)
                data_segment = chunk_data
                segment_start_index = chunk_index * CHUNK_SIZE

            next_chunk_index = chunk_index + 1

        if length:
            if data_segment:
                yield DataSegment(segment_start_index, data_segment)
        if not length:
            yield DataSegment(0, b"")

    @to_tuple
    def get_minimal_proof_elements(
        self, start_chunk_index: int, num_chunks: int,
    ) -> Iterable[ProofElement]:
        """
        Return the minimal set of tree nodes needed to prove the designated
        section of the tree.  Nodes which belong to the same subtree are
        collapsed into the parent intermediate node.

        The `group_by_subtree` utility function implements the core logic for
        this functionality and documents how nodes are grouped.
        """
        if num_chunks < 1:
            raise Exception("Invariant")

        end_chunk_index = start_chunk_index + num_chunks - 1

        num_chunks = end_chunk_index - start_chunk_index + 1
        chunk_index_groups = group_by_subtree(start_chunk_index, num_chunks)
        groups_with_bit_lengths = tuple(
            # Each group will contain an even "power-of-two" number of
            # elements.  This tells us how many tailing bits each element has
            # which need to be truncated to get the group's common prefix.
            (group[0], (len(group) - 1).bit_length())
            for group in chunk_index_groups
        )
        subtree_paths = tuple(
            # We take a candidate element from each group and shift it to
            # remove the bits that are not common to other group members, then
            # we convert it to a tree path that all elements from this group
            # have in common.
            chunk_index_to_path(
                chunk_index >> bits_to_truncate,
                self.path_bit_length - bits_to_truncate,
            )
            for chunk_index, bits_to_truncate in groups_with_bit_lengths
        )
        for path in subtree_paths:
            element_group = self.get_elements_under(path)
            if len(element_group) == 1:
                yield element_group[0]
            else:
                yield ProofElement(path=path, value=merklize_elements(element_group))


def merklize_elements(elements: Sequence[ProofElement]) -> Hash32:
    """
    Given a set of `ProofElement` compute the `hash_tree_root`.

    This also verifies that the proof is both "well-formed" and "minimal".
    """
    elements_by_depth = groupby(operator.attrgetter("depth"), elements)
    max_depth = max(elements_by_depth.keys())

    for depth in range(max_depth, 0, -1):
        try:
            elements_at_depth = sorted(elements_by_depth.pop(depth))
        except KeyError:
            continue

        # Verify that all of the paths at this level are unique
        paths = set(el.path for el in elements_at_depth)
        if len(paths) != len(elements_at_depth):
            raise BrokenTree(
                f"Duplicate paths detected: depth={depth}  elements={elements_at_depth}"
            )

        sibling_pairs = tuple(
            (left, right)
            for left, right in sliding_window(2, elements_at_depth)
            if left.path[:-1] == right.path[:-1]
        )

        # Check to see if any of the elements didn't have a sibling which
        # indicates either a missing sibling, or a duplicate node.
        orphans = set(elements_at_depth).difference(itertools.chain(*sibling_pairs))
        if orphans:
            raise BrokenTree(f"Orphaned tree elements: dept={depth} orphans={orphans}")

        parents = tuple(
            ProofElement(path=left.path[:-1], value=hash_eth2(left.value + right.value))
            for left, right in sibling_pairs
        )

        if not elements_by_depth and len(parents) == 1:
            return parents[0].value
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
