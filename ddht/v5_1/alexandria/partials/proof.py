import bisect
from dataclasses import dataclass
import operator
from typing import (
    IO,
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
from eth_utils.toolz import cons, sliding_window
from ssz.constants import CHUNK_SIZE, ZERO_HASHES
from ssz.sedes import List as ListSedes

from ddht.exceptions import ParseError
from ddht.v5_1.alexandria.constants import POWERS_OF_TWO
from ddht.v5_1.alexandria.leb128 import encode_leb128, parse_leb128
from ddht.v5_1.alexandria.partials._utils import (
    decompose_into_powers_of_two,
    display_path,
    get_chunk_count_for_data_length,
    get_longest_common_path,
    merge_paths,
)
from ddht.v5_1.alexandria.partials.chunking import (
    MissingSegment,
    chunk_index_to_path,
    compute_chunks,
    path_to_left_chunk_index,
)
from ddht.v5_1.alexandria.partials.tree import ProofTree, construct_node_tree
from ddht.v5_1.alexandria.partials.typing import TreePath
from ddht.v5_1.alexandria.sedes import content_sedes


@dataclass(frozen=True)
class ProofElement:
    path: TreePath
    value: Hash32

    @property
    def depth(self) -> int:
        return len(self.path)

    def __str__(self) -> str:
        return f"{display_path(self.path)}: {self.value.hex()}"

    def serialize(self, previous: Optional[TreePath]) -> bytes:
        """
        Proof elements are serialized in sequence, lexographically sorted.
        Encoding is contextual on the previous encoded element.

        We encode the path as follows.

        path := path_head || path_tail

        The `path_head` are the leading bits from the previous path that are
        shared by this path.  `path_tail` are the remaining bits from this
        path.

        For example:

        previous: (0, 0, 1, 1, 0, 0, 1, 1)
        path:     (0, 0, 1, 1, 1, 0, 1, 1)

        path_head = (0, 0, 1, 1)
        path_tail = (1, 0, 1, 1)

        Encoding of the path involves:

        tail_length      := len(path_tail)
        path_tail_as_int := sum(2**i for i in range(tail_length))
        head_length := len(path_head)

        Since path's are constrained to a total of 26 bits, we can account for
        the full length in 5 bits (2**5 == 32).  The most bits we could need
        for a path are when the path shares no bits with the previous path,
        meaning 26 bits for the `path_tail`, and another 5 bits for the length,
        and zero bits needed for the `head_length`.

        We encode these three values together into a single little endian
        encoded integer and then LEB128 encode them.

        encoded_path := tail_length ^ shifted_path_as_int ^ shifted_head_length
        shifted_path_as_int := path_as_int << 5
        shifted_head_length := head_length << (5 + tail_length)

        To decode, we apply the same process in reverse.  First, decode the
        LEB128 encoded value.

        The first 5 bits encode the `tail_length`.

        Once the `tail_length` is known, we shift off that many bits to
        determine the actual `path_tail`.

        All of the remaining bits are the `head_length`.
        """
        if previous is None:
            path = self.path
            common_bits = 0
        else:
            common_path = get_longest_common_path(self.path, previous)
            common_bits = len(common_path)
            path = self.path[common_bits:]

        path_length = len(path)

        assert path_length < 32
        assert common_bits < 32 - path_length.bit_length() - len(path)

        path_as_int = sum(
            power_of_two
            for path_bit, power_of_two in zip(path, POWERS_OF_TWO[:path_length],)
            if path_bit
        )
        full_encoded_path_as_int = (
            path_length ^ (path_as_int << 5) ^ (common_bits << (5 + path_length))
        )
        return encode_leb128(full_encoded_path_as_int) + self.value

    @classmethod
    def deserialize(
        cls, stream: IO[bytes], previous: Optional[TreePath]
    ) -> "ProofElement":
        """
        Serialized proof elements can only be deserialized in the context of a
        stream with access to path of the previously decoded element.

        See the ``serialize(...)`` method for information on the format.
        """
        header_as_int = parse_leb128(stream)
        value = stream.read(32)
        if len(value) != 32:
            raise ParseError("Premature end of stream")

        path_length = header_as_int & 0b11111
        path_as_int = (header_as_int >> 5) & (2 ** path_length - 1)
        common_bits = header_as_int >> (5 + path_length)

        partial_path = tuple(
            bool(path_as_int & power_of_two)
            for power_of_two in POWERS_OF_TWO[:path_length]
        )
        if common_bits:
            if previous is None or len(previous) < common_bits:
                raise Exception("Need previous path when common bits is not 0")
            else:
                full_path = previous[:common_bits] + partial_path
        else:
            full_path = partial_path

        return cls(full_path, Hash32(value))


@to_tuple
def get_padding_elements(
    start_index: int, num_padding_chunks: int, path_bit_length: int
) -> Iterable[ProofElement]:
    """
    Get the padding elements for a proof.

    By decomposing the number of chunks needed into the powers of two which
    make up the number we can construct the minimal right hand sub-tree(s)
    needed to pad the hash tree.
    """
    for power_of_two in decompose_into_powers_of_two(num_padding_chunks):
        depth = power_of_two.bit_length() - 1
        left_index = start_index + power_of_two
        left_path = chunk_index_to_path(left_index, path_bit_length)
        padding_hash_tree_root = ZERO_HASHES[depth]
        yield ProofElement(left_path[: path_bit_length - depth], padding_hash_tree_root)


class DataPartial:
    """
    A wrapper around a partial data proof which allows data retrieval by
    indexing or slicing.

    Raise `IndexError` if the requested data is not part of the proof.
    """

    def __init__(self, length: int, segments: Tuple[Tuple[int, bytes], ...]) -> None:
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
        segment_start, segment_data = self._segments[candidate_index]
        segment_length = len(segment_data)
        segment_end = max(segment_start, segment_start + segment_length - 1)
        if not (
            segment_start <= start_at <= segment_end and data_length <= segment_length
        ):
            raise IndexError(
                f"Requested data is out of bounds: segment=({segment_start} - "
                f"{segment_end}) slice=({start_at} - {end_at})"
            )

        if isinstance(index_or_slice, slice):
            offset_slice = slice(start_at - segment_start, end_at - segment_start)
            return segment_data[offset_slice]
        elif isinstance(index_or_slice, int):
            offset_index = index_or_slice - segment_start
            return segment_data[offset_index]
        else:
            raise TypeError(f"Unsupported type: {type(index_or_slice)}")


@to_tuple
def _parse_element_stream(stream: IO[bytes]) -> Iterable[ProofElement]:
    """
    Parse a stream of serialized `ProofEelement` objects.
    """
    previous_path = None
    while True:
        # TODO: in the event that the stream still hase a few bytes but they
        # are insufficient to decode a full element and the stream is left
        # empty, this will result in a false positive of successful decoding.
        # We may need to create a thin wrapper around the stream to allow us to
        # detect this situation if we want more strict decoding.
        try:
            element = ProofElement.deserialize(stream, previous=previous_path)
        except ParseError:
            remainder = stream.read()
            if remainder:
                raise
            else:
                break
        else:
            previous_path = element.path
            yield element


class DataSegment(NamedTuple):
    start_at: int
    data: bytes


class Proof:
    """
    Representation of a merkle proof for an SSZ byte string (aka List[uint8,
    max_length=...]).
    """

    # TODO: look at where `self.sedes` is actually used and figure out if it belongs in this class.
    sedes: ListSedes
    # TODO: footgun.  Without performing verification (validate_proof(proof))
    # we don't know this value is actually valid.
    hash_tree_root: Hash32
    elements: Tuple[ProofElement, ...]

    _tree_cache: Optional[ProofTree] = None

    def __init__(
        self,
        hash_tree_root: Hash32,
        elements: Collection[ProofElement],
        sedes: ListSedes,
        tree: Optional[ProofTree] = None,
    ) -> None:
        self.hash_tree_root = hash_tree_root
        self.elements = tuple(sorted(elements, key=operator.attrgetter("path")))
        self.sedes = sedes
        self._tree_cache = tree

    def __eq__(self, other: Any) -> bool:
        if type(self) is not type(other):
            return False
        else:
            return (  # type: ignore
                self.hash_tree_root == other.hash_tree_root
                and self.elements == other.elements
            )

    @property
    def path_bit_length(self) -> int:
        return self.sedes.chunk_count.bit_length()  # type: ignore

    def get_tree(self) -> ProofTree:
        # TODO: Measure cost of tree construction.  The tree is useful for
        # proof construction but it's probably more expensive than is necessary
        # for verifying state root.  Also, it uses a lot of recursive
        # algorithms which aren't very efficient.
        if self._tree_cache is None:
            tree_data = tuple((el.path, el.value) for el in self.elements)
            root_node = construct_node_tree(tree_data, (), self.path_bit_length)
            self._tree_cache = ProofTree(root_node)
        return self._tree_cache

    def serialize(self) -> bytes:
        # TODO: we can elimenate the need for the tree object by 1) directly
        # fetching the length node since we know its path and 2) directly
        # filtering the data elements out since we know the bounds on their
        # paths.
        tree = self.get_tree()
        length = tree.get_data_length()
        num_data_chunks = get_chunk_count_for_data_length(length)
        last_data_chunk_index = max(0, num_data_chunks - 1)
        last_data_chunk_path = chunk_index_to_path(
            last_data_chunk_index, self.path_bit_length
        )

        data_nodes = tuple(
            node for node in tree.walk(end_at=last_data_chunk_path) if node.is_terminal
        )
        data_only_elements = tuple(
            ProofElement(path=node.path, value=node.get_value()) for node in data_nodes
        )

        serialized_elements = b"".join(
            (
                element.serialize(previous.path if previous is not None else None)
                for previous, element in sliding_window(
                    2,
                    cons(
                        None,
                        sorted(data_only_elements, key=operator.attrgetter("path")),
                    ),
                )
            )
        )
        return encode_leb128(length) + serialized_elements

    @classmethod
    def deserialize(
        cls,
        stream: IO[bytes],
        hash_tree_root: Hash32,
        sedes: ListSedes = content_sedes,
    ) -> "Proof":
        length = parse_leb128(stream)

        data_elements = _parse_element_stream(stream)

        num_data_chunks = get_chunk_count_for_data_length(length)
        last_data_chunk_index = max(0, num_data_chunks - 1)

        num_padding_chunks = sedes.chunk_count - max(1, num_data_chunks)

        padding_elements = get_padding_elements(
            last_data_chunk_index, num_padding_chunks, sedes.chunk_count.bit_length(),
        )
        assert not any(el.path == (True,) for el in data_elements)
        assert not any(el.path == (True,) for el in padding_elements)
        length_element = ProofElement(
            path=(True,), value=Hash32(length.to_bytes(CHUNK_SIZE, "little"))
        )

        elements = data_elements + padding_elements + (length_element,)

        proof = cls(hash_tree_root, elements, sedes)
        validate_proof(proof)
        return cls(hash_tree_root, elements, sedes)

    def to_partial(self, start_at: int, partial_data_length: int) -> "Proof":
        """
        Return another proof with the minimal number of tree elements necessary
        to prove the slice of the underlying bytestring denoted by the
        `start_at` and `partial_data_length` parameters.
        """
        # First retrieve the overall data length from the proof.  The `length`
        # shoudl always be found on the path `(True,)` which should always be present in the
        tree = self.get_tree()
        length = tree.get_data_length()

        if start_at >= length:
            raise Exception(
                f"Starting point for partial is outside of data bounds: "
                f"start_at={start_at} length={length}"
            )

        # Ensure that we aren't requesting data that exceeds the overall length
        # of the underlying data.
        end_at = start_at + partial_data_length
        if end_at > length:
            raise Exception(
                f"Cannot create partial that exceeds the data length: "
                f"end_at={end_at} length={length}"
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
        leaf_nodes_for_partial = tuple(
            node
            for node in tree.walk(first_partial_chunk_path, last_partial_chunk_path)
            if node.is_leaf
        )
        expected_partial_chunk_count = (
            last_partial_chunk_index - first_partial_chunk_index
        ) + 1
        if len(leaf_nodes_for_partial) != expected_partial_chunk_count:
            raise Exception(
                "Proof is missing leaf nodes required for partial construction."
            )

        # Compute the total number of non-padding chunks in the tree.
        num_data_chunks = get_chunk_count_for_data_length(length)
        last_data_chunk_index = max(0, num_data_chunks - 1)
        last_data_chunk_path = chunk_index_to_path(
            last_data_chunk_index, self.path_bit_length
        )

        #
        # Data chunks left of the partial
        #
        # start-at : 0
        # end-at   : first_partial_chunk_index - 1
        if first_partial_chunk_index > 0:
            nodes_left_of_partial = tree.get_subtree_proof_nodes(
                chunk_index_to_path(0, self.path_bit_length),
                chunk_index_to_path(
                    first_partial_chunk_index - 1, self.path_bit_length
                ),
            )
        else:
            nodes_left_of_partial = ()

        #
        # Data chunks right of the partial
        #
        # start-at : last_partial_chunk_index + 1
        # end-at   : last_data_chunk_index
        if last_partial_chunk_index + 1 <= last_data_chunk_index:
            nodes_right_of_partial = tree.get_subtree_proof_nodes(
                chunk_index_to_path(last_partial_chunk_index + 1, self.path_bit_length),
                last_data_chunk_path,
            )
        else:
            nodes_right_of_partial = ()

        #
        # Padding
        #
        # start-at : last_partial_chunk_index + 1
        # end-at   : N/A
        if last_data_chunk_index + 1 <= self.sedes.chunk_count:
            first_padding_chunk_path = chunk_index_to_path(
                last_data_chunk_index + 1, self.path_bit_length
            )
            actual_first_padding_node = tree.get_deepest_node_on_path(
                first_padding_chunk_path
            )
            padding_nodes = tuple(
                node
                for node in tree.walk(actual_first_padding_node.path)
                if node.is_terminal
            )
        else:
            padding_nodes = ()

        # Now re-assembly the sections of the tree for the minimal proof for
        # the partial data.
        partial_nodes = (
            nodes_left_of_partial
            + leaf_nodes_for_partial
            + nodes_right_of_partial
            + padding_nodes
        )
        partial_elements = tuple(
            ProofElement(path=node.path, value=node.get_value())
            for node in partial_nodes
        )
        proof = Proof(self.hash_tree_root, partial_elements, self.sedes)
        validate_proof(proof)

        return Proof(self.hash_tree_root, partial_elements, self.sedes)

    def get_proven_data(self) -> DataPartial:
        """
        Returns a view over the proven data which can be accessed similar to a
        bytestring by either indexing or slicing.
        """
        length = self.get_tree().get_data_length()
        segments = self.get_proven_data_segments(length)
        return DataPartial(length, segments)

    @property
    def is_complete(self) -> bool:
        # TODO: something less than O(n) complexity
        return (
            len(tuple(self.get_missing_segments(self.get_tree().get_data_length())))
            == 0
        )

    def has_chunk(self, chunk_index: int) -> bool:
        path = chunk_index_to_path(chunk_index, self.path_bit_length)
        # TODO: something less than O(n) complexity
        return path in {el.path for el in self.elements}

    @to_tuple
    def get_missing_segments(self, length: int) -> Iterable[MissingSegment]:
        num_data_chunks = get_chunk_count_for_data_length(length)

        last_data_chunk_index = max(0, num_data_chunks - 1)
        last_data_chunk_path = chunk_index_to_path(
            last_data_chunk_index, self.path_bit_length
        )

        last_data_chunk_size = length % CHUNK_SIZE or CHUNK_SIZE

        tree = self.get_tree()

        next_chunk_index = 0

        for node in tree.walk(end_at=last_data_chunk_path):
            if not node.is_leaf:
                continue

            chunk_index = path_to_left_chunk_index(node.path, self.path_bit_length)

            # If the chunk index matches the expected next chunk index then we
            # are in a data segment and should continue.  Otherwise we have
            # skipped over some data and need to yield a segment.
            if chunk_index > next_chunk_index:
                yield MissingSegment(
                    next_chunk_index * 32, (chunk_index - next_chunk_index) * 32,
                )

            next_chunk_index = chunk_index + 1

        # To detect the case where we are missing the last segment, we look to see if
        if next_chunk_index <= last_data_chunk_index:
            last_segment_start = next_chunk_index * 32
            last_segment_length = (
                last_data_chunk_index * 32 + last_data_chunk_size - last_segment_start
            )
            yield MissingSegment(
                last_segment_start, last_segment_length,
            )

    @to_tuple
    def get_proven_data_segments(self, length: int) -> Iterable[DataSegment]:
        num_data_chunks = get_chunk_count_for_data_length(length)

        last_data_chunk_index = max(0, num_data_chunks - 1)
        last_data_chunk_path = chunk_index_to_path(
            last_data_chunk_index, self.path_bit_length
        )

        last_data_chunk_data_size = length % CHUNK_SIZE

        next_chunk_index = 0
        segment_start_index = 0
        data_segment = b""

        tree = self.get_tree()

        # Walk over the section of the tree where the data chunks are located,
        # merging contigious chunks into a single segment.
        for node in tree.walk(end_at=last_data_chunk_path):
            if not node.is_leaf:
                continue
            chunk_index = path_to_left_chunk_index(node.path, self.path_bit_length)

            if chunk_index == last_data_chunk_index:
                if last_data_chunk_data_size:
                    chunk_data = node.get_value()[:last_data_chunk_data_size]
                else:
                    chunk_data = node.get_value()
            else:
                chunk_data = node.get_value()

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

    def merge(self, other: "Proof") -> "Proof":
        if self.hash_tree_root != other.hash_tree_root:
            raise ValueError(
                f"Mismatched roots: {self.hash_tree_root.hex()} !+ {other.hash_tree_root.hex()}"
            )

        all_self_elements_by_path = {el.path: el for el in self.elements}
        all_other_elements_by_path = {el.path: el for el in other.elements}

        all_self_paths = {el.path for el in self.elements}
        all_other_paths = {el.path for el in other.elements}

        common_paths = all_self_paths & all_other_paths
        paths_to_merge = sorted(all_self_paths ^ all_other_paths)

        merged_paths = merge_paths(*paths_to_merge)

        common_elements = tuple(
            all_self_elements_by_path[path] for path in common_paths
        )
        merged_elements_from_self = tuple(
            all_self_elements_by_path[path]
            for path in merged_paths
            if path in all_self_elements_by_path
        )
        merged_elements_from_other = tuple(
            all_other_elements_by_path[path]
            for path in merged_paths
            if path in all_other_elements_by_path
        )

        all_merged_elements = (
            common_elements + merged_elements_from_self + merged_elements_from_other
        )

        proof = Proof(self.hash_tree_root, all_merged_elements, self.sedes)
        validate_proof(proof)
        return proof


def validate_proof(proof: Proof) -> None:
    if proof.get_tree().get_hash_tree_root() != proof.hash_tree_root:
        raise ValidationError("Merkle root mismatch")


def is_proof_valid(proof: Proof) -> bool:
    return proof.get_tree().get_hash_tree_root() == proof.hash_tree_root


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


def compute_proof(content: bytes, sedes: ListSedes) -> Proof:
    """
    Compute the full proof, including the mixed-in length value.
    """
    chunks = compute_chunks(content)

    chunk_count = sedes.chunk_count
    path_bit_length = chunk_count.bit_length()

    data_elements = compute_proof_elements(chunks, chunk_count)
    length_element = ProofElement(
        path=(True,), value=Hash32(len(content).to_bytes(CHUNK_SIZE, "little")),
    )
    all_elements = data_elements + (length_element,)
    tree_data = tuple((el.path, el.value) for el in all_elements)

    root_node = construct_node_tree(tree_data, (), path_bit_length)
    tree = ProofTree(root_node)
    hash_tree_root = tree.get_hash_tree_root()

    return Proof(hash_tree_root, all_elements, sedes, tree=tree)
