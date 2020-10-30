from dataclasses import dataclass
import operator
from typing import Any, Collection, Iterable, Optional, Sequence, Tuple

from eth_typing import Hash32
from eth_utils import to_tuple
from ssz.constants import CHUNK_SIZE, ZERO_HASHES
from ssz.sedes import List as ListSedes

from ddht.v5_1.alexandria.partials._utils import (
    decompose_into_powers_of_two,
    display_path,
)
from ddht.v5_1.alexandria.partials.chunking import chunk_index_to_path, compute_chunks
from ddht.v5_1.alexandria.partials.tree import ProofTree, construct_node_tree
from ddht.v5_1.alexandria.partials.typing import TreePath


@dataclass(frozen=True)
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
    path_bit_length = chunk_count.bit_length()

    data_elements = compute_proof_elements(chunks, chunk_count)
    length_element = ProofElement(
        path=(True,), value=Hash32(len(data).to_bytes(CHUNK_SIZE, "little")),
    )
    all_elements = data_elements + (length_element,)
    tree_data = tuple((el.path, el.value) for el in all_elements)

    root_node = construct_node_tree(tree_data, (), path_bit_length)
    tree = ProofTree(root_node)
    hash_tree_root = tree.get_hash_tree_root()

    return Proof(hash_tree_root, all_elements, sedes, tree=tree)


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
