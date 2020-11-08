import enum
from typing import Collection, Iterable, List, Optional, Tuple

from eth_typing import Hash32
from eth_utils import ValidationError, to_tuple
from eth_utils.toolz import sliding_window
from ssz.constants import ZERO_HASHES
from ssz.hash import hash_eth2

from ddht.v5_1.alexandria.partials._utils import display_path
from ddht.v5_1.alexandria.partials.chunking import (
    chunk_index_to_path,
    group_by_subtree,
    path_to_left_chunk_index,
)
from ddht.v5_1.alexandria.partials.typing import TreePath


class NodePosition(enum.IntEnum):
    left = 0
    right = 1
    root = 2


def construct_node_tree(
    tree_data: Collection[Tuple[TreePath, Hash32]],
    current_path: TreePath,
    path_bit_length: int,
) -> "Node":
    """
    Construct the tree of nodes for a ProofTree.

    TODO: How can we do this non-recursively....
    """
    #
    # TERMINAL NODE
    #
    # - First we check to see if the tree terminates at this path.  If so there
    # should only be a single node.
    terminal_data = tuple(
        (path, value) for (path, value) in tree_data if path == current_path
    )

    if terminal_data:
        if len(terminal_data) > 1:
            raise ValidationError(f"Multiple terminal nodes: {terminal_data}")
        elif not current_path:
            raise ValidationError(
                f"Invalid tree termination at root path: {terminal_data}"
            )

        _, terminal_value = terminal_data[0]
        return Node(
            path=current_path,
            value=terminal_value,
            left=None,
            right=None,
            position=NodePosition(current_path[-1]),
            path_bit_length=path_bit_length,
        )

    depth = len(current_path)

    # If the tree does not terminate then it always branches in both
    # directions.  Split the data into their left/right sides and construct
    # the subtrees, and then the node at this level.
    left_data = tuple(
        (path, value) for (path, value) in tree_data if path[depth] is False
    )
    right_data = tuple(
        (path, value) for (path, value) in tree_data if path[depth] is True
    )

    left_node: Optional[Node]
    right_node: Optional[Node]

    if left_data:
        left_node = construct_node_tree(
            left_data,
            current_path=current_path + (False,),
            path_bit_length=path_bit_length,
        )
    else:
        left_node = None

    if right_data:
        right_node = construct_node_tree(
            right_data,
            current_path=current_path + (True,),
            path_bit_length=path_bit_length,
        )
    else:
        right_node = None

    if current_path:
        position = NodePosition(current_path[-1])
    else:
        position = NodePosition.root

    return Node(
        path=current_path,
        value=None,
        left=left_node,
        right=right_node,
        position=position,
        path_bit_length=path_bit_length,
    )


class BrokenTree(Exception):
    """
    Exception signaling that there are missing nodes in a `ProofTree` which are
    required for the proof to be valid.
    """

    ...


class TerminalPathError(Exception):
    """
    Exception signaling an attempt to navigate too deep into a tree.

    Can occur at either a leaf node, or a padding node.
    """

    ...


class Node:
    """
    Representation of a single node within a proof tree.
    """

    _computed_value: Optional[Hash32] = None

    def __init__(
        self,
        path: TreePath,
        value: Optional[Hash32],
        left: Optional["Node"],
        right: Optional["Node"],
        position: NodePosition,
        path_bit_length: int,
    ) -> None:
        self.path = path
        self._value = value
        self._left = left
        self._right = right
        self.position = position
        self._path_bit_length = path_bit_length

    def __str__(self) -> str:
        if self._left is not None:
            left = "L"
        else:
            left = "?" if self.is_terminal else "X"

        if self._right is not None:
            right = "R"
        else:
            right = "?" if self.is_terminal else "X"
        return f"Node[path={display_path(self.path)} children=({left}^{right})]"

    def get_value(self) -> Hash32:
        """
        The value at this node.  For intermediate nodes, this is a hash.  For
        leaf nodes this is the actual data.  Intermediate nodes that were not
        part of the proof are lazily computed.
        """
        if self._value is not None:
            return self._value
        elif self._computed_value is None:
            if self._left is None or self._right is None:
                raise BrokenTree(f"Tree path breaks below: {display_path(self.path)}")
            self._computed_value = hash_eth2(
                self._left.get_value() + self._right.get_value()
            )
        return self._computed_value

    @property
    def left(self) -> "Node":
        """
        The left child of this node.
        """
        if self._left is None:
            if self.is_terminal:
                raise TerminalPathError(
                    f"Tree path terminates: {display_path(self.path)}"
                )
            else:
                raise BrokenTree(f"Tree path breaks left of: {display_path(self.path)}")
        return self._left

    @property
    def right(self) -> "Node":
        """
        The right child of this node.
        """
        if self._right is None:
            if self.is_terminal:
                raise TerminalPathError(
                    f"Tree path terminates: {display_path(self.path)}"
                )
            else:
                raise BrokenTree(
                    f"Tree path breaks right of: {display_path(self.path)}"
                )
        return self._right

    @property
    def depth(self) -> int:
        """
        The number of layers below the merkle root that this node is located.
        """
        return len(self.path)

    @property
    def is_computed(self) -> bool:
        """
        Boolean whether this node was computed or part of the original proof.
        """
        return not self._value

    @property
    def is_intermediate(self) -> bool:
        """
        Boolean whether this node is an intermediate tree node or part of the actual data.
        """
        return not self.is_leaf

    @property
    def is_leaf(self) -> bool:
        """
        Boolean whether this node is part of the actual data.
        """
        return len(self.path) == self._path_bit_length or self.path == (True,)

    @property
    def is_terminal(self) -> bool:
        """
        Boolean whether it is possible to navigate below this node in the tree.
        """
        return self._left is None and self._right is None and self._value is not None

    @property
    def is_padding(self) -> bool:
        """
        Boolean whether this node *looks* like padding.  Will return true for
        content trees in which the data is empty bytes.

        TODO: Fix the false-positive cases for this helper.
        """
        return self._value == ZERO_HASHES[self._path_bit_length - self.depth]  # type: ignore


class Visitor:
    """
    Thin helper for navigating around a ProofTree.
    """

    tree: "ProofTree"
    node: Node

    def __init__(self, tree: "ProofTree", node: Node) -> None:
        self.tree = tree
        self.node = node

    def visit_left(self) -> "Visitor":
        return Visitor(self.tree, self.node.left)

    def visit_right(self) -> "Visitor":
        return Visitor(self.tree, self.node.right)


class ProofTree:
    """
    Tree representation of a Proof
    """

    root_node: Node

    _hash_tree_root: Optional[Hash32] = None

    def __init__(self, root_node: Node) -> None:
        self.root_node = root_node

    def get_hash_tree_root(self) -> Hash32:
        return self.root_node.get_value()

    def get_data_length(self) -> int:
        length_node = self.get_node((True,))
        if not length_node.is_leaf:
            raise Exception("Bad Proof")
        return int.from_bytes(length_node.get_value(), "little")

    def get_node(self, path: TreePath) -> Node:
        return self.get_nodes_on_path(path)[-1]

    @to_tuple
    def get_nodes_on_path(self, path: TreePath) -> Iterable[Node]:
        node = self.root_node
        yield node
        for el in path:
            if el is False:
                node = node.left
            elif el is True:
                node = node.right
            else:
                raise Exception("Invariant")
            yield node

    def get_deepest_node_on_path(self, path: TreePath) -> Node:
        node = self.root_node
        for el in path:
            if el is False:
                node = node.left
            elif el is True:
                node = node.right
            else:
                raise Exception("Invariant")
            if node.is_terminal:
                break

        return node

    def visit(self, path: TreePath) -> Visitor:
        node = self.get_node(path)
        return Visitor(self, node)

    def visit_left(self) -> Visitor:
        return self.visit((False,))

    def visit_right(self) -> Visitor:
        return self.visit((True,))

    def walk(
        self, start_at: Optional[TreePath] = None, end_at: Optional[TreePath] = None
    ) -> Iterable[Node]:
        if end_at is not None and start_at is not None and end_at < start_at:
            raise Exception("Invariant")

        nodes_on_path: Tuple[Node, ...]
        if start_at is None:
            nodes_on_path = (self.root_node,)
        else:
            nodes_on_path = self.get_nodes_on_path(start_at)

        stack: List[Node] = []
        for parent, child in sliding_window(2, nodes_on_path):
            if child.position is NodePosition.left:
                stack.append(parent.right)

        stack.append(nodes_on_path[-1])

        while stack:
            node = stack.pop()
            if end_at is not None and node.path > end_at:
                break

            yield node
            if not node.is_terminal:
                stack.append(node.right)
                stack.append(node.left)

    @to_tuple
    def get_subtree_proof_nodes(
        self, start_at: TreePath, end_at: TreePath
    ) -> Iterable[Node]:
        """
        Return the minimal set of tree nodes needed to prove the designated
        section of the tree.  Nodes which belong to the same subtree are
        collapsed into the parent intermediate node.

        The `group_by_subtree` utility function implements the core logic for
        this functionality and documents how nodes are grouped.
        """
        if len(start_at) != len(end_at):
            raise Exception("Paths must be at the same depth")
        path_bit_length = len(start_at)
        first_chunk_index = path_to_left_chunk_index(start_at, path_bit_length)
        last_chunk_index = path_to_left_chunk_index(end_at, path_bit_length)

        if last_chunk_index < first_chunk_index:
            raise Exception("end path is before starting path")

        num_chunks = last_chunk_index - first_chunk_index + 1
        chunk_index_groups = group_by_subtree(first_chunk_index, num_chunks)
        groups_with_bit_lengths = tuple(
            # Each group will contain an even "power-of-two" number of
            # elements.  This tells us how many tailing bits each element has
            # which need to be truncated to get the group's common prefix.
            (group[0], (len(group) - 1).bit_length())
            for group in chunk_index_groups
        )
        subtree_node_paths = tuple(
            # We take a candidate element from each group and shift it to
            # remove the bits that are not common to other group members, then
            # we convert it to a tree path that all elements from this group
            # have in common.
            chunk_index_to_path(
                chunk_index >> bits_to_truncate, path_bit_length - bits_to_truncate,
            )
            for chunk_index, bits_to_truncate in groups_with_bit_lengths
        )
        for path in subtree_node_paths:
            yield self.get_node(path)
