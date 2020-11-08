from hypothesis import given
from hypothesis import strategies as st
import pytest
from ssz.constants import ZERO_HASHES

from ddht.v5_1.alexandria.partials.proof import compute_proof
from ddht.v5_1.alexandria.partials.tree import (
    BrokenTree,
    ProofTree,
    TerminalPathError,
    construct_node_tree,
)
from ddht.v5_1.alexandria.sedes import ByteList

short_content_sedes = ByteList(max_length=32 * 16)


@pytest.fixture
def short_proof():
    data = b"\x01" * 32 + b"\x02" * 32 + b"\x03" * 32 + b"\x04" * 32 + b"\x05" * 32
    proof = compute_proof(data, sedes=short_content_sedes)
    return proof


#
# Tree Walking
#
# The following diagram visualizes the expected walk order of the tree wich
# each node labeled with the letters A-Q.  Nodes labeled with a `?` are
# expected to not be reached during a tree walk.
r"""
0:                               A
                                / \
                              /     \
1:                           B       Q
                            / \   (length)
                          /     \
                        /         \
                      /             \
                    /                 \
                  /                     \
                /                         \
2:             C                           P
             /   \                       /   \
           /       \                   /       \
         /           \               /           \
3:      D             K             ?             ?
       / \           / \           / \           / \
      /   \         /   \         /   \         /   \
4:   E     H       L     O       ?     ?       ?     ?
    / \   / \     / \   / \     / \   / \     / \   / \
5: F   G I   J   M   N ?   ?   ?   ? ?   ?   ?   ? ?   ?

  |<-----DATA---->| |<-----------PADDING-------------->|
"""


def p(*crumbs):
    return tuple(bool(crumb) for crumb in crumbs)


# These are the paths that we expect to encounter on a full walk of the tree.
EXPECTED_WALK_ORDER = (
    p(),  # A
    p(0,),  # B
    p(0, 0),  # C
    p(0, 0, 0),  # D
    p(0, 0, 0, 0),  # E
    p(0, 0, 0, 0, 0),  # F
    p(0, 0, 0, 0, 1),  # G
    p(0, 0, 0, 1),  # H
    p(0, 0, 0, 1, 0),  # I
    p(0, 0, 0, 1, 1),  # J
    p(0, 0, 1),  # K
    p(0, 0, 1, 0),  # L
    p(0, 0, 1, 0, 0),  # M
    p(0, 0, 1, 0, 1),  # N
    p(0, 0, 1, 1),  # O
    p(0, 1),  # P
    p(1,),  # Q
)


def test_proof_tree_full_traversal(short_proof):
    tree = short_proof.get_tree()

    nodes_in_walk_order = tuple(tree.walk())
    assert len(nodes_in_walk_order) == len(EXPECTED_WALK_ORDER)
    for idx, (node, expected_path) in enumerate(
        zip(nodes_in_walk_order, EXPECTED_WALK_ORDER)
    ):
        assert node.path == expected_path


@given(
    start_at_index=st.integers(min_value=0, max_value=len(EXPECTED_WALK_ORDER) - 1),
    num_nodes_to_walk=st.one_of(
        st.none(), st.integers(min_value=1, max_value=len(EXPECTED_WALK_ORDER) - 1),
    ),
)
def test_proof_tree_partial_traversal(short_proof, start_at_index, num_nodes_to_walk):
    start_at = EXPECTED_WALK_ORDER[start_at_index]
    if num_nodes_to_walk is None:
        end_at_index = len(EXPECTED_WALK_ORDER)
        end_at = None
    else:
        end_at_index = min(len(EXPECTED_WALK_ORDER), start_at_index + num_nodes_to_walk)
        end_at = EXPECTED_WALK_ORDER[end_at_index - 1]

    expected_paths = EXPECTED_WALK_ORDER[start_at_index:end_at_index]

    tree = short_proof.get_tree()
    nodes_in_walk_order = tuple(tree.walk(start_at, end_at))
    assert len(nodes_in_walk_order) == len(expected_paths)
    for idx, (node, expected_path) in enumerate(
        zip(nodes_in_walk_order, expected_paths)
    ):
        assert node.path == expected_path


def test_proof_tree_visitor_api(short_proof):
    r"""
    0:                               X
                                    / \
                                  /     \
    1:                           0       1
                                / \   (length)
                              /     \
                            /         \
                          /             \
                        /                 \
                      /                     \
                    /                         \
    2:             0                           P
                 /   \                       /   \
               /       \                   /       \
             /           \               /           \
    3:      0             1             0             1
           / \           / \           / \           / \
          /   \         /   \         /   \         /   \
    4:   0     1       0     P       0     1       0     1
        / \   / \     / \   / \     / \   / \     / \   / \
    5: 0   1 0   1   0   P 0   1   0   1 0   1   0   1 0   1

      |<-----DATA---->| |<-----------PADDING-------------->|
    """
    tree = short_proof.get_tree()

    assert tree.get_hash_tree_root() == short_proof.hash_tree_root

    walked_nodes = tuple(tree.walk())
    walked_paths = tuple(sorted(node.path for node in walked_nodes if node.is_terminal))

    expected_paths = tuple(sorted(el.path for el in short_proof.elements))

    assert walked_paths == expected_paths

    #
    # Check finding deepest node on a path which would otherwise extend past a
    # terminal node.
    #
    node_011 = tree.get_deepest_node_on_path((False, False, True, True))
    assert node_011.path == ((False, False, True, True))
    assert tree.get_deepest_node_on_path((False, False, True, True, False)) == node_011
    assert tree.get_deepest_node_on_path((False, False, True, True, True)) == node_011

    #
    # Now we walk to every node in the tree and verify it looks as expected.
    #

    #    X
    #   /
    #  0
    #   \
    #    1
    #
    visitor_01 = tree.visit_left().visit_right()

    assert not visitor_01.node.is_computed
    assert visitor_01.node.is_intermediate
    assert visitor_01.node.is_terminal
    assert visitor_01.node.get_value() == ZERO_HASHES[3]
    assert visitor_01.node.is_padding
    assert not visitor_01.node.is_leaf
    assert visitor_01.node.depth == 2

    with pytest.raises(TerminalPathError):
        visitor_01.visit_left()
    with pytest.raises(TerminalPathError):
        visitor_01.visit_right()

    #      X
    #     /
    #    0
    #   /
    #  0
    #
    visitor_00 = tree.visit_left().visit_left()

    assert visitor_00.node.is_computed
    assert visitor_00.node.is_intermediate
    assert not visitor_00.node.is_terminal
    assert not visitor_00.node.is_padding
    assert not visitor_00.node.is_leaf
    assert visitor_00.node.depth == 2

    #        X
    #       /
    #      0
    #     /
    #    0
    #   /
    #  0
    #
    visitor_000 = visitor_00.visit_left()

    assert visitor_000.node.is_computed
    assert visitor_000.node.is_intermediate
    assert not visitor_000.node.is_terminal
    assert not visitor_000.node.is_padding
    assert not visitor_000.node.is_leaf
    assert visitor_000.node.depth == 3

    #      X
    #     /
    #    0
    #   /
    #  0
    #   \
    #    1
    #
    visitor_001 = visitor_00.visit_right()

    assert visitor_001.node.is_computed
    assert visitor_001.node.is_intermediate
    assert not visitor_001.node.is_terminal
    assert not visitor_001.node.is_padding
    assert not visitor_001.node.is_leaf
    assert visitor_001.node.depth == 3

    #      X
    #     /
    #    0
    #   /
    #  0
    #   \
    #    1
    #     \
    #      1
    #
    visitor_0011 = visitor_001.visit_right()

    assert not visitor_0011.node.is_computed
    assert visitor_0011.node.is_intermediate
    assert visitor_0011.node.is_terminal
    assert visitor_0011.node.is_padding
    assert visitor_0011.node.get_value() == ZERO_HASHES[1]
    assert not visitor_0011.node.is_leaf
    assert visitor_0011.node.depth == 4

    with pytest.raises(TerminalPathError):
        visitor_0011.visit_right()
    with pytest.raises(TerminalPathError):
        visitor_0011.visit_left()

    #      X
    #     /
    #    0
    #   /
    #  0
    #   \
    #    1
    #   /
    #  0
    #
    visitor_0010 = visitor_001.visit_left()

    assert visitor_0010.node.is_computed
    assert visitor_0010.node.is_intermediate
    assert not visitor_0010.node.is_terminal
    assert not visitor_0010.node.is_padding
    assert not visitor_0010.node.is_leaf
    assert visitor_0010.node.depth == 4

    #      X
    #     /
    #    0
    #   /
    #  0
    #   \
    #    1
    #   /
    #  0
    #   \
    #    1
    #
    visitor_00101 = visitor_0010.visit_right()

    assert not visitor_00101.node.is_computed
    assert not visitor_00101.node.is_intermediate
    assert visitor_00101.node.is_terminal
    assert visitor_00101.node.is_padding
    assert visitor_00101.node.get_value() == ZERO_HASHES[0]
    assert visitor_00101.node.is_leaf
    assert visitor_00101.node.depth == 5

    with pytest.raises(TerminalPathError):
        visitor_00101.visit_right()
    with pytest.raises(TerminalPathError):
        visitor_00101.visit_left()

    #        X
    #       /
    #      0
    #     /
    #    0
    #     \
    #      1
    #     /
    #    0
    #   /
    #  0
    #
    visitor_00100 = visitor_0010.visit_left()

    assert not visitor_00100.node.is_computed
    assert not visitor_00100.node.is_intermediate
    assert visitor_00100.node.is_terminal
    assert not visitor_00100.node.is_padding
    assert visitor_00100.node.is_leaf
    assert visitor_00100.node.depth == 5
    assert visitor_00100.node.get_value() == b"\x05" * 32

    with pytest.raises(TerminalPathError):
        visitor_00100.visit_right()
    with pytest.raises(TerminalPathError):
        visitor_00100.visit_left()

    #          X
    #         /
    #        0
    #       /
    #      0
    #     /
    #    0
    #   / \
    #  0   1
    #
    visitor_0000 = visitor_000.visit_left()
    visitor_0001 = visitor_000.visit_right()

    for visitor in (visitor_0000, visitor_0001):
        assert visitor.node.is_computed
        assert visitor.node.is_intermediate
        assert not visitor.node.is_terminal
        assert not visitor.node.is_padding
        assert not visitor.node.is_leaf
        assert visitor.node.depth == 4

    visitor_00000 = visitor_0000.visit_left()
    visitor_00001 = visitor_0000.visit_right()
    visitor_00010 = visitor_0001.visit_left()
    visitor_00011 = visitor_0001.visit_right()

    #             X
    #            /
    #           0
    #          /
    #         0
    #        /
    #       0
    #      / \
    #     /   \
    #    0     1
    #   / \   / \
    #  0   1 0   1
    #
    for visitor in (visitor_00000, visitor_00001, visitor_00010, visitor_00011):
        assert not visitor.node.is_computed
        assert not visitor.node.is_intermediate
        assert visitor.node.is_terminal
        assert not visitor.node.is_padding
        assert visitor.node.is_leaf
        assert visitor.node.depth == 5

        with pytest.raises(TerminalPathError):
            visitor.visit_right()
        with pytest.raises(TerminalPathError):
            visitor.visit_left()

    assert visitor_00000.node.get_value() == b"\x01" * 32
    assert visitor_00001.node.get_value() == b"\x02" * 32
    assert visitor_00010.node.get_value() == b"\x03" * 32
    assert visitor_00011.node.get_value() == b"\x04" * 32


def test_tree_proof_with_empty_data():
    proof = compute_proof(b"", sedes=short_content_sedes)
    tree = proof.get_tree()

    node = tree.get_node((False, False, False, False, False))

    assert not node.is_computed
    assert not node.is_intermediate
    assert node.is_terminal
    # TODO: false positive that needs to be addressed
    # assert not node.is_padding
    assert node.is_leaf
    assert node.depth == 5

    walked_nodes = tuple(node for node in tree.walk() if node.is_leaf)
    # (padded-data-node, padding-node, length-node)
    assert len(walked_nodes) == 3


def test_tree_proof_with_missing_leaf_node(short_proof):
    all_proof_elements = short_proof.elements

    assert all_proof_elements[3].value == b"\x04" * 32

    # Remove the element that would have been @ 0011
    proof_elements = all_proof_elements[:3] + all_proof_elements[4:]
    tree_data = tuple((el.path, el.value) for el in proof_elements)
    tree = ProofTree(
        construct_node_tree(tree_data, (), short_proof.sedes.chunk_count.bit_length())
    )

    with pytest.raises(BrokenTree):
        # TODO: property access with side-effects is surprising
        tree.get_hash_tree_root()

    with pytest.raises(BrokenTree):
        tuple(tree.walk())

    # we should still be able to visit the nodes around the missing one.
    tree.visit((False, False, False, True))  # the parent of the node
    tree.visit((False, False, False, True, False))  # the sibling node to the left

    with pytest.raises(BrokenTree):
        tree.visit((False, False, False, True, True))


def test_tree_proof_with_missing_intermediate_node(short_proof):
    all_proof_elements = short_proof.elements

    assert all_proof_elements[6].value == ZERO_HASHES[1]

    # Remove the element that would have been @ 011
    proof_elements = all_proof_elements[:6] + all_proof_elements[7:]
    tree_data = tuple((el.path, el.value) for el in proof_elements)
    tree = ProofTree(
        construct_node_tree(tree_data, (), short_proof.sedes.chunk_count.bit_length())
    )

    with pytest.raises(BrokenTree):
        # TODO: property access with side-effects is surprising
        tree.get_hash_tree_root()

    with pytest.raises(BrokenTree):
        tuple(tree.walk())

    # we should still be able to visit the nodes around the missing one.
    tree.visit((False, False, True))  # the parent of the missing node
    tree.visit((False, False, True, False))  # the sibling node to the left

    with pytest.raises(BrokenTree):
        tree.visit((False, False, True, True))


def test_proof_tree_subtree_proof(short_proof):
    r"""
    0:                               X
                                    / \
                                  /     \
    1:                           0       1
                                / \   (length)
                              /     \
                            /         \
                          /             \
                        /                 \
                      /                     \
                    /                         \
    2:             0                           P
                 /   \                       /   \
               /       \                   /       \
             /           \               /           \
    3:      0             1             0             1
           / \           / \           / \           / \
          /   \         /   \         /   \         /   \
    4:   0     1       0     P       0     1       0     1
        / \   / \     / \   / \     / \   / \     / \   / \
    5: 0   1 0   1   0   P 0   1   0   1 0   1   0   1 0   1

      |<-----DATA---->| |<-----------PADDING-------------->|
    """
    tree = short_proof.get_tree()

    # two sibling nodes should return the single parent node for those siblings
    subtree_proof = tree.get_subtree_proof_nodes(
        (False, False, False, False, False), (False, False, False, False, True),
    )
    assert len(subtree_proof) == 1
    assert subtree_proof[0].path == (False, False, False, False)
