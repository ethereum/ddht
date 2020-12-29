from hypothesis import given
from hypothesis import strategies as st
import pytest

from ddht.v5_1.alexandria.fog import (
    explore_path,
    Fog,
)
from ddht.v5_1.alexandria.partials._utils import filter_overlapping_paths


def p(*crumbs):
    return tuple(bool(crumb) for crumb in crumbs)


UNEXPLORED = ((),)
EXPLORED = ()


@pytest.mark.parametrize(
    'unexplored,to_explore,expected',
    (
        # Explore exact path
        (UNEXPLORED, (), ()),
        ((p(1),), p(1), ()),
        ((p(0),), p(0), ()),
        # Explore child path
        (UNEXPLORED, p(1), (p(0),)),
        (UNEXPLORED, p(0), (p(1),)),
        ((p(0),), p(0, 1), (p(0, 0),)),
        ((p(0),), p(0, 0), (p(0, 1),)),
        (UNEXPLORED, p(0, 0), (p(0, 1), p(1))),
        ((p(0),), p(0, 0, 0), (p(0, 0, 1), p(0, 1))),
        # Explore parent path
        ((p(0, 0, 0),), p(0, 0), EXPLORED),
        # No change
        ((p(0),), p(1), (p(0),)),
        ((p(1),), p(0), (p(1),)),
        ((p(0),), p(1, 0), (p(0),)),
        ((p(1),), p(0, 1), (p(1),)),
        # Already explored
        (EXPLORED, p(0), EXPLORED),
        (EXPLORED, p(1), EXPLORED),
    ),
)
def test_explore_path(unexplored, to_explore, expected):
    actual = explore_path(unexplored, to_explore)
    assert actual == expected


@given(data=st.data())
def test_explore_path_fuzzy(data):
    explored = set()
    unexplored = UNEXPLORED

    to_explore = data.draw(st.lists(
        st.lists(st.booleans(), min_size=0, max_size=4).map(tuple),
        min_size=1,
        max_size=31,
    ))

    for path in to_explore:
        unexplored = explore_path(unexplored, path)

        explored.add(path)

        # the unexplored list should be sorted and deduplicated
        assert len(unexplored) == len(filter_overlapping_paths(*set(unexplored)))
        assert tuple(sorted(unexplored)) == unexplored

        # None of the already explored paths should be in the unexplored paths
        assert not explored.intersection(unexplored)

        for explored_path in explored:
            for unexplored_path in unexplored:
                # None of the unexlpored paths should be a prefix of an
                # explored paths
                assert not explored_path[:len(unexplored_path)] == unexplored_path

                # none of the explored paths should be a prefix of an
                # unexplored_path
                assert not unexplored_path[:len(explored_path)] == explored_path


r"""


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
    4: 0   1 2   3   4   5 6   7   8   9 A   B   C   D E   F
"""


def test_fog_path_exploration():
    fog = Fog(4)

    assert not fog.is_path_explored(p(0, 1, 0))
    fog.explore_path(p(0, 1, 0))
    assert fog.is_path_explored(p(0, 1, 0))

    # sub-paths should be explored
    assert fog.is_path_explored(p(0, 1, 0, 1))
    assert fog.is_path_explored(p(0, 1, 0, 0))

    # sibling paths shouldn't
    assert not fog.is_path_explored(p(0, 1, 1))

    # parent paths shouldn't
    assert not fog.is_path_explored(p(0, 1))
    assert not fog.is_path_explored(p(0))


def test_fog_leaf_exploration():
    fog = Fog(4)

    assert not fog.is_leaf_explored(0)
    fog.explore_leaf(0)
    assert fog.is_leaf_explored(0)

    for leaf in range(1, 16):
        if leaf == 2:
            fog._throw = True
        assert not fog.is_leaf_explored(leaf)

    fog.explore_leaf(7)
    assert fog.is_leaf_explored(7)

    for leaf in range(1, 7):
        assert not fog.is_leaf_explored(leaf)
    for leaf in range(8, 16):
        assert not fog.is_leaf_explored(leaf)


@pytest.mark.parametrize(
    'from_leaf,num_leaves,should_be_explored',
    (
        (0, 1, (0,)),
        (0, 2, (0, 1)),
        (0, 3, (0, 1, 2)),
        (2, 1, (2,)),
    ),
)
def test_fog_explore_from(from_leaf, num_leaves, should_be_explored):
    fog = Fog(4)
    fog.explore_from(from_leaf, num_leaves)

    for leaf in should_be_explored:
        assert fog.is_leaf_explored(leaf)

    for leaf in set(range(16)).difference(should_be_explored):
        assert not fog.is_leaf_explored(leaf)


def test_fog_iter_unexplored():
    fog_a = Fog(4)
    fog_a.explore_leaf(7)

    unexplored = tuple(fog_a.iter_unexplored())
    assert len(unexplored) == 2
    left, right = unexplored
    assert left == slice(0, 7)
    assert right == slice(8, 16)
