import pytest

from ddht.v5_1.alexandria.partials._utils import (
    decompose_into_powers_of_two,
    display_path,
    get_chunk_count_for_data_length,
    get_longest_common_path,
    merge_paths,
)


def p(*crumbs):
    return tuple(bool(crumb) for crumb in crumbs)


@pytest.mark.parametrize(
    "paths,expected",
    (
        ((), (),),  # no paths
        ((p(0, 1, 0),), p(0, 1, 0),),  # single path
        (((),), (),),  # single empty path
        (((), ()), (),),  # all empty paths
        ((p(1, 1, 1), p(0, 0, 0)), (),),  # no common crumbs
        ((p(0, 1, 1), p(0, 0, 0)), p(0,),),  # single crumb in common
        ((p(0, 0, 1), p(0, 0, 0)), p(0, 0),),  # multiple crumbs in common
        ((p(0, 0, 0), p(0, 0, 0)), p(0, 0, 0),),  # all crumbs in common
    ),
)
def test_get_longest_common_path(paths, expected):
    common_path = get_longest_common_path(*paths)
    assert common_path == expected


@pytest.mark.parametrize(
    "value,expected",
    (
        (1, (1,)),
        (2, (2,)),
        (3, (1, 2)),
        (4, (4,)),
        (5, (1, 4)),
        (6, (2, 4)),
        (7, (1, 2, 4)),
        (8, (8,)),
        (9, (1, 8)),
        (31, (1, 2, 4, 8, 16)),
        (33, (1, 32)),
    ),
)
def test_decompose_into_powers_of_two(value, expected):
    actual = decompose_into_powers_of_two(value)
    assert actual == expected
    assert sum(actual) == value


@pytest.mark.parametrize(
    "length,expected", ((0, 0), (1, 1), (31, 1), (32, 1), (33, 2),),
)
def test_get_chunk_count_for_data_length(length, expected):
    actual = get_chunk_count_for_data_length(length)
    assert actual == expected


@pytest.mark.parametrize(
    "path,expected",
    (
        (p(), ""),
        (p(0), "0"),
        (p(1), "1"),
        (p(0, 1), "01"),
        (p(1, 0), "10"),
        (p(1, 0, 1, 0, 1, 0, 1), "1010101"),
        (p(0, 1, 0, 1, 0, 1, 0), "0101010"),
    ),
)
def test_display_path(path, expected):
    actual = display_path(path)
    assert actual == expected


@pytest.mark.parametrize(
    "paths,expected",
    (
        ((p(0, 0, 1), p(0, 0, 0), p(0, 0)), (p(0, 0, 0), p(0, 0, 1)),),
        ((p(0, 0, 1), p(0,), p(0, 0, 0), p(0, 0)), (p(0, 0, 0), p(0, 0, 1)),),
    ),
)
def test_merge_paths(paths, expected):
    actual = merge_paths(*paths)
    assert actual == expected
