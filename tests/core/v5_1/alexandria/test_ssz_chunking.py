from hypothesis import example, given, settings
from hypothesis import strategies as st
import pytest
from ssz.constants import CHUNK_SIZE

from ddht.v5_1.alexandria.constants import GB
from ddht.v5_1.alexandria.partials._utils import get_chunk_count_for_data_length
from ddht.v5_1.alexandria.partials.chunking import (
    chunk_index_to_path,
    compute_chunks,
    group_by_subtree,
    path_to_left_chunk_index,
)


@settings(max_examples=1000)
@example(content=b"\x00" * 31)
@example(content=b"\x00" * 32)
@example(content=b"\x00" * 33)
@example(content=b"\x01" * 31)
@example(content=b"\x01" * 32)
@example(content=b"\x01" * 33)
@example(
    content=b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00"  # noqa: E501
)
@given(content=st.binary(min_size=0, max_size=GB))
def test_ssz_compute_chunks(content):
    # TODO: clear up this discrepancy.  When the content is the empty
    # bytestring, we actually have zero chunks, but most of the code treats it
    # as if there is always one chunk, even though there technically is only
    # padding in the tree.
    if len(content):
        expected_chunk_count = get_chunk_count_for_data_length(len(content))
    else:
        expected_chunk_count = 1

    chunks = compute_chunks(content)

    assert len(chunks) == expected_chunk_count

    for chunk_index, chunk in enumerate(chunks):
        start_at = chunk_index * CHUNK_SIZE
        end_at = min(len(content), start_at + CHUNK_SIZE)

        assert chunk == content[start_at:end_at].ljust(CHUNK_SIZE, b"\x00")

        if chunk_index == expected_chunk_count - 1:
            padding_start_idx = len(content) % CHUNK_SIZE
            if padding_start_idx:
                padding = chunk[padding_start_idx:]
                assert all(tuple(byte == 0 for byte in padding))


def p(*crumbs):
    return tuple(bool(crumb) for crumb in crumbs)


@pytest.mark.parametrize(
    "chunk_index,expected",
    ((0, p(0, 0, 0, 0)), (1, p(0, 0, 0, 1)), (3, p(0, 0, 1, 1)), (15, p(1, 1, 1, 1)),),
)
def test_chunk_index_to_path(chunk_index, expected):
    path = chunk_index_to_path(chunk_index, 4)
    assert path == expected


@pytest.mark.parametrize(
    "path,expected",
    (
        (p(0, 0, 0, 0), 0),
        (p(0, 0, 0), 0),
        (p(0, 0), 0),
        (p(0,), 0),
        (p(0, 0, 0, 1), 1),
        (p(0, 0, 1, 1), 3),
        (p(0, 0, 1), 2),
        (p(1, 1, 1, 1), 15),
        (p(1, 1, 1), 14),
        (p(1, 1), 12),
        (p(1,), 8),
    ),
)
def test_path_to_left_chunk_index(path, expected):
    chunk_index = path_to_left_chunk_index(path, 4)
    assert chunk_index == expected


@pytest.mark.parametrize(
    "first_chunk_index,num_chunks,expected",
    (
        # Tree for reference
        #
        # 0:                            0
        #                              / \
        #                            /     \
        #                          /         \
        #                        /             \
        #                      /                 \
        #                    /                     \
        #                  /                         \
        # 1:              0                           1
        #               /   \                       /   \
        #             /       \                   /       \
        #           /           \               /           \
        # 2:       0             1             0             1
        #        /   \         /   \         /   \         /   \
        # 3:    0     1       0     1       0     1       0     1
        #      / \   / \     / \   / \     / \   / \     / \   / \
        # 4:  A   B C   D   E   F G   H   I   J K   L   M   N O   P
        #
        # Indices:
        #     0   1 2   3   4   5 6   7   8   9 1   1   1   1 1   1
        #                                       0   1   2   3 4   5
        (0, 1, ((0,),)),
        (1, 1, ((1,),)),
        (2, 1, ((2,),)),
        (3, 1, ((3,),)),
        (4, 1, ((4,),)),
        (5, 1, ((5,),)),
        (6, 1, ((6,),)),
        (7, 1, ((7,),)),
        (8, 1, ((8,),)),
        (9, 1, ((9,),)),
        (9, 4, ((9,), (10, 11), (12,))),
        (3, 8, ((3,), (4, 5, 6, 7), (8, 9), (10,),),),
    ),
)
def test_groub_by_subtree(first_chunk_index, num_chunks, expected):
    actual = group_by_subtree(first_chunk_index, num_chunks)
    assert actual == expected
