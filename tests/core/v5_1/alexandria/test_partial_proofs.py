from hypothesis import example, given, settings
from hypothesis import strategies as st
import pytest
from ssz import get_hash_tree_root

from ddht.v5_1.alexandria.constants import GB
from ddht.v5_1.alexandria.partials.proof import (
    compute_proof,
    is_proof_valid,
    validate_proof,
)
from ddht.v5_1.alexandria.sedes import ByteList, content_sedes


@settings(max_examples=1000)
@given(data=st.binary(min_size=0, max_size=GB))
@example(data=b"")
@example(data=b"\x00" * 31)
@example(data=b"\x00" * 32)
@example(data=b"\x00" * 33)
@example(data=b"\x00" * 63)
@example(data=b"\x00" * 64)
@example(data=b"\x00" * 65)
def test_ssz_full_proofs(data):
    expected_hash_tree_root = get_hash_tree_root(data, sedes=content_sedes)
    proof = compute_proof(data, sedes=content_sedes)

    validate_proof(proof)
    assert is_proof_valid(proof)
    assert proof.hash_tree_root == expected_hash_tree_root

    proven_data_segments = proof.get_proven_data_segments(len(data))

    assert len(proven_data_segments) == 1
    start_index, proven_data = proven_data_segments[0]
    assert start_index == 0
    assert proven_data == data

    proven_data = proof.get_proven_data()
    assert proven_data[0 : len(data)] == data


short_content_sedes = ByteList(max_length=32 * 16)

CONTENT_12345 = b"\x01" * 32 + b"\x02" * 32 + b"\x03" * 32 + b"\x04" * 32 + b"\x05" * 32
r"""
                Tree for CONTENT_12345

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


@pytest.mark.parametrize(
    "content,data_slice",
    (
        # short cases
        (b"", slice(0, 0)),
        (b"\x00", slice(0, 0)),
        (b"\x00", slice(0, 1)),
        (b"\x01", slice(0, 0)),
        (b"\x01", slice(0, 1)),
        (b"\x00" * 32, slice(0, 0)),
        (b"\x00" * 32, slice(0, 1)),
        (b"\x00" * 32, slice(0, 32)),
        (b"\x00" * 33, slice(0, 0)),
        # longer content
        (CONTENT_12345, slice(0, 0)),
        (CONTENT_12345, slice(0, 32)),
        (CONTENT_12345, slice(0, 31)),
        (CONTENT_12345, slice(1, 32)),
        (CONTENT_12345, slice(1, 33)),
        (CONTENT_12345, slice(0, 64)),
        (CONTENT_12345, slice(32, 64)),
        (CONTENT_12345, slice(32, 65)),
        (CONTENT_12345, slice(31, 64)),
        (CONTENT_12345, slice(31, 65)),
        (CONTENT_12345, slice(64, 128)),
        (CONTENT_12345, slice(64, 129)),
        (CONTENT_12345, slice(63, 128)),
        (CONTENT_12345, slice(63, 129)),
        (CONTENT_12345, slice(0, 160)),
        (CONTENT_12345, slice(128, 160)),
        (CONTENT_12345, slice(127, 160)),
    ),
)
def test_ssz_partial_proof_construction(content, data_slice):
    full_proof = compute_proof(content, sedes=short_content_sedes)

    slice_length = data_slice.stop - data_slice.start

    partial_proof = full_proof.to_partial(
        start_at=data_slice.start, partial_data_length=slice_length,
    )
    assert partial_proof.hash_tree_root == full_proof.hash_tree_root

    validate_proof(partial_proof)
    assert is_proof_valid(partial_proof)

    partial = partial_proof.get_proven_data()
    data_from_partial = partial[data_slice]
    assert data_from_partial == content[data_slice]


@settings(max_examples=1000)
@given(data=st.data())
def test_ssz_partial_proof_fuzzy(data):
    content = data.draw(st.binary(min_size=0, max_size=GB))

    slice_start = data.draw(
        st.integers(min_value=0, max_value=max(0, len(content) - 1))
    )
    slice_stop = data.draw(st.integers(min_value=slice_start, max_value=len(content)))
    data_slice = slice(slice_start, slice_stop)

    full_proof = compute_proof(content, sedes=content_sedes)

    slice_length = max(0, data_slice.stop - data_slice.start - 1)

    partial_proof = full_proof.to_partial(
        start_at=data_slice.start, partial_data_length=slice_length,
    )
    assert partial_proof.hash_tree_root == full_proof.hash_tree_root

    validate_proof(partial_proof)
    assert is_proof_valid(partial_proof)

    partial = partial_proof.get_proven_data()
    data_from_partial = partial[data_slice]
    assert data_from_partial == content[data_slice]
