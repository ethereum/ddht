from hypothesis import example, given, settings
from hypothesis import strategies as st
import pytest
from ssz import get_hash_tree_root
from ssz.hash import hash_eth2

from ddht.v5_1.alexandria.constants import GB
from ddht.v5_1.alexandria.partials.proof import (
    Proof,
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
    assert proof.get_hash_tree_root() == expected_hash_tree_root

    proven_data_segments = proof.get_proven_data_segments()

    assert len(proven_data_segments) == 1
    start_index, proven_data = proven_data_segments[0]
    assert start_index == 0
    assert proven_data == data

    proven_data = proof.get_proven_data()
    assert proven_data[0 : len(data)] == data


short_content_sedes = ByteList(max_length=32 * 16)

CONTENT_12345 = b"\x01" * 32 + b"\x02" * 32 + b"\x03" * 32 + b"\x04" * 32 + b"\x05" * 32
CONTENT_512 = bytes((i % 256 for i in range(512)))
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
        # full content
        (CONTENT_512, slice(0, 128)),
        (CONTENT_512, slice(128, 256)),
        (CONTENT_512, slice(0, 512)),
        (CONTENT_512, slice(128, 512)),
        (CONTENT_512, slice(256, 512)),
        (CONTENT_512, slice(500, 512)),
    ),
)
def test_ssz_partial_proof_construction(content, data_slice):
    full_proof = compute_proof(content, sedes=short_content_sedes)

    slice_length = data_slice.stop - data_slice.start

    partial_proof = full_proof.to_partial(
        start_at=data_slice.start, partial_data_length=slice_length,
    )
    assert partial_proof.get_hash_tree_root() == full_proof.get_hash_tree_root()

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
    assert partial_proof.get_hash_tree_root() == full_proof.get_hash_tree_root()

    validate_proof(partial_proof)
    assert is_proof_valid(partial_proof)

    partial = partial_proof.get_proven_data()
    data_from_partial = partial[data_slice]
    assert data_from_partial == content[data_slice]


def test_proof_get_element_api():
    content = bytes((i for i in range(160)))

    proof = compute_proof(content, sedes=short_content_sedes)
    # paths in this proof:
    #
    # 0: 00000
    # 1: 00001
    # 2: 00010
    # 3: 00011
    # 4: 00100
    # 5: 00101
    # 6: 0011
    # 7: 01
    # 8: 1
    path_0 = (False, False, False, False, False)
    path_1 = (False, False, False, False, True)
    path_2 = (False, False, False, True, False)
    path_3 = (False, False, False, True, True)
    path_4 = (False, False, True, False, False)
    path_5 = (False, False, True, False, True)
    path_6 = (False, False, True, True)
    path_7 = (False, True)
    path_8 = (True,)

    el_0 = proof.get_element(path_0)
    el_1 = proof.get_element(path_1)
    el_2 = proof.get_element(path_2)
    el_3 = proof.get_element(path_3)
    el_4 = proof.get_element(path_4)
    el_5 = proof.get_element(path_5)
    el_6 = proof.get_element(path_6)
    el_7 = proof.get_element(path_7)
    el_8 = proof.get_element(path_8)

    assert el_0.path == path_0
    assert el_1.path == path_1
    assert el_2.path == path_2
    assert el_3.path == path_3
    assert el_4.path == path_4
    assert el_5.path == path_5
    assert el_6.path == path_6
    assert el_7.path == path_7
    assert el_8.path == path_8

    with pytest.raises(IndexError):
        proof.get_element((False,))
    with pytest.raises(IndexError):
        proof.get_element((False, False))
    with pytest.raises(IndexError):
        proof.get_element((False, True, False))
    with pytest.raises(IndexError):
        proof.get_element((False, True, True))
    with pytest.raises(IndexError):
        proof.get_element((True, False))


@given(content_length=st.integers(min_value=1, max_value=512),)
def test_proof_get_content_length_api(content_length):
    content = bytes((i % 256 for i in range(content_length)))

    proof = compute_proof(content, sedes=short_content_sedes)
    assert proof.get_content_length() == content_length


@pytest.mark.parametrize(
    "content,expected",
    (
        (b"\x00", 0),
        (b"\x00" * 32, 0),
        (b"\x00" * 33, 1),
        (b"\x00" * 64, 1),
        (b"\x00" * 65, 2),
        (b"\x00" * 96, 2),
        (b"\x00" * 97, 3),
        (b"\x00" * 128, 3),
        (b"\x00" * 129, 4),
    ),
)
def test_proof_get_last_data_chunk_index_api(content, expected):
    proof = compute_proof(content, sedes=short_content_sedes)
    actual = proof.get_last_data_chunk_index()
    assert actual == expected


def p(*bits):
    return tuple(bool(bit) for bit in bits)


@pytest.mark.parametrize(
    "content,expected",
    (
        (b"\x00", p(0, 0, 0, 0, 0)),
        (b"\x00" * 32, p(0, 0, 0, 0, 0)),
        (b"\x00" * 33, p(0, 0, 0, 0, 1)),
        (b"\x00" * 64, p(0, 0, 0, 0, 1)),
        (b"\x00" * 65, p(0, 0, 0, 1, 0)),
        (b"\x00" * 96, p(0, 0, 0, 1, 0)),
        (b"\x00" * 97, p(0, 0, 0, 1, 1)),
        (b"\x00" * 128, p(0, 0, 0, 1, 1)),
        (b"\x00" * 129, p(0, 0, 1, 0, 0)),
        (b"\x00" * 512, p(0, 1, 1, 1, 1)),
    ),
)
def test_proof_get_last_data_chunk_path_api(content, expected):
    proof = compute_proof(content, sedes=short_content_sedes)
    actual = proof.get_last_data_chunk_path()
    assert actual == expected


@pytest.mark.parametrize(
    "content,expected",
    (
        (b"\x00", (p(0, 0, 0, 0, 0),)),
        (b"\x00" * 32, (p(0, 0, 0, 0, 0),)),
        (b"\x00" * 33, (p(0, 0, 0, 0, 0), p(0, 0, 0, 0, 1))),
        (b"\x00" * 64, (p(0, 0, 0, 0, 0), p(0, 0, 0, 0, 1))),
        (b"\x00" * 65, (p(0, 0, 0, 0, 0), p(0, 0, 0, 0, 1), p(0, 0, 0, 1, 0))),
    ),
)
def test_full_proof_get_data_elements_api(content, expected):
    proof = compute_proof(content, sedes=short_content_sedes)
    elements = proof.get_data_elements()
    paths = tuple(el.path for el in elements)
    assert paths == expected


@pytest.mark.parametrize(
    "content,expected",
    (
        (b"\x00", 1),
        (b"\x00" * 32, 1),
        (b"\x00" * 33, 2),
        (b"\x00" * 64, 2),
        (b"\x00" * 65, 3),
        (b"\x00" * 96, 3),
        (b"\x00" * 97, 4),
        (b"\x00" * 128, 4),
        (b"\x00" * 129, 5),
    ),
)
def test_proof_get_first_padding_chunk_index_api(content, expected):
    proof = compute_proof(content, sedes=short_content_sedes)
    actual = proof.get_first_padding_chunk_index()
    assert actual == expected


def test_proof_get_first_padding_chunk_index_full_tree():
    content = bytes((i % 256 for i in range(512)))
    proof = compute_proof(content, sedes=short_content_sedes)
    with pytest.raises(IndexError):
        proof.get_first_padding_chunk_index()


@pytest.mark.parametrize(
    "content,expected",
    (
        (b"\x00", p(0, 0, 0, 0, 1)),
        (b"\x00" * 32, p(0, 0, 0, 0, 1)),
        (b"\x00" * 33, p(0, 0, 0, 1, 0)),
        (b"\x00" * 64, p(0, 0, 0, 1, 0)),
        (b"\x00" * 65, p(0, 0, 0, 1, 1)),
        (b"\x00" * 96, p(0, 0, 0, 1, 1)),
        (b"\x00" * 97, p(0, 0, 1, 0, 0)),
        (b"\x00" * 128, p(0, 0, 1, 0, 0)),
        (b"\x00" * 129, p(0, 0, 1, 0, 1)),
    ),
)
def test_proof_get_first_padding_chunk_path_api(content, expected):
    proof = compute_proof(content, sedes=short_content_sedes)
    actual = proof.get_first_padding_chunk_path()
    assert actual == expected


def test_proof_get_first_padding_chunk_path_full_proof():
    content = bytes((i % 256 for i in range(512)))
    proof = compute_proof(content, sedes=short_content_sedes)
    with pytest.raises(IndexError):
        proof.get_first_padding_chunk_path()


@pytest.mark.parametrize(
    "content,expected",
    (
        (b"\x00", (p(0, 0, 0, 0, 1), p(0, 0, 0, 1), p(0, 0, 1), p(0, 1))),
        (b"\x00" * 32, (p(0, 0, 0, 0, 1), p(0, 0, 0, 1), p(0, 0, 1), p(0, 1))),
        (b"\x00" * 33, (p(0, 0, 0, 1), p(0, 0, 1), p(0, 1))),
        (b"\x00" * 64, (p(0, 0, 0, 1), p(0, 0, 1), p(0, 1))),
        (b"\x00" * 65, (p(0, 0, 0, 1, 1), p(0, 0, 1), p(0, 1))),
        (b"\x00" * 96, (p(0, 0, 0, 1, 1), p(0, 0, 1), p(0, 1))),
        (b"\x00" * 97, (p(0, 0, 1), p(0, 1))),
        (b"\x00" * 128, (p(0, 0, 1), p(0, 1))),
        (b"\x00" * 129, (p(0, 0, 1, 0, 1), p(0, 0, 1, 1), p(0, 1))),
        (b"\x00" * 256, (p(0, 1),)),
        (b"\x00" * 512, ()),
    ),
)
def test_full_proof_get_padding_elements_api(content, expected):
    proof = compute_proof(content, sedes=short_content_sedes)
    elements = proof.get_padding_elements()
    paths = tuple(el.path for el in elements)
    assert paths == expected


def test_proof_get_elements_api():
    content = bytes((i for i in range(160)))

    proof = compute_proof(content, sedes=short_content_sedes)
    # paths in this proof:
    #
    # 0: 00000
    # 1: 00001
    # 2: 00010
    # 3: 00011
    # 4: 00100
    # 5: 00101
    # 6: 0011
    # 7: 01
    # 8: 1
    path_0 = (False, False, False, False, False)
    path_1 = (False, False, False, False, True)
    path_2 = (False, False, False, True, False)
    path_3 = (False, False, False, True, True)
    path_4 = (False, False, True, False, False)
    path_5 = (False, False, True, False, True)
    path_6 = (False, False, True, True)
    path_7 = (False, True)
    path_8 = (True,)

    paths = (
        path_0,
        path_1,
        path_2,
        path_3,
        path_4,
        path_5,
        path_6,
        path_7,
        path_8,
    )

    def to_paths(elements):
        return tuple(el.path for el in elements)

    between_all = proof.get_elements()
    assert to_paths(between_all) == paths

    between_none_00000 = proof.get_elements(right=path_0)
    assert to_paths(between_none_00000) == ()

    between_none_00000_inclusive = proof.get_elements(
        right=path_0, right_inclusive=True
    )
    assert to_paths(between_none_00000_inclusive) == (path_0,)

    between_00000_00000 = proof.get_elements(left=path_0, right=path_0)
    assert to_paths(between_00000_00000) == ()

    between_00000_00000_inclusive = proof.get_elements(
        left=path_0, right=path_0, right_inclusive=True,
    )
    assert to_paths(between_00000_00000_inclusive) == (path_0,)

    between_none_00001 = proof.get_elements(right=path_1)
    assert to_paths(between_none_00001) == (path_0,)

    between_none_00001_inclusive = proof.get_elements(
        right=path_1, right_inclusive=True
    )
    assert to_paths(between_none_00001_inclusive) == (path_0, path_1)

    between_0_00001 = proof.get_elements(left=(False,), right=path_1)
    assert to_paths(between_0_00001) == (path_0,)

    between_0_00001_inclusive = proof.get_elements(
        left=(False,), right=path_1, right_inclusive=True,
    )
    assert to_paths(between_0_00001_inclusive) == (path_0, path_1)

    between_00000_00001 = proof.get_elements(left=path_0, right=path_1)
    assert to_paths(between_00000_00001) == (path_0,)

    between_00000_00001_inclusive = proof.get_elements(
        left=path_0, right=path_1, right_inclusive=True,
    )
    assert to_paths(between_00000_00001_inclusive) == (path_0, path_1)

    between_00001_00001 = proof.get_elements(left=path_1, right=path_1)
    assert to_paths(between_00001_00001) == ()

    between_00001_00001 = proof.get_elements(
        left=path_1, right=path_1, right_inclusive=True,
    )
    assert to_paths(between_00001_00001) == (path_1,)

    between_00000_00002 = proof.get_elements(left=path_0, right=path_2)
    assert to_paths(between_00000_00002) == (path_0, path_1)

    between_00001_00002 = proof.get_elements(left=path_1, right=path_2)
    assert to_paths(between_00001_00002) == (path_1,)

    between_0000_0001_inclusive = proof.get_elements(
        left=p(0, 0, 0, 0), right=p(0, 0, 0, 1)
    )
    assert to_paths(between_0000_0001_inclusive) == (path_0, path_1)

    between_0000_0010_inclusive = proof.get_elements(
        left=p(0, 0, 0, 0), right=p(0, 0, 1, 0)
    )
    assert to_paths(between_0000_0010_inclusive) == (path_0, path_1, path_2, path_3)

    between_0001_none = proof.get_elements(left=p(0, 0, 0, 1))
    assert to_paths(between_0001_none) == paths[2:]

    between_0001_1 = proof.get_elements(left=p(0, 0, 0, 1), right=p(1))
    assert to_paths(between_0001_1) == paths[2:-1]


def test_proof_get_elements_under_api():
    content = bytes((i for i in range(160)))

    proof = compute_proof(content, sedes=short_content_sedes)
    # paths in this proof:
    #
    # 0: 00000
    # 1: 00001
    # 2: 00010
    # 3: 00011
    # 4: 00100
    # 5: 00101
    # 6: 0011
    # 7: 01
    # 8: 1
    path_0 = (False, False, False, False, False)
    path_1 = (False, False, False, False, True)
    path_2 = (False, False, False, True, False)
    path_3 = (False, False, False, True, True)
    path_4 = (False, False, True, False, False)
    path_5 = (False, False, True, False, True)
    path_6 = (False, False, True, True)
    path_7 = (False, True)
    path_8 = (True,)

    def to_paths(elements):
        return tuple(el.path for el in elements)

    under_0000 = proof.get_elements_under((False, False, False, False))
    assert to_paths(under_0000) == (path_0, path_1)

    under_000 = proof.get_elements_under((False, False, False))
    assert to_paths(under_000) == (path_0, path_1, path_2, path_3)

    under_00 = proof.get_elements_under((False, False))
    assert to_paths(under_00) == (
        path_0,
        path_1,
        path_2,
        path_3,
        path_4,
        path_5,
        path_6,
    )

    under_0001 = proof.get_elements_under((False, False, False, True))
    assert to_paths(under_0001) == (path_2, path_3)

    under_001 = proof.get_elements_under((False, False, True))
    assert to_paths(under_001) == (path_4, path_5, path_6)

    under_01 = proof.get_elements_under((False, True))
    assert to_paths(under_01) == (path_7,)

    under_1 = proof.get_elements_under((True,))
    assert to_paths(under_1) == (path_8,)


@pytest.fixture
def short_proof():
    data = b"\x01" * 32 + b"\x02" * 32 + b"\x03" * 32 + b"\x04" * 32 + b"\x05" * 32
    proof = compute_proof(data, sedes=short_content_sedes)
    return proof


def test_proof_get_minimal_proof_elements_all_leaves():
    r"""
    0:                                X
                                     / \
                                   /     \
    1:                            0       1
                                 / \   (length)
                               /     \
                             /         \
                           /             \
                         /                 \
                       /                     \
                     /                         \
    2:              0                           1
                  /   \                       /   \
                /       \                   /       \
              /           \               /           \
    3:       0             1             0             1
            / \           / \           / \           / \
           /   \         /   \         /   \         /   \
    4:    0     1       0     1       0     1       0     1
         / \   / \     / \   / \     / \   / \     / \   / \
    5:  0   1 0   1   0   1 0   1   0   1 0   1   0   1 0   1

    Tests:
    A: |-|
    B:     |-|
    C:                         |-|
    D: |<--->|
    E:               |<--->|
    F:     |<->|
    G: |<----->|
    H:     |<----->|
    I:     |<----------------------->|
    """
    proof = compute_proof(CONTENT_512, sedes=short_content_sedes)

    # Just a single node
    elements_a = proof.get_minimal_proof_elements(0, 1)
    assert len(elements_a) == 1
    assert elements_a[0].path == p(0, 0, 0, 0, 0)
    assert elements_a[0].value == CONTENT_512[:32]

    elements_b = proof.get_minimal_proof_elements(1, 1)
    assert len(elements_b) == 1
    assert elements_b[0].path == p(0, 0, 0, 0, 1)
    assert elements_b[0].value == CONTENT_512[32:64]

    elements_c = proof.get_minimal_proof_elements(7, 1)
    assert len(elements_c) == 1
    assert elements_c[0].path == p(0, 0, 1, 1, 1)
    assert elements_c[0].value == CONTENT_512[224:256]

    # pair of sibling nodes
    elements_d = proof.get_minimal_proof_elements(0, 2)
    assert len(elements_d) == 1
    assert elements_d[0].path == p(0, 0, 0, 0)
    assert elements_d[0].value == hash_eth2(CONTENT_512[0:64])

    elements_e = proof.get_minimal_proof_elements(4, 2)
    assert len(elements_e) == 1
    assert elements_e[0].path == p(0, 0, 1, 0)
    assert elements_e[0].value == hash_eth2(CONTENT_512[128:192])

    # pair of non-sibling nodes
    elements_f = proof.get_minimal_proof_elements(1, 2)
    assert len(elements_f) == 2
    assert elements_f[0].path == p(0, 0, 0, 0, 1)
    assert elements_f[0].value == CONTENT_512[32:64]
    assert elements_f[1].path == p(0, 0, 0, 1, 0)
    assert elements_f[1].value == CONTENT_512[64:96]

    # disjoint sets of three
    elements_g = proof.get_minimal_proof_elements(0, 3)
    assert len(elements_g) == 2
    assert elements_g[0].path == p(0, 0, 0, 0)
    assert elements_g[0].value == hash_eth2(CONTENT_512[0:64])
    assert elements_g[1].path == p(0, 0, 0, 1, 0)
    assert elements_g[1].value == CONTENT_512[64:96]

    elements_h = proof.get_minimal_proof_elements(1, 3)
    assert len(elements_h) == 2
    assert elements_h[0].path == p(0, 0, 0, 0, 1)
    assert elements_h[0].value == CONTENT_512[32:64]
    assert elements_h[1].path == p(0, 0, 0, 1)
    assert elements_h[1].value == hash_eth2(CONTENT_512[64:128])

    # span of 8
    elements_i = proof.get_minimal_proof_elements(1, 8)
    assert len(elements_i) == 4
    assert elements_i[0].path == p(0, 0, 0, 0, 1)
    assert elements_i[0].value == CONTENT_512[32:64]
    assert elements_i[1].path == p(0, 0, 0, 1)
    assert elements_i[1].value == hash_eth2(CONTENT_512[64:128])
    assert elements_i[2].path == p(0, 0, 1)
    assert elements_i[2].value == hash_eth2(
        hash_eth2(CONTENT_512[128:192]) + hash_eth2(CONTENT_512[192:256])
    )
    assert elements_i[3].path == p(0, 1, 0, 0, 0)
    assert elements_i[3].value == CONTENT_512[256:288]


def test_proof_get_minimal_proof_elements_mixed_depths():
    r"""
    0:                                X
                                     / \
                                   /     \
    1:                            0       1
                                 / \   (length)
                               /     \
                             /         \
                           /             \
                         /                 \
                       /                     \
                     /                         \
    2:              0                           1
                  /   \                       /   \
                /       \                   /       \
              /           \               /           \
    3:       0             1             0             1
            / \           / \           / \           / \
           /   \         /   \         /   \         /   \
    4:    0     1       0     1       X     X       0     1
         / \   / \     / \   / \     / \   / \     / \   / \
    5:  0   1 X   X   0   1 0   1   X   X X   X   0   1 X   X

    Nodes marked with `X` have already been collapsed

    Tests:
    A: |<--------->|
    B:                             |<----------------------->|
    """
    full_proof = compute_proof(CONTENT_512, sedes=short_content_sedes)

    section_a = full_proof.get_elements_under(p(0, 0, 0, 0))
    section_b = full_proof.get_minimal_proof_elements(2, 2)
    section_c = full_proof.get_elements_under(p(0, 0, 1))
    section_d = full_proof.get_minimal_proof_elements(8, 4)
    section_e = full_proof.get_elements_under(p(0, 1, 1, 0))
    section_f = full_proof.get_minimal_proof_elements(14, 2)
    section_g = (full_proof.get_element((True,)),)

    sparse_elements = sum(
        (section_a, section_b, section_c, section_d, section_e, section_f, section_g,),
        (),
    )
    sparse_proof = Proof(elements=sparse_elements, sedes=short_content_sedes,)
    validate_proof(sparse_proof)

    # spans nodes at different levels
    elements_a = sparse_proof.get_minimal_proof_elements(0, 4)
    assert len(elements_a) == 1
    assert elements_a[0].path == p(0, 0, 0)
    assert elements_a[0].value == hash_eth2(
        hash_eth2(CONTENT_512[0:64]) + hash_eth2(CONTENT_512[64:128])
    )

    elements_b = sparse_proof.get_minimal_proof_elements(8, 8)
    assert len(elements_b) == 1
    assert elements_b[0].path == p(0, 1)
    hash_010 = hash_eth2(
        hash_eth2(CONTENT_512[256:320]) + hash_eth2(CONTENT_512[320:384])
    )
    hash_011 = hash_eth2(
        hash_eth2(CONTENT_512[384:448]) + hash_eth2(CONTENT_512[448:512])
    )
    assert elements_b[0].value == hash_eth2(hash_010 + hash_011)
