from hypothesis import given
from hypothesis import strategies as st
import pytest

from ddht.v5_1.alexandria.partials.proof import compute_proof
from ddht.v5_1.alexandria.sedes import ByteList

short_content_sedes = ByteList(max_length=32 * 16)


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
