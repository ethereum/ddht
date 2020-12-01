from hypothesis import given
from hypothesis import strategies as st
import pytest

from ddht.tools.factories.content import ContentFactory
from ddht.v5_1.alexandria.partials.proof import (
    Proof,
    compute_proof,
    deserialize_path,
    serialize_path,
    validate_proof,
)
from ddht.v5_1.alexandria.sedes import content_sedes

VALUE = bytes(bytearray((i for i in range(32))))


def p(*crumbs):
    return tuple(bool(crumb) for crumb in crumbs)


@pytest.mark.parametrize(
    "path,previous",
    (
        ((), (),),
        (p(1), (),),
        (p(0), (),),
        ((True,) * 25, (),),
        ((False,) * 25, (),),
        (p(0, 0, 1, 1, 0, 0), (),),
        (p(1, 1, 0, 0, 1, 1), (),),
        # Previous path without any common bits
        (p(1, 0, 1, 0, 1, 0), p(0, 1)),
        # Previous path without one common bit
        (p(1, 0, 1, 0, 1, 0), p(1, 1)),
        # Previous path without multiple common bits
        (p(1, 0, 1, 0, 1, 0), p(1, 0, 1, 0, 0),),
        # Exceed single byte boundaries
        (p(1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0), p(1, 0),),
        (p(1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0), p(1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1),),
    ),
)
def test_proof_path_serialization_round_trip(path, previous):
    serialized = serialize_path(previous, path)
    result = deserialize_path(previous, serialized)
    assert result == path


@given(data=st.data(),)
def test_proof_path_serialization_round_trip_fuzzy(data):
    path = data.draw(st.lists(st.booleans(), min_size=0, max_size=25).map(tuple))
    previous = data.draw(st.lists(st.booleans(), min_size=0, max_size=25).map(tuple))

    serialized = serialize_path(previous, path)
    result = deserialize_path(previous, serialized)
    assert result == path


MB = 1024 * 1024


@given(content=st.binary(min_size=0, max_size=MB))
def test_full_proof_serialization_and_deserialization(content):
    proof = compute_proof(content, sedes=content_sedes)

    serialized = proof.serialize()
    result = Proof.deserialize(serialized)

    assert result.get_hash_tree_root() == proof.get_hash_tree_root()

    validate_proof(result)

    assert result == proof


@given(data=st.data())
def test_partial_proof_serialization_and_deserialization(data):
    content = data.draw(
        st.one_of(
            st.binary(min_size=1, max_size=1024),
            st.integers(min_value=1, max_value=256).map(
                lambda v: ContentFactory(v * 128)
            ),
        )
    )

    slice_start = data.draw(st.integers(min_value=0, max_value=len(content) - 1))
    slice_stop = data.draw(st.integers(min_value=slice_start, max_value=len(content)))
    data_slice = slice(slice_start, slice_stop)

    full_proof = compute_proof(content, sedes=content_sedes)

    slice_length = max(0, data_slice.stop - data_slice.start)

    partial_proof = full_proof.to_partial(
        start_at=data_slice.start, partial_data_length=slice_length,
    )
    assert partial_proof.get_hash_tree_root() == full_proof.get_hash_tree_root()

    serialized = partial_proof.serialize()
    result = Proof.deserialize(serialized)

    validate_proof(result)

    assert result == partial_proof

    partial = result.get_proven_data()
    data_from_partial = partial[data_slice]
    assert data_from_partial == content[data_slice]
