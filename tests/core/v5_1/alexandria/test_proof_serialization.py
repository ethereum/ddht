import io

from hypothesis import given, settings
from hypothesis import strategies as st
import pytest

from ddht.v5_1.alexandria.constants import GB
from ddht.v5_1.alexandria.partials.proof import (
    Proof,
    ProofElement,
    compute_proof,
    validate_proof,
)
from ddht.v5_1.alexandria.sedes import content_sedes

VALUE = bytes(bytearray((i for i in range(32))))


def p(*crumbs):
    return tuple(bool(crumb) for crumb in crumbs)


@pytest.mark.parametrize(
    "path,value,previous",
    (
        ((), VALUE, None,),
        ((True,), VALUE, None,),
        ((False,), VALUE, None,),
        ((True,) * 25, VALUE, None,),
        ((False,) * 25, VALUE, None,),
        ((False, False, True, True, False, False), VALUE, None,),
        ((True, True, False, False, True, True), VALUE, None,),
        # Previous path without any common bits
        ((True, False, True, False, True, False), VALUE, (False, True),),
        # Previous path without one common bit
        ((True, False, True, False, True, False), VALUE, (True, True),),
        # Previous path without multiple common bits
        (
            (True, False, True, False, True, False),
            VALUE,
            (True, False, True, False, False),
        ),
        # Exceed single byte boundaries
        (
            (
                True,
                False,
                True,
                False,
                True,
                False,
                True,
                False,
                True,
                False,
                True,
                False,
            ),
            VALUE,
            (True, False),
        ),
        (
            (
                True,
                False,
                True,
                False,
                True,
                False,
                True,
                False,
                True,
                False,
                True,
                False,
            ),
            VALUE,
            (True, False, True, False, True, False, True, False, True, False),
        ),
    ),
)
def test_proof_element_serialization_round_trip(path, value, previous):
    path = tuple(path)
    element = ProofElement(path, value)
    serialized = element.serialize(previous)
    result = ProofElement.deserialize(io.BytesIO(serialized), previous)
    assert result == element


@given(data=st.data(),)
def test_proof_element_serialization_round_trip_fuzzy(data):
    path = data.draw(st.lists(st.booleans(), min_size=0, max_size=25).map(tuple))
    value = data.draw(st.binary(min_size=32, max_size=32))
    previous = data.draw(
        st.one_of(
            st.lists(st.booleans(), min_size=0, max_size=25 - len(path)).map(tuple),
            st.none(),
        )
    )

    element = ProofElement(path, value)
    serialized = element.serialize(previous)
    result = ProofElement.deserialize(io.BytesIO(serialized), previous)
    assert result == element


@settings(max_examples=1000)
@given(content=st.binary(min_size=0, max_size=GB))
def test_full_proof_serialization_and_deserialization(content):
    proof = compute_proof(content, sedes=content_sedes)

    serialized = proof.serialize()
    result = Proof.deserialize(io.BytesIO(serialized), proof.hash_tree_root)

    validate_proof(result)

    assert result == proof


@settings(max_examples=1000)
@given(data=st.data())
def test_partial_proof_serialization_and_deserialization(data):
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

    serialized = partial_proof.serialize()
    result = Proof.deserialize(io.BytesIO(serialized), partial_proof.hash_tree_root)

    validate_proof(result)

    assert result == partial_proof

    partial = result.get_proven_data()
    data_from_partial = partial[data_slice]
    assert data_from_partial == content[data_slice]
