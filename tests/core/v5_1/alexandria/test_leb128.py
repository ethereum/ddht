import io

from hypothesis import given
from hypothesis import strategies as st
import pytest

from ddht.v5_1.alexandria.leb128 import encode_leb128, parse_leb128


@pytest.mark.parametrize(
    "value,expected", ((b"\x00", 0), (b"\xe5\x8e\x26", 624485),),
)
def test_parse_leb128(value, expected):
    actual = parse_leb128(io.BytesIO(value))
    assert actual == expected


@pytest.mark.parametrize(
    "value,expected", ((0, b"\x00"), (624485, b"\xe5\x8e\x26"),),
)
def test_encode_leb128(value, expected):
    actual = encode_leb128(value)
    assert actual == expected


@given(value=st.integers(min_value=0, max_value=2 ** 32 - 1))
def test_leb128_encoding_round_trip(value):
    encoded_value = encode_leb128(value)
    decoded_value = parse_leb128(io.BytesIO(encoded_value))
    assert decoded_value == value
