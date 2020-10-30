from hypothesis import given
from hypothesis import strategies as st
import pytest

from ddht.exceptions import ParseError
from ddht.v5_1.alexandria.leb128 import (
    decode_leb128,
    encode_leb128,
    parse_leb128,
    partition_leb128,
)


@pytest.mark.parametrize(
    "value,expected", ((b"\x00", 0), (b"\xe5\x8e\x26", 624485),),
)
def test_decode_leb128(value, expected):
    actual = decode_leb128(value)
    assert actual == expected


@pytest.mark.parametrize(
    "value,expected", ((b"\x00\x00", 0), (b"\xe5\x8e\x26\x00", 624485),),
)
def test_decode_leb128_dangling_bytes(value, expected):
    with pytest.raises(ParseError):
        decode_leb128(value)


@pytest.mark.parametrize(
    "value,expected", ((0, b"\x00"), (624485, b"\xe5\x8e\x26"),),
)
def test_encode_leb128(value, expected):
    actual = encode_leb128(value)
    assert actual == expected


@given(value=st.integers(min_value=0, max_value=2 ** 32 - 1))
def test_leb128_encoding_round_trip(value):
    encoded_value = encode_leb128(value)
    decoded_value = decode_leb128(encoded_value)
    assert decoded_value == value


@pytest.mark.parametrize(
    "encoded_stream,expected",
    (
        (b"", ()),
        (b"\x00", (b"\x00",)),
        (b"\x00\x00", (b"\x00", b"\x00")),
        (b"\xe5\x8e\x26", (b"\xe5\x8e\x26",)),
        (b"\xe5\x8e\x26\x00", (b"\xe5\x8e\x26", b"\x00")),
        (b"\x00\xe5\x8e\x26\x00", (b"\x00", b"\xe5\x8e\x26", b"\x00")),
    ),
)
def test_partition_leb128(encoded_stream, expected):
    actual = partition_leb128(encoded_stream)
    assert actual == expected


@pytest.mark.parametrize(
    "data,expected",
    (
        (b"\x00", (0, b"")),
        (b"\x00\x00", (0, b"\x00")),
        (b"\xe5\x8e\x26\x00", (624485, b"\x00")),
        (b"\xe5\x8e\x26\xff", (624485, b"\xff")),
    ),
)
def test_parse_leb128(data, expected):
    actual = parse_leb128(data)
    assert actual == expected


@given(
    value=st.integers(min_value=0, max_value=2 ** 32 - 1),
    remainder=st.binary(min_size=0, max_size=16),
)
def test_parse_leb128_fuzzy(value, remainder):
    data = encode_leb128(value) + remainder
    actual = parse_leb128(data)
    assert actual == (value, remainder)


@given(
    values=st.lists(
        st.integers(min_value=0, max_value=2 ** 32 - 1), min_size=0, max_size=50,
    ).map(tuple),
)
def test_partition_leb128_fuzzy(values):
    encoded = b"".join((encode_leb128(value) for value in values))
    encoded_values = partition_leb128(encoded)
    decoded_values = tuple(
        decode_leb128(encoded_value) for encoded_value in encoded_values
    )
    assert decoded_values == values
