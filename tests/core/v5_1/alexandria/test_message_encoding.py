from eth_enr.tools.factories import ENRFactory
from hypothesis import given
from hypothesis import strategies as st
import rlp

from ddht.v5_1.alexandria.messages import (
    FindNodesMessage,
    FoundNodesMessage,
    FindContentMessage,
    FoundContentMessage,
    PingMessage,
    PongMessage,
    decode_message,
)
from ddht.v5_1.alexandria.payloads import (
    FindNodesPayload,
    FoundNodesPayload,
    FindContentPayload,
    FoundContentPayload,
    PingPayload,
    PongPayload,
)


@given(
    enr_seq=st.integers(min_value=0, max_value=2 ** 32 - 1),
    advertisement_radius=st.integers(min_value=0, max_value=2 ** 256 - 1),
)
def test_ping_message_encoding_round_trip(enr_seq, advertisement_radius):
    payload = PingPayload(enr_seq=enr_seq, advertisement_radius=advertisement_radius)
    message = PingMessage(payload)
    encoded = message.to_wire_bytes()
    result = decode_message(encoded)
    assert result == message


@given(
    enr_seq=st.integers(min_value=0, max_value=2 ** 32 - 1),
    advertisement_radius=st.integers(min_value=0, max_value=2 ** 256 - 1),
)
def test_pong_message_encoding_round_trip(enr_seq, advertisement_radius):
    payload = PongPayload(enr_seq=enr_seq, advertisement_radius=advertisement_radius)
    message = PongMessage(payload)
    encoded = message.to_wire_bytes()
    result = decode_message(encoded)
    assert result == message


@given(
    distances=st.lists(
        st.integers(min_value=0, max_value=256), min_size=1, max_size=32, unique=True,
    ).map(tuple)
)
def test_find_nodes_message_encoding_round_trip(distances):
    payload = FindNodesPayload(distances)
    message = FindNodesMessage(payload)
    encoded = message.to_wire_bytes()
    result = decode_message(encoded)
    assert result == message


@given(num_enr_records=st.integers(min_value=0, max_value=5))
def test_found_nodes_message_encoding_round_trip(num_enr_records):
    enrs = tuple(ENRFactory() for _ in range(num_enr_records))
    encoded_enrs = tuple(rlp.encode(enr) for enr in enrs)
    payload = FoundNodesPayload(num_enr_records, encoded_enrs)
    message = FoundNodesMessage(payload)
    encoded = message.to_wire_bytes()
    result = decode_message(encoded)
    assert result.payload == message.payload


@given(
    content_key=st.binary(min_size=1, max_size=16),
)
def test_find_content_message_encoding_round_trip(content_key):
    payload = FindContentPayload(content_key)
    message = FindContentMessage(payload)
    encoded = message.to_wire_bytes()
    result = decode_message(encoded)
    assert result == message


@given(data=st.data())
def test_found_content_message_encoding_round_trip(data):
    is_content = data.draw(st.booleans())
    if is_content:
        content = data.draw(st.binary(min_size=32, max_size=32))
        enrs = ()
    else:
        num_enrs = data.draw(st.integers(min_value=0, max_value=3))
        enrs = tuple(ENRFactory() for _ in range(num_enrs))
        content = b''

    encoded_enrs = tuple(rlp.encode(enr) for enr in enrs)
    payload = FoundContentPayload(encoded_enrs, content)
    message = FoundContentMessage(payload)
    encoded = message.to_wire_bytes()
    result = decode_message(encoded)
    assert result.payload == message.payload
