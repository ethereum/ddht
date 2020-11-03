from eth_enr.tools.factories import ENRFactory
from hypothesis import given
from hypothesis import strategies as st
import rlp

from ddht.v5_1.alexandria.messages import (
    FindNodesMessage,
    FoundNodesMessage,
    PingMessage,
    PongMessage,
    decode_message,
)
from ddht.v5_1.alexandria.payloads import (
    FindNodesPayload,
    FoundNodesPayload,
    PingPayload,
    PongPayload,
)


@given(enr_seq=st.integers(min_value=0, max_value=2 ** 32 - 1))
def test_ping_message_encoding_round_trip(enr_seq):
    payload = PingPayload(enr_seq=enr_seq)
    message = PingMessage(payload)
    encoded = message.to_wire_bytes()
    result = decode_message(encoded)
    assert result == message


@given(enr_seq=st.integers(min_value=0, max_value=2 ** 32 - 1))
def test_pong_message_encoding_round_trip(enr_seq):
    payload = PongPayload(enr_seq=enr_seq)
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
