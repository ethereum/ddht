from hypothesis import given
from hypothesis import strategies as st

from ddht.v5_1.alexandria.messages import PingMessage, PongMessage, decode_message
from ddht.v5_1.alexandria.payloads import PingPayload, PongPayload


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
