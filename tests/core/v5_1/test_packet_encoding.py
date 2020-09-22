from eth_enr.tools.factories import ENRFactory
import pytest

from ddht.v5_1.messages import PingMessage
from ddht.v5_1.packets import (
    HandshakeHeader,
    HandshakePacket,
    MessagePacket,
    Packet,
    WhoAreYouPacket,
    decode_packet,
)


def test_message_packet_encoding():
    initiator_key = b"\x01" * 16
    aes_gcm_nonce = b"\x02" * 12
    source_node_id = b"\x03" * 32
    dest_node_id = b"\x04" * 32
    message = PingMessage(b"\x01", 0)
    auth_data = MessagePacket(source_node_id)

    packet = Packet.prepare(
        aes_gcm_nonce=aes_gcm_nonce,
        initiator_key=initiator_key,
        message=message,
        auth_data=auth_data,
        dest_node_id=dest_node_id,
    )
    packet_wire_bytes = packet.to_wire_bytes()
    result = decode_packet(packet_wire_bytes, dest_node_id)

    assert result == packet


def test_who_are_you_packet_encoding():
    initiator_key = b"\x01" * 16
    aes_gcm_nonce = b"\x02" * 12
    dest_node_id = b"\x04" * 32
    message = PingMessage(b"\x01", 0)
    auth_data = WhoAreYouPacket(id_nonce=b"\x06" * 16, enr_sequence_number=0x07)

    packet = Packet.prepare(
        aes_gcm_nonce=aes_gcm_nonce,
        initiator_key=initiator_key,
        message=message,
        auth_data=auth_data,
        dest_node_id=dest_node_id,
    )
    packet_wire_bytes = packet.to_wire_bytes()
    result = decode_packet(packet_wire_bytes, dest_node_id)

    assert result == packet


@pytest.mark.parametrize(
    "enr", (None, ENRFactory(),),
)
def test_handshake_packet_encoding(enr):
    initiator_key = b"\x01" * 16
    aes_gcm_nonce = b"\x02" * 12
    source_node_id = b"\x03" * 32
    dest_node_id = b"\x04" * 32
    message = PingMessage(b"\x01", 0)
    auth_data = HandshakePacket(
        auth_data_head=HandshakeHeader(
            source_node_id=source_node_id, signature_size=64, ephemeral_key_size=33,
        ),
        id_signature=b"\x05" * 64,
        ephemeral_public_key=b"\x06" * 33,
        record=enr,
    )

    packet = Packet.prepare(
        aes_gcm_nonce=aes_gcm_nonce,
        initiator_key=initiator_key,
        message=message,
        auth_data=auth_data,
        dest_node_id=dest_node_id,
    )
    packet_wire_bytes = packet.to_wire_bytes()
    result = decode_packet(packet_wire_bytes, dest_node_id)

    assert result == packet
