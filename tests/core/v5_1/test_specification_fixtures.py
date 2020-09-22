import io

from eth_keys import keys
from eth_utils import decode_hex, to_int
import pytest

from ddht.v5_1.constants import HEADER_PACKET_SIZE, WHO_ARE_YOU_PACKET_SIZE
from ddht.v5_1.handshake_schemes import V4HandshakeScheme
from ddht.v5_1.messages import PingMessage, decode_message
from ddht.v5_1.packets import (
    HandshakeHeader,
    HandshakePacket,
    Header,
    MessagePacket,
    WhoAreYouPacket,
    decode_packet,
)

NODE_KEY_A = decode_hex(
    "0xeef77acb6c6a6eebc5b363a475ac583ec7eccdb42b6481424c60f59aa326547f"
)
NODE_KEY_B = decode_hex(
    "0x66fb62bfbd66b9177a138c1e5cddbe4f7c30c343e94e68df8769459cb1cde628"
)


PACKET_DECODING_FIXTURES = (
    # ping message packet
    {
        "src-node-id": "0xaaaa8419e9f49d0083561b48287df592939a8d19947d8c0ef88f2a4856a69fbb",
        "dest-node-id": "0xbbbb9d047f0488c0b5a93c1c3f2d8bafc7c8ff337024a55434a0d0555de64db9",
        "read-key": "0x00000000000000000000000000000000",
        "type": "decoding",
        "nonce": "0xffffffffffffffffffffffff",
        "packet": {
            "type": "message",
            "message": {"req-id": "0x00000001", "enr-seq": "0x2"},
        },
        "encoded": (
            "00000000000000000000000000000000088b3d4342774649325f313964a39e55"
            "ea96c005ad52be8c7560413a7008f16c9e6d2f43bbea8814a546b7409ce783d3"
            "4c4f53245d08dab84102ed931f66d1492acb308fa1c6715b9d139b81acbdcc"
        ),
    },
    # who are you packet
    {
        "src-node-id": "0xaaaa8419e9f49d0083561b48287df592939a8d19947d8c0ef88f2a4856a69fbb",
        "dest-node-id": "0xbbbb9d047f0488c0b5a93c1c3f2d8bafc7c8ff337024a55434a0d0555de64db9",
        "type": "decoding",
        "packet": {
            "type": "whoareyou",
            "iv": "0x00000000000000000000000000000000",
            "authdata": "0x0102030405060708090a0b0c0d0e0f100000000000000000",
            "request-nonce": "0x0102030405060708090a0b0c",
            "id-nonce": "0x0102030405060708090a0b0c0d0e0f10",
            "enr-seq": "0x0",
        },
        "encoded": (
            "00000000000000000000000000000000088b3d434277464933a1ccc59f5967ad"
            "1d6035f15e528627dde75cd68292f9e6c27d6b66c8100a873fcbaed4e16b8d"
        ),
    },
    # handshake packet (ping message) (without ENR)
    {
        "read-key": "0x4f9fac6de7567d1e3b1241dffe90f662",
        "src-node-id": "0xaaaa8419e9f49d0083561b48287df592939a8d19947d8c0ef88f2a4856a69fbb",
        "dest-node-id": "0xbbbb9d047f0488c0b5a93c1c3f2d8bafc7c8ff337024a55434a0d0555de64db9",
        "nonce": "0xffffffffffffffffffffffff",
        "type": "decoding",
        "packet": {
            "type": "handshake",
            "message": {"req-id": "0x00000001", "enr-seq": "0x1"},
        },
        "handshake-inputs": {
            "whoareyou": {
                "challenge-data": (
                    "0x"
                    "0000000000000000000000000000000064697363763500010101020304050607"
                    "08090a0b0c00180102030405060708090a0b0c0d0e0f100000000000000001"
                ),
                "authdata": "0x0102030405060708090a0b0c0d0e0f100000000000000001",
                "request-nonce": "0x0102030405060708090a0b0c",
                "id-nonce": "0x0102030405060708090a0b0c0d0e0f10",
                "enr-seq": "0x1",
            },
            "ephemeral-key": "0x0288ef00023598499cb6c940146d050d2b1fb914198c327f76aad590bead68b6",
            "ephemeral-pubkey": "0x039a003ba6517b473fa0cd74aefe99dadfdb34627f90fec6362df85803908f53a5",  # noqa: E501
        },
        "encoded": (
            "00000000000000000000000000000000088b3d4342774649305f313964a39e55"
            "ea96c005ad521d8c7560413a7008f16c9e6d2f43bbea8814a546b7409ce783d3"
            "4c4f53245d08da4bb252012b2cba3f4f374a90a75cff91f142fa9be3e0a5f3ef"
            "268ccb9065aeecfd67a999e7fdc137e062b2ec4a0eb92947f0d9a74bfbf44dfb"
            "a776b21301f8b65efd5796706adff216ab862a9186875f9494150c4ae06fa4d1"
            "f0396c93f215fa4ef524f1eadf5f0f4126b79336671cbcf7a885b1f8bd2a5d83"
            "9cf8"
        ),
    },
    # handshake packet (ping message) (with ENR)
    {
        "read-key": "0x53b1c075f41876423154e157470c2f48",
        "src-node-id": "0xaaaa8419e9f49d0083561b48287df592939a8d19947d8c0ef88f2a4856a69fbb",
        "dest-node-id": "0xbbbb9d047f0488c0b5a93c1c3f2d8bafc7c8ff337024a55434a0d0555de64db9",
        "nonce": "0xffffffffffffffffffffffff",
        "type": "decoding",
        "packet": {
            "type": "handshake",
            "message": {"req-id": "0x00000001", "enr-seq": "0x1"},
        },
        "handshake-inputs": {
            "whoareyou": {
                "challenge-data": (
                    "0x"
                    "000000000000000000000000000000006469736376350001010102030405060"
                    "708090a0b0c00180102030405060708090a0b0c0d0e0f100000000000000000"
                ),
                "request-nonce": "0x0102030405060708090a0b0c",
                "id-nonce": "0x0102030405060708090a0b0c0d0e0f10",
                "enr-seq": "0x0",
            },
            "ephemeral-key": "0x0288ef00023598499cb6c940146d050d2b1fb914198c327f76aad590bead68b6",
            "ephemeral-pubkey": "0x039a003ba6517b473fa0cd74aefe99dadfdb34627f90fec6362df85803908f53a5",  # noqa: E501
        },
        "encoded": (
            "00000000000000000000000000000000088b3d4342774649305f313964a39e55"
            "ea96c005ad539c8c7560413a7008f16c9e6d2f43bbea8814a546b7409ce783d3"
            "4c4f53245d08da4bb23698868350aaad22e3ab8dd034f548a1c43cd246be9856"
            "2fafa0a1fa86d8e7a3b95ae78cc2b988ded6a5b59eb83ad58097252188b902b2"
            "1481e30e5e285f19735796706adff216ab862a9186875f9494150c4ae06fa4d1"
            "f0396c93f215fa4ef524e0ed04c3c21e39b1868e1ca8105e585ec17315e755e6"
            "cfc4dd6cb7fd8e1a1f55e49b4b5eb024221482105346f3c82b15fdaae36a3bb1"
            "2a494683b4a3c7f2ae41306252fed84785e2bbff3b022812d0882f06978df84a"
            "80d443972213342d04b9048fc3b1d5fcb1df0f822152eced6da4d3f6df27e70e"
            "4539717307a0208cd208d65093ccab5aa596a34d7511401987662d8cf62b1394"
            "71"
        ),
    },
)


@pytest.mark.parametrize("fixture", PACKET_DECODING_FIXTURES)
def test_v51_specification_packet_decoding_fixtures(fixture):
    if fixture["type"] == "decoding":
        do_encoding_fixture_test(fixture)
    else:
        raise Exception("Not supported")


def do_handshake_packet_fixture_decoding_test(fixture):
    source_node_id = decode_hex(fixture["src-node-id"])
    dest_node_id = decode_hex(fixture["dest-node-id"])
    encoded_packet = decode_hex(fixture["encoded"])
    ping_enr_seq = to_int(hexstr=fixture["packet"]["message"]["enr-seq"])
    who_are_you_enr_seq = to_int(
        hexstr=fixture["handshake-inputs"]["whoareyou"]["enr-seq"]
    )

    if who_are_you_enr_seq == ping_enr_seq and who_are_you_enr_seq != 0:
        should_have_record = False
    else:
        should_have_record = True

    # ephemeral_private_key = decode_hex(fixture['handshake-inputs']['ephemeral-key'])
    ephemeral_public_key = decode_hex(fixture["handshake-inputs"]["ephemeral-pubkey"])
    # ephemeral_private_key = decode_hex(fixture["handshake-inputs"]["ephemeral-key"])

    # request_nonce = decode_hex(fixture['handshake-inputs']['whoareyou']['request-nonce'])
    challenge_data = decode_hex(
        fixture["handshake-inputs"]["whoareyou"]["challenge-data"]
    )
    masking_iv, static_header, who_are_you = extract_challenge_data(challenge_data)

    id_nonce = decode_hex(fixture["handshake-inputs"]["whoareyou"]["id-nonce"])
    assert who_are_you.id_nonce == id_nonce

    aes_gcm_nonce = decode_hex(fixture["nonce"])
    # TODO: why doesn't this match
    # assert static_header.aes_gcm_nonce == aes_gcm_nonce

    signature_inputs = V4HandshakeScheme.signature_inputs_cls(
        iv=masking_iv,
        header=static_header,
        who_are_you=WhoAreYouPacket(id_nonce, who_are_you_enr_seq),
        ephemeral_public_key=ephemeral_public_key,
        recipient_node_id=dest_node_id,
    )

    id_nonce_signature = V4HandshakeScheme.create_id_nonce_signature(
        signature_inputs=signature_inputs, private_key=NODE_KEY_A,
    )

    packet = decode_packet(encoded_packet, dest_node_id)
    expected_auth_data = HandshakePacket(
        auth_data_head=HandshakeHeader(source_node_id, 64, 33),
        id_signature=id_nonce_signature,
        ephemeral_public_key=ephemeral_public_key,
        record=packet.auth_data.record,
    )

    assert expected_auth_data == packet.auth_data
    assert packet.header.aes_gcm_nonce == aes_gcm_nonce

    if should_have_record:
        assert packet.auth_data.record is not None
        assert packet.auth_data.record.node_id == source_node_id
    else:
        assert packet.auth_data.record is None

    expected_message = PingMessage(
        request_id=decode_hex(fixture["packet"]["message"]["req-id"]),
        enr_seq=to_int(hexstr=fixture["packet"]["message"]["enr-seq"]),
    )
    actual_message = decode_message(
        decryption_key=decode_hex(fixture["read-key"]),
        aes_gcm_nonce=aes_gcm_nonce,
        message_cipher_text=packet.message_cipher_text,
        authenticated_data=packet.challenge_data,
    )
    assert expected_message == actual_message


def do_encoding_fixture_test(fixture):
    if fixture["packet"]["type"] == "whoareyou":
        do_who_are_you_packet_fixture_decoding_test(fixture)
    elif fixture["packet"]["type"] == "message":
        do_message_packet_fixture_decoding_test(fixture)
    elif fixture["packet"]["type"] == "handshake":
        do_handshake_packet_fixture_decoding_test(fixture)
    else:
        raise Exception("Not supported")


def do_who_are_you_packet_fixture_decoding_test(fixture):
    dest_node_id = decode_hex(fixture["dest-node-id"])
    expected_auth_data = WhoAreYouPacket(
        id_nonce=decode_hex(fixture["packet"]["id-nonce"]),
        enr_sequence_number=to_int(hexstr=fixture["packet"]["enr-seq"]),
    )
    encoded_packet = decode_hex(fixture["encoded"])
    aes_gcm_nonce = decode_hex(fixture["packet"]["request-nonce"])

    packet = decode_packet(encoded_packet, dest_node_id)

    assert packet.auth_data == expected_auth_data
    assert packet.header.aes_gcm_nonce == aes_gcm_nonce


def do_message_packet_fixture_decoding_test(fixture):
    dest_node_id = decode_hex(fixture["dest-node-id"])
    expected_auth_data = MessagePacket(
        source_node_id=decode_hex(fixture["src-node-id"]),
    )
    expected_message = PingMessage(
        request_id=decode_hex(fixture["packet"]["message"]["req-id"]),
        enr_seq=to_int(hexstr=fixture["packet"]["message"]["enr-seq"]),
    )
    encoded_packet = decode_hex(fixture["encoded"])
    packet = decode_packet(encoded_packet, dest_node_id)
    assert packet.auth_data == expected_auth_data

    aes_gcm_nonce = decode_hex(fixture["nonce"])

    actual_message = decode_message(
        decryption_key=decode_hex(fixture["read-key"]),
        aes_gcm_nonce=aes_gcm_nonce,
        message_cipher_text=packet.message_cipher_text,
        authenticated_data=packet.challenge_data,
    )
    assert actual_message == expected_message


ID_NONCE_SIGNING_FIXTURES = (
    {
        "static-key": "0xfb757dc581730490a1d7a00deea65e9b1936924caaea8f44d476014856b68736",
        "challenge-data": (
            "0x"
            "0000000000000000000000000000000064697363763500010101020304050607"
            "08090a0b0c00180102030405060708090a0b0c0d0e0f100000000000000000"
        ),
        "ephemeral-pubkey": "0x039961e4c2356d61bedb83052c115d311acb3a96f5777296dcf297351130266231",
        "node-id-B": "0xbbbb9d047f0488c0b5a93c1c3f2d8bafc7c8ff337024a55434a0d0555de64db9",
        "signature": (
            "0x"
            "94852a1e2318c4e5e9d422c98eaf19d1d90d876b29cd06ca7cb7546d0fff7b48"
            "4fe86c09a064fe72bdbef73ba8e9c34df0cd2b53e9d65528c2c7f336d5dfc6e6"
        ),
    },
)


def extract_challenge_data(challenge_data):
    stream = io.BytesIO(challenge_data)

    masking_iv = stream.read(16)
    static_header = Header.from_wire_bytes(stream.read(HEADER_PACKET_SIZE))
    who_are_you = WhoAreYouPacket.from_wire_bytes(stream.read(WHO_ARE_YOU_PACKET_SIZE))

    assert stream.read() == b""

    return masking_iv, static_header, who_are_you


@pytest.mark.parametrize("fixture", ID_NONCE_SIGNING_FIXTURES)
def test_v51_specification_id_nonce_signing_fixtures(fixture):
    private_key = decode_hex(fixture["static-key"])
    public_key = keys.PrivateKey(private_key).public_key.to_compressed_bytes()

    challenge_data = decode_hex(fixture["challenge-data"])
    masking_iv, static_header, who_are_you = extract_challenge_data(challenge_data)

    ephemeral_public_key = decode_hex(fixture["ephemeral-pubkey"])
    recipient_node_id = decode_hex(fixture["node-id-B"])
    expected_signature = decode_hex(fixture["signature"])

    signature_inputs = V4HandshakeScheme.signature_inputs_cls(
        iv=masking_iv,
        header=static_header,
        who_are_you=who_are_you,
        ephemeral_public_key=ephemeral_public_key,
        recipient_node_id=recipient_node_id,
    )
    actual_signature = V4HandshakeScheme.create_id_nonce_signature(
        signature_inputs=signature_inputs, private_key=private_key,
    )
    assert actual_signature == expected_signature

    V4HandshakeScheme.validate_id_nonce_signature(
        signature_inputs=signature_inputs,
        signature=expected_signature,
        public_key=public_key,
    )


KEY_DERIVATION_FIXTURES = (
    {
        "ephemeral-key": "0xfb757dc581730490a1d7a00deea65e9b1936924caaea8f44d476014856b68736",
        "dest-pubkey": "0x0317931e6e0840220642f230037d285d122bc59063221ef3226b1f403ddc69ca91",
        "node-id-a": "0xaaaa8419e9f49d0083561b48287df592939a8d19947d8c0ef88f2a4856a69fbb",
        "node-id-b": "0xbbbb9d047f0488c0b5a93c1c3f2d8bafc7c8ff337024a55434a0d0555de64db9",
        "challenge-data": (
            "0x"
            "000000000000000000000000000000006469736376350001010102030405060"
            "708090a0b0c00180102030405060708090a0b0c0d0e0f100000000000000000"
        ),
        "initiator-key": "0xdccc82d81bd610f4f76d3ebe97a40571",
        "recipient-key": "0xac74bb8773749920b0d3a8881c173ec5",
    },
)


@pytest.mark.parametrize("fixture", KEY_DERIVATION_FIXTURES)
def test_v51_specification_key_derivation(fixture):
    ephemeral_private_key = decode_hex(fixture["ephemeral-key"])
    dest_public_key = decode_hex(fixture["dest-pubkey"])
    node_id_A = decode_hex(fixture["node-id-a"])
    node_id_B = decode_hex(fixture["node-id-b"])
    challenge_data = decode_hex(fixture["challenge-data"])
    initiator_key = decode_hex(fixture["initiator-key"])
    recipient_key = decode_hex(fixture["recipient-key"])

    session_keys = V4HandshakeScheme.compute_session_keys(
        local_private_key=ephemeral_private_key,
        remote_public_key=dest_public_key,
        local_node_id=node_id_A,
        remote_node_id=node_id_B,
        salt=challenge_data,
        is_locally_initiated=True,
    )
    assert session_keys.encryption_key == initiator_key
    assert session_keys.decryption_key == recipient_key
