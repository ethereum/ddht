from hashlib import sha256

from eth_keys.datatypes import NonRecoverableSignature, PrivateKey
from eth_utils import ValidationError, decode_hex, keccak
from hypothesis import given
import pytest

from ddht.handshake_schemes import ecdh_agree, hkdf_expand_and_extract
from ddht.tools.v5_strategies import id_nonce_st, private_key_st
from ddht.v5.constants import ID_NONCE_SIGNATURE_PREFIX
from ddht.v5.handshake_schemes import SignatureInputs, V4HandshakeScheme


def test_handshake_key_generation():
    private_key, public_key = V4HandshakeScheme.create_handshake_key_pair()
    V4HandshakeScheme.validate_uncompressed_public_key(public_key)
    V4HandshakeScheme.validate_handshake_public_key(public_key)
    assert PrivateKey(private_key).public_key.to_bytes() == public_key


@pytest.mark.parametrize("public_key", (b"\x01" * 64, b"\x02" * 64))
def test_handshake_public_key_validation_valid(public_key):
    V4HandshakeScheme.validate_handshake_public_key(public_key)


@pytest.mark.parametrize(
    "public_key",
    (b"", b"\x02" * 31, b"\x02" * 32, b"\x02" * 33, b"\x02" * 63, b"\x02" * 65),
)
def test_handshake_public_key_validation_invalid(public_key):
    with pytest.raises(ValidationError):
        V4HandshakeScheme.validate_handshake_public_key(public_key)


@given(private_key=private_key_st, id_nonce=id_nonce_st, ephemeral_key=private_key_st)
def test_id_nonce_signing(private_key, id_nonce, ephemeral_key):
    ephemeral_public_key = PrivateKey(ephemeral_key).public_key.to_bytes()
    signature = V4HandshakeScheme.create_id_nonce_signature(
        signature_inputs=SignatureInputs(id_nonce, ephemeral_public_key),
        private_key=private_key,
    )
    signature_object = NonRecoverableSignature(signature)
    message_hash = sha256(
        ID_NONCE_SIGNATURE_PREFIX + id_nonce + ephemeral_public_key
    ).digest()
    assert signature_object.verify_msg_hash(
        message_hash, PrivateKey(private_key).public_key
    )


@given(private_key=private_key_st, id_nonce=id_nonce_st, ephemeral_key=private_key_st)
def test_valid_id_nonce_signature_validation(private_key, id_nonce, ephemeral_key):
    ephemeral_public_key = PrivateKey(ephemeral_key).public_key.to_bytes()
    signature = V4HandshakeScheme.create_id_nonce_signature(
        signature_inputs=SignatureInputs(id_nonce, ephemeral_public_key),
        private_key=private_key,
    )
    public_key = PrivateKey(private_key).public_key.to_compressed_bytes()
    V4HandshakeScheme.validate_id_nonce_signature(
        signature_inputs=SignatureInputs(id_nonce, ephemeral_public_key),
        signature=signature,
        public_key=public_key,
    )


def test_invalid_id_nonce_signature_validation():
    id_nonce = b"\xff" * 10
    private_key = b"\x11" * 32
    ephemeral_public_key = b"\x22" * 64
    signature = V4HandshakeScheme.create_id_nonce_signature(
        signature_inputs=SignatureInputs(id_nonce, ephemeral_public_key),
        private_key=private_key,
    )

    public_key = PrivateKey(private_key).public_key.to_compressed_bytes()
    different_public_key = PrivateKey(b"\x22" * 32).public_key.to_compressed_bytes()
    different_id_nonce = b"\x00" * 10
    different_ephemeral_public_key = b"\x00" * 64
    assert different_public_key != public_key
    assert different_id_nonce != id_nonce
    assert different_ephemeral_public_key != ephemeral_public_key

    with pytest.raises(ValidationError):
        V4HandshakeScheme.validate_id_nonce_signature(
            signature_inputs=SignatureInputs(id_nonce, ephemeral_public_key),
            signature=signature,
            public_key=different_public_key,
        )

    with pytest.raises(ValidationError):
        V4HandshakeScheme.validate_id_nonce_signature(
            signature_inputs=SignatureInputs(different_id_nonce, ephemeral_public_key),
            signature=signature,
            public_key=public_key,
        )

    with pytest.raises(ValidationError):
        V4HandshakeScheme.validate_id_nonce_signature(
            signature_inputs=SignatureInputs(id_nonce, different_ephemeral_public_key),
            signature=signature,
            public_key=public_key,
        )


@given(
    initiator_private_key=private_key_st,
    recipient_private_key=private_key_st,
    id_nonce=id_nonce_st,
)
def test_session_key_derivation(initiator_private_key, recipient_private_key, id_nonce):
    initiator_private_key_object = PrivateKey(initiator_private_key)
    recipient_private_key_object = PrivateKey(recipient_private_key)

    initiator_public_key = initiator_private_key_object.public_key.to_bytes()
    recipient_public_key = recipient_private_key_object.public_key.to_bytes()

    initiator_node_id = keccak(initiator_private_key_object.public_key.to_bytes())
    recipient_node_id = keccak(recipient_private_key_object.public_key.to_bytes())

    initiator_session_keys = V4HandshakeScheme.compute_session_keys(
        local_private_key=initiator_private_key,
        remote_public_key=recipient_public_key,
        local_node_id=initiator_node_id,
        remote_node_id=recipient_node_id,
        salt=id_nonce,
        is_locally_initiated=True,
    )
    recipient_session_keys = V4HandshakeScheme.compute_session_keys(
        local_private_key=recipient_private_key,
        remote_public_key=initiator_public_key,
        local_node_id=recipient_node_id,
        remote_node_id=initiator_node_id,
        salt=id_nonce,
        is_locally_initiated=False,
    )

    assert (
        initiator_session_keys.auth_response_key
        == recipient_session_keys.auth_response_key
    )
    assert (
        initiator_session_keys.encryption_key == recipient_session_keys.decryption_key
    )
    assert (
        initiator_session_keys.decryption_key == recipient_session_keys.encryption_key
    )


@pytest.mark.parametrize(
    ["local_secret_key", "remote_public_key", "shared_secret_key"],
    [
        [
            decode_hex(
                "0xfb757dc581730490a1d7a00deea65e9b1936924caaea8f44d476014856b68736"
            ),
            decode_hex(
                "0x9961e4c2356d61bedb83052c115d311acb3a96f5777296dcf297351130266231503061ac4aaee666073d"  # noqa: E501
                "7e5bc2c80c3f5c5b500c1cb5fd0a76abbb6b675ad157"
            ),
            decode_hex(
                "0x033b11a2a1f214567e1537ce5e509ffd9b21373247f2a3ff6841f4976f53165e7e"
            ),
        ]
    ],
)
def test_official_key_agreement(local_secret_key, remote_public_key, shared_secret_key):
    assert ecdh_agree(local_secret_key, remote_public_key) == shared_secret_key


@pytest.mark.parametrize(
    [
        "secret",
        "initiator_node_id",
        "recipient_node_id",
        "id_nonce",
        "initiator_key",
        "recipient_key",
        "auth_response_key",
    ],
    [
        [
            decode_hex(
                "0x02a77e3aa0c144ae7c0a3af73692b7d6e5b7a2fdc0eda16e8d5e6cb0d08e88dd04"
            ),
            decode_hex(
                "0xa448f24c6d18e575453db13171562b71999873db5b286df957af199ec94617f7"
            ),
            decode_hex(
                "0x885bba8dfeddd49855459df852ad5b63d13a3fae593f3f9fa7e317fd43651409"
            ),
            decode_hex(
                "0x0101010101010101010101010101010101010101010101010101010101010101"
            ),
            decode_hex("0x238d8b50e4363cf603a48c6cc3542967"),
            decode_hex("0xbebc0183484f7e7ca2ac32e3d72c8891"),
            decode_hex("0xe987ad9e414d5b4f9bfe4ff1e52f2fae"),
        ]
    ],
)
def test_official_key_derivation(
    secret,
    initiator_node_id,
    recipient_node_id,
    id_nonce,
    initiator_key,
    recipient_key,
    auth_response_key,
):
    derived_keys = hkdf_expand_and_extract(
        secret, initiator_node_id, recipient_node_id, id_nonce
    )
    assert derived_keys[0] == initiator_key
    assert derived_keys[1] == recipient_key
    assert derived_keys[2] == auth_response_key


@pytest.mark.parametrize(
    ["id_nonce", "ephemeral_public_key", "local_secret_key", "id_nonce_signature"],
    [
        [
            decode_hex(
                "0xa77e3aa0c144ae7c0a3af73692b7d6e5b7a2fdc0eda16e8d5e6cb0d08e88dd04"
            ),
            decode_hex(
                "0x9961e4c2356d61bedb83052c115d311acb3a96f5777296dcf297351130266231503061ac4aaee666"
                "073d7e5bc2c80c3f5c5b500c1cb5fd0a76abbb6b675ad157"
            ),
            decode_hex(
                "0xfb757dc581730490a1d7a00deea65e9b1936924caaea8f44d476014856b68736"
            ),
            decode_hex(
                "0xc5036e702a79902ad8aa147dabfe3958b523fd6fa36cc78e2889b912d682d8d35fdea142e141f690"
                "736d86f50b39746ba2d2fc510b46f82ee08f08fd55d133a4"
            ),
        ]
    ],
)
def test_official_id_nonce_signature(
    id_nonce, ephemeral_public_key, local_secret_key, id_nonce_signature
):
    created_signature = V4HandshakeScheme.create_id_nonce_signature(
        signature_inputs=SignatureInputs(id_nonce, ephemeral_public_key),
        private_key=local_secret_key,
    )
    assert created_signature == id_nonce_signature
