from hashlib import sha256
import secrets
from typing import Tuple, Type

import coincurve
from cryptography.hazmat.backends import default_backend as cryptography_default_backend
from cryptography.hazmat.primitives.hashes import SHA256
from cryptography.hazmat.primitives.kdf.hkdf import HKDF
from eth_enr.identity_schemes import V4IdentityScheme
from eth_keys.datatypes import NonRecoverableSignature, PrivateKey, PublicKey
from eth_keys.exceptions import BadSignature
from eth_keys.exceptions import ValidationError as EthKeysValidationError
from eth_typing import NodeID
from eth_utils import ValidationError, encode_hex

from ddht.abc import HandshakeSchemeAPI, HandshakeSchemeRegistryAPI
from ddht.constants import AES128_KEY_SIZE, HKDF_INFO, ID_NONCE_SIGNATURE_PREFIX
from ddht.typing import AES128Key, IDNonce, SessionKeys


class HandshakeSchemeRegistry(HandshakeSchemeRegistryAPI):
    def register(
        self, handshake_scheme_class: Type[HandshakeSchemeAPI]
    ) -> Type[HandshakeSchemeAPI]:
        """Class decorator to register handshake schemes."""
        is_missing_identity_scheme = (
            not hasattr(handshake_scheme_class, "identity_scheme")
            or handshake_scheme_class.identity_scheme is None
        )
        if is_missing_identity_scheme:
            raise ValueError("Handshake schemes must define ID")

        if handshake_scheme_class.identity_scheme in self:
            raise ValueError(
                f"Handshake scheme with id "
                f"{handshake_scheme_class.identity_scheme!r} is already "
                f"registered"
            )

        self[handshake_scheme_class.identity_scheme] = handshake_scheme_class

        return handshake_scheme_class


default_handshake_scheme_registry = HandshakeSchemeRegistry()


def ecdh_agree(private_key: bytes, public_key: bytes) -> bytes:
    """
    Perform the ECDH key agreement.

    The public key is expected in uncompressed format and the resulting secret point will be
    formatted as a 0x02 or 0x03 prefix (depending on the sign of the secret's y component)
    followed by 32 bytes of the x component.
    """
    # We cannot use `cryptography.hazmat.primitives.asymmetric.ec.ECDH only gives us the x
    # component of the shared secret point, but we need both x and y.
    if len(public_key) == 33:
        public_key_eth_keys = PublicKey.from_compressed_bytes(public_key)
    else:
        public_key_eth_keys = PublicKey(public_key)
    public_key_compressed = public_key_eth_keys.to_compressed_bytes()
    public_key_coincurve = coincurve.keys.PublicKey(public_key_compressed)
    secret_coincurve = public_key_coincurve.multiply(private_key)
    return secret_coincurve.format()  # type: ignore


def hkdf_expand_and_extract(
    secret: bytes,
    initiator_node_id: NodeID,
    recipient_node_id: NodeID,
    id_nonce: IDNonce,
) -> Tuple[bytes, bytes, bytes]:
    info = b"".join((HKDF_INFO, initiator_node_id, recipient_node_id))

    hkdf = HKDF(
        algorithm=SHA256(),
        length=3 * AES128_KEY_SIZE,
        salt=id_nonce,
        info=info,
        backend=cryptography_default_backend(),
    )
    expanded_key = hkdf.derive(secret)

    if len(expanded_key) != 3 * AES128_KEY_SIZE:
        raise Exception("Invariant: Secret is expanded to three AES128 keys")

    initiator_key = expanded_key[:AES128_KEY_SIZE]
    recipient_key = expanded_key[AES128_KEY_SIZE : 2 * AES128_KEY_SIZE]  # noqa: E203
    auth_response_key = expanded_key[
        2 * AES128_KEY_SIZE : 3 * AES128_KEY_SIZE  # noqa: E203
    ]

    return initiator_key, recipient_key, auth_response_key


@default_handshake_scheme_registry.register
class V4HandshakeScheme(HandshakeSchemeAPI):
    identity_scheme = V4IdentityScheme

    #
    # Handshake
    #
    @classmethod
    def create_handshake_key_pair(cls) -> Tuple[bytes, bytes]:
        private_key = secrets.token_bytes(cls.identity_scheme.private_key_size)
        public_key = PrivateKey(private_key).public_key.to_bytes()
        return private_key, public_key

    @classmethod
    def validate_handshake_public_key(cls, public_key: bytes) -> None:
        cls.identity_scheme.validate_uncompressed_public_key(public_key)

    @classmethod
    def compute_session_keys(
        cls,
        *,
        local_private_key: bytes,
        remote_public_key: bytes,
        local_node_id: NodeID,
        remote_node_id: NodeID,
        id_nonce: IDNonce,
        is_locally_initiated: bool,
    ) -> SessionKeys:
        secret = ecdh_agree(local_private_key, remote_public_key)

        if is_locally_initiated:
            initiator_node_id, recipient_node_id = local_node_id, remote_node_id
        else:
            initiator_node_id, recipient_node_id = remote_node_id, local_node_id

        initiator_key, recipient_key, auth_response_key = hkdf_expand_and_extract(
            secret, initiator_node_id, recipient_node_id, id_nonce
        )

        if is_locally_initiated:
            encryption_key, decryption_key = initiator_key, recipient_key
        else:
            encryption_key, decryption_key = recipient_key, initiator_key

        return SessionKeys(
            encryption_key=AES128Key(encryption_key),
            decryption_key=AES128Key(decryption_key),
            auth_response_key=AES128Key(auth_response_key),
        )

    @classmethod
    def create_id_nonce_signature(
        cls, *, id_nonce: IDNonce, ephemeral_public_key: bytes, private_key: bytes
    ) -> bytes:
        private_key_object = PrivateKey(private_key)
        signature_input = cls.create_id_nonce_signature_input(
            id_nonce=id_nonce, ephemeral_public_key=ephemeral_public_key
        )
        signature = private_key_object.sign_msg_hash_non_recoverable(signature_input)
        return bytes(signature)

    @classmethod
    def validate_id_nonce_signature(
        cls,
        *,
        id_nonce: IDNonce,
        ephemeral_public_key: bytes,
        signature: bytes,
        public_key: bytes,
    ) -> None:
        signature_input = cls.create_id_nonce_signature_input(
            id_nonce=id_nonce, ephemeral_public_key=ephemeral_public_key
        )
        cls.identity_scheme.validate_signature(
            message_hash=signature_input, signature=signature, public_key=public_key
        )

    #
    # Helpers
    #
    @classmethod
    def validate_compressed_public_key(cls, public_key: bytes) -> None:
        try:
            PublicKey.from_compressed_bytes(public_key)
        except (EthKeysValidationError, ValueError) as error:
            raise ValidationError(
                f"Public key {encode_hex(public_key)} is invalid compressed public key: {error}"
            ) from error

    @classmethod
    def validate_uncompressed_public_key(cls, public_key: bytes) -> None:
        try:
            PublicKey(public_key)
        except EthKeysValidationError as error:
            raise ValidationError(
                f"Public key {encode_hex(public_key)} is invalid uncompressed public key: {error}"
            ) from error

    @classmethod
    def validate_signature(
        cls, *, message_hash: bytes, signature: bytes, public_key: bytes
    ) -> None:
        public_key_object = PublicKey.from_compressed_bytes(public_key)

        try:
            signature_object = NonRecoverableSignature(signature)
        except BadSignature:
            is_valid = False
        else:
            is_valid = signature_object.verify_msg_hash(message_hash, public_key_object)

        if not is_valid:
            raise ValidationError(
                f"Signature {encode_hex(signature)} is not valid for message hash "
                f"{encode_hex(message_hash)} and public key {encode_hex(public_key)}"
            )

    @classmethod
    def create_id_nonce_signature_input(
        cls, *, id_nonce: IDNonce, ephemeral_public_key: bytes
    ) -> bytes:
        preimage = b"".join((ID_NONCE_SIGNATURE_PREFIX, id_nonce, ephemeral_public_key))
        return sha256(preimage).digest()
