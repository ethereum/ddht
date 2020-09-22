from hashlib import sha256
from typing import NamedTuple

from eth_keys.datatypes import PrivateKey
from eth_typing import NodeID

from ddht.handshake_schemes import BaseV4HandshakeScheme, HandshakeSchemeRegistry
from ddht.v5_1.constants import ID_NONCE_SIGNATURE_PREFIX
from ddht.v5_1.packets import Header, WhoAreYouPacket

v51_handshake_scheme_registry = HandshakeSchemeRegistry()


class SignatureInputs(NamedTuple):
    iv: bytes
    header: Header
    who_are_you: WhoAreYouPacket
    ephemeral_public_key: bytes
    recipient_node_id: NodeID


@v51_handshake_scheme_registry.register
class V4HandshakeScheme(BaseV4HandshakeScheme[SignatureInputs]):
    signature_inputs_cls = SignatureInputs

    @classmethod
    def create_id_nonce_signature(
        cls, *, signature_inputs: SignatureInputs, private_key: bytes,
    ) -> bytes:
        private_key_object = PrivateKey(private_key)
        signature_input = cls.create_id_nonce_signature_input(
            signature_inputs=signature_inputs
        )
        signature = private_key_object.sign_msg_hash_non_recoverable(signature_input)
        return bytes(signature)

    @classmethod
    def validate_id_nonce_signature(
        cls, *, signature_inputs: SignatureInputs, signature: bytes, public_key: bytes,
    ) -> None:
        signature_input = cls.create_id_nonce_signature_input(
            signature_inputs=signature_inputs
        )
        cls.identity_scheme.validate_signature(
            message_hash=signature_input, signature=signature, public_key=public_key
        )

    @classmethod
    def create_id_nonce_signature_input(
        cls, *, signature_inputs: SignatureInputs,
    ) -> bytes:
        preimage = b"".join(
            (
                ID_NONCE_SIGNATURE_PREFIX,
                signature_inputs.iv,
                signature_inputs.header.to_wire_bytes(),
                signature_inputs.who_are_you.to_wire_bytes(),
                signature_inputs.ephemeral_public_key,
                signature_inputs.recipient_node_id,
            )
        )
        return sha256(preimage).digest()
