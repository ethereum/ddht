from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from eth.validation import validate_length

from ddht.constants import AES128_KEY_SIZE
from ddht.exceptions import DecryptionError
from ddht.typing import AES128Key, Nonce
from ddht.v5.constants import NONCE_SIZE


def validate_aes128_key(key: AES128Key) -> None:
    validate_length(key, AES128_KEY_SIZE, "AES128 key")


def validate_nonce(nonce: bytes) -> None:
    validate_length(nonce, NONCE_SIZE, "nonce")


def aesgcm_encrypt(
    key: AES128Key, nonce: Nonce, plain_text: bytes, authenticated_data: bytes
) -> bytes:
    validate_aes128_key(key)
    validate_nonce(nonce)

    aesgcm = AESGCM(key)
    cipher_text = aesgcm.encrypt(nonce, plain_text, authenticated_data)
    return cipher_text


def aesgcm_decrypt(
    key: AES128Key, nonce: Nonce, cipher_text: bytes, authenticated_data: bytes
) -> bytes:
    validate_aes128_key(key)
    validate_nonce(nonce)

    aesgcm = AESGCM(key)
    try:
        plain_text = aesgcm.decrypt(nonce, cipher_text, authenticated_data)
    except InvalidTag as error:
        raise DecryptionError() from error
    else:
        return plain_text
