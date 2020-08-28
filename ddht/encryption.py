import math
from typing import Iterator

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.ciphers import Cipher
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.ciphers.algorithms import AES
from cryptography.hazmat.primitives.ciphers.modes import CTR

from ddht.constants import AES128_KEY_SIZE
from ddht.exceptions import DecryptionError
from ddht.typing import AES128Key, Nonce
from ddht.v5.constants import NONCE_SIZE
from ddht.validation import validate_length


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


def aesctr_encrypt(key: AES128Key, iv: bytes, plain_text: bytes) -> bytes:
    cipher = Cipher(AES(key), CTR(iv), backend=default_backend())
    encryptor = cipher.encryptor()
    return encryptor.update(plain_text) + encryptor.finalize()


def aesctr_decrypt_stream(
    key: AES128Key, iv: bytes, cipher_text: bytes
) -> Iterator[int]:
    aes_key = AES(key)
    ctr = CTR(iv)

    try:
        cipher = Cipher(aes_key, ctr, backend=default_backend())
    except ValueError as err:
        raise DecryptionError(str(err)) from err

    decryptor = cipher.decryptor()
    num_blocks = int(math.ceil(len(cipher_text) / 16))
    for i in range(num_blocks):
        cipher_text_block = cipher_text[i * 16 : (i + 1) * 16]
        plain_text_block = decryptor.update(cipher_text_block)
        yield from plain_text_block
    yield from decryptor.finalize()


def aesctr_decrypt(key: AES128Key, iv: bytes, cipher_text: bytes) -> bytes:
    return bytes(aesctr_decrypt_stream(key, iv, cipher_text))
