AES128Key = NewType("AES128Key", bytes)
Nonce = NewType("Nonce", bytes)
IDNonce = NewType("IDNonce", bytes)
NodeID = NewType("NodeID", bytes)


class SessionKeys(NamedTuple):
    encryption_key: AES128Key
    decryption_key: AES128Key
    auth_response_key: AES128Key
