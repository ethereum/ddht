import ipaddress
from typing import NamedTuple, NewType, Tuple, Union

AES128Key = NewType("AES128Key", bytes)
Nonce = NewType("Nonce", bytes)
IDNonce = NewType("IDNonce", bytes)
NodeID = NewType("NodeID", bytes)


class SessionKeys(NamedTuple):
    encryption_key: AES128Key
    decryption_key: AES128Key
    auth_response_key: AES128Key


AnyIPAddress = Union[ipaddress.IPv4Address, ipaddress.IPv6Address]

ENR_KV = Tuple[bytes, Union[int, bytes]]
