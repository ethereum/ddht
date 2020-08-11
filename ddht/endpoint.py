from socket import inet_ntoa
from typing import NamedTuple


class Endpoint(NamedTuple):
    ip_address: bytes
    port: int

    def __str__(self) -> str:
        return f"{inet_ntoa(self.ip_address)}:{self.port}"
