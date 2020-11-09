from socket import inet_ntoa
from typing import NamedTuple

from eth_enr import ENRAPI
from eth_enr.constants import IP_V4_ADDRESS_ENR_KEY, UDP_PORT_ENR_KEY

from ddht.exceptions import MissingEndpointFields


class Endpoint(NamedTuple):
    ip_address: bytes
    port: int

    def __str__(self) -> str:
        return f"{inet_ntoa(self.ip_address)}:{self.port}"

    @classmethod
    def from_enr(self, enr: ENRAPI) -> "Endpoint":
        try:
            ip_address = enr[IP_V4_ADDRESS_ENR_KEY]
            port = enr[UDP_PORT_ENR_KEY]
        except KeyError as err:
            raise MissingEndpointFields(f"Missing endpoint address information: {err}")

        return Endpoint(ip_address, port)
