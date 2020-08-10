from rlp.sedes import Binary

from ddht.constants import IP_V4_SIZE, IP_V6_SIZE


#
# Custom sedes objects
#
class IPAddressSedes(Binary):  # type: ignore
    def __init__(self) -> None:
        super().__init__()

    def is_valid_length(self, length: int) -> bool:
        return length in (IP_V4_SIZE, IP_V6_SIZE)


ip_address_sedes = IPAddressSedes()
