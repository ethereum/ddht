from typing import Tuple

from ddht.constants import DISCOVERY_MAX_PACKET_SIZE

ALEXANDRIA_PROTOCOL_ID = b"alexandria"

DEFAULT_BOOTNODES: Tuple[str, ...] = (
    "enr:-IS4QDZscd3qpOw_l86T6GjpRZ82r226SZJDCP7-omblk3O5UIIx6vESBpI3G2ipmJwbfU_lh4oBdLHQ6OokswcaIvQDgmlkgnY0gmlwhK3_32GJc2VjcDI1NmsxoQI5dQz6gpFLH57j0OCVEatTsgVpB7J9RYgvkn-LHzcAwYN1ZHCCdl8",  # noqa: E501
    "enr:-IS4QFoF5kk9l4xQrXDJvaGcKfkQUwLnJCny0u73FvkfZUmHSjBJAvG34Efr19MOqzkPqK5D7-NF5f7kj8rcTsLcPGIDgmlkgnY0gmlwhC1PSECJc2VjcDI1NmsxoQKNsjWTyTWNLBG0DtW_ycHCUo2XL29zLN0bCAs4ykcZVoN1ZHCCdl8",  # noqa: E501
)

# 1 gigabyte
GB = 1024 * 1024 * 1024  # 2**30


# All of the powers of two
POWERS_OF_TWO = tuple(2 ** n for n in range(256))

# Safe upper bound for the raw payload of an alexandria packet.  We expect the following overhead:
#
# - 23 bytes for the packet header
# - up-to 34 bytes of packet overhead.
# - up-to 4 bytes for request_id
# - 10 bytes for the `protocol_id`
# - 1 byte for the alexandria message type
# - up-to 10 bytes for RLP encoding overhead.
#
MAX_PAYLOAD_SIZE = DISCOVERY_MAX_PACKET_SIZE - 90


# One hour in seconds
ONE_HOUR = 60 * 60
