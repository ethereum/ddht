from typing import Tuple

from ddht.constants import DISCOVERY_MAX_PACKET_SIZE

ALEXANDRIA_PROTOCOL_ID = b"alexandria"

DEFAULT_BOOTNODES: Tuple[str, ...] = ()

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


# One Megabyte
MB = 1024 * 1024


# Default max bytes for "commons" storage
DEFAULT_COMMONS_STORAGE_SIZE = 100 * MB


# Default maximum number of advertisements
DEFAULT_MAX_ADVERTISEMENTS = 1048576


MAX_RADIUS = 2 ** 256 - 1
