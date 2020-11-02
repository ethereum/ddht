from typing import Tuple

ALEXANDRIA_PROTOCOL_ID = b"alexandria"

DEFAULT_BOOTNODES: Tuple[str, ...] = ()

# 1 gigabyte
GB = 1024 * 1024 * 1024  # 2**30


# All of the powers of two
POWERS_OF_TWO = tuple(2 ** n for n in range(256))
