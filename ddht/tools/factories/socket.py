import collections
from typing import Deque

from ddht._utils import get_open_port

RECENT_PORTS: Deque[int] = collections.deque(maxlen=256)


def robust_get_open_port() -> int:
    while True:
        port = get_open_port()
        if port not in RECENT_PORTS:
            break
    RECENT_PORTS.appendleft(port)
    return port
