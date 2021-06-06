import collections
from typing import Sequence

import trio


class DataBuffer:
    lock: trio.Condition

    def __init__(self) -> None:
        self._buffer = collections.deque()
        self._lock = trio.Condition()

    async def write(self, chunks: Sequence[bytes]) -> None:
        async with self._lock:
            self._buffer.extend(chunks)
            self._lock.notify_all()

    async def receive_some(self, max_bytes: int) -> bytes:
        async with self._lock:
            # If the buffer is empty then we block, reading until we have some
            # data in the buffer.
            while not self._buffer:
                await self._lock.wait()

            data = b''

            while self._buffer and len(data) < max_bytes:
                data += self._buffer.popleft()

            result = data[:max_bytes]
            remainder = data[max_bytes:]

            if remainder:
                self._buffer.appendleft(remainder)

            return result
