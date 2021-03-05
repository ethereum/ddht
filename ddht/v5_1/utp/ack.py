import bisect
import collections
from typing import Tuple, Iterable

from eth_utils import to_tuple


class AckTracker:
    def __init__(self) -> None:
        self.ack_nr = 0
        self.ack_buffer = collections.deque()

    @to_tuple
    def ack(self, seq_nr: int) -> Iterable[int]:
        if seq_nr == self.ack_nr:
            self.ack_nr += 1
            yield seq_nr
        elif seq_nr > self.ack_nr and seq_nr not in self.ack_buffer:
            bisect.insort(self.ack_buffer, seq_nr)
        elif seq_nr < self.ack_nr:
            # already acked at some point in the past...
            pass
        else:
            raise Exception("Invariant")

        while self.ack_buffer and self.ack_buffer[0] == self.ack_nr:
            self.ack_nr += 1
            yield self.ack_buffer.popleft()

        yield from self.ack_buffer

    @property
    def acked(self) -> Tuple[int]:
        return tuple(self.ack_buffer)
