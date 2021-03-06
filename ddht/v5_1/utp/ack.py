import bisect
import collections
from typing import Tuple, Iterable, Optional

from eth_utils import to_tuple


class AckTracker:
    _ack_nr: Optional[int]

    __slots__ = ('_ack_nr', 'ack_buffer')

    def __init__(self) -> None:
        self._ack_nr = None
        self.ack_buffer = collections.deque()

    @property
    def ack_nr(self) -> int:
        if self._ack_nr is None:
            raise AttributeError("`ack_nr` not yet initialized")
        else:
            return self._ack_nr

    @to_tuple
    def ack(self, seq_nr: int) -> Iterable[int]:
        if self._ack_nr is None:
            self._ack_nr = seq_nr
        elif seq_nr == self._ack_nr + 1:
            self._ack_nr += 1
            yield seq_nr
        elif seq_nr > self._ack_nr and seq_nr not in self.ack_buffer:
            bisect.insort(self.ack_buffer, seq_nr)
        elif seq_nr < self._ack_nr + 1:
            # already acked at some point in the past...
            pass
        else:
            raise Exception("Invariant")

        while self.ack_buffer and self.ack_buffer[0] == self._ack_nr + 1:
            self._ack_nr += 1
            yield self.ack_buffer.popleft()

        yield from self.ack_buffer

    @property
    def acked(self) -> Tuple[int]:
        return tuple(self.ack_buffer)
