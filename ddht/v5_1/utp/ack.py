import bisect
import collections
from typing import Iterable, Optional, Tuple

from eth_utils import to_tuple, get_extended_debug_logger


class AckTracker:
    _ack_nr: Optional[int]

    __slots__ = ('_ack_nr', '_buffer', 'logger')

    def __init__(self) -> None:
        self.logger = get_extended_debug_logger('ddht.AckTracker')

        self._ack_nr = None
        self._buffer = collections.deque()

    @property
    def ack_nr(self) -> int:
        if self._ack_nr is None:
            raise AttributeError("`ack_nr` not yet initialized")
        else:
            return self._ack_nr

    def ack(self, seq_nr: int) -> True:
        if self._ack_nr is None:
            self._ack_nr = seq_nr
            return

        if seq_nr == self._ack_nr + 1:
            self._ack_nr += 1
        elif seq_nr > self._ack_nr and seq_nr not in self._buffer:
            bisect.insort(self._buffer, seq_nr)
        elif seq_nr < self._ack_nr + 1:
            # already acked at some point in the past...
            pass
        else:
            raise Exception("Invariant")

        while self._buffer and self._buffer[0] == self._ack_nr + 1:
            self._buffer.popleft()
            self._ack_nr += 1

    @property
    def selective_acks(self) -> Tuple[int, ...]:
        return tuple(self._buffer)

    @property
    @to_tuple
    def missing_seq_nr(self) -> Iterable[int]:
        if not self._buffer:
            return

        all_seq_nr = set(range(self.ack_nr + 1, self._buffer[-1]))
        acked_seq_nr = set(self._buffer)
        unacked = all_seq_nr - acked_seq_nr
        yield from sorted(unacked)
