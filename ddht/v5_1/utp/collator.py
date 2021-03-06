import bisect
import collections
from typing import NamedTuple, Set, Deque, Iterable

from eth_utils import ValidationError, to_tuple


class Segment(NamedTuple):
    seq_nr: int
    data: bytes


class DataCollator:
    seq_nr: int

    def __init__(self, seq_nr: int) -> None:
        self.seq_nr = seq_nr
        self._buffer: Deque[Segment] = collections.deque()
        self._buffer_seqs: Set[int] = set()

    @to_tuple
    def collate(self, segment: Segment) -> Iterable[bytes]:
        if segment.seq_nr in self._buffer_seqs:
            raise ValidationError("Invalid: segment=%s  reason=duplicate-seq-nr", segment)
        elif segment.seq_nr == self.seq_nr:
            # this is the next packet, yield it directly
            yield segment.data
            self.seq_nr += 1
        else:
            bisect.insort(self._buffer, segment)
            self._buffer_seqs.add(segment.seq_nr)

        while self._buffer and self._buffer[0].seq_nr == self.seq_nr:
            segment = self._buffer.popleft()
            self._buffer_seqs.remove(self.seq_nr)
            self.seq_nr += 1
            yield segment.data
