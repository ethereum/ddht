import bisect
import collections
from typing import NamedTuple, Set, Deque, Iterable

from eth_utils import to_tuple, get_extended_debug_logger


class Segment(NamedTuple):
    seq_nr: int
    data: bytes


class DataCollator:
    seq_nr: int

    __slots__ = ('logger', 'seq_nr', '_buffer', '_buffer_seqs')

    def __init__(self, seq_nr: int) -> None:
        self.logger = get_extended_debug_logger('ddht.utp.DataCollator')
        self.seq_nr = seq_nr
        self._buffer: Deque[Segment] = collections.deque()
        self._buffer_seqs: Set[int] = set()

    @to_tuple
    def collate(self, segment: Segment) -> Iterable[bytes]:
        if segment.seq_nr in self._buffer_seqs or segment.seq_nr < self.seq_nr:
            self.logger.debug('Already processed: seq_nr=%d', segment.seq_nr)
            return

        if segment.seq_nr == self.seq_nr:
            self.logger.debug('Processing new segment: seq_nr=%d', segment.seq_nr)
            # this is the next packet, yield it directly
            yield segment.data
            self.seq_nr += 1
        elif segment.seq_nr > self.seq_nr:
            self.logger.debug('Buffering future segment: seq_nr=%d', segment.seq_nr)
            bisect.insort(self._buffer, segment)
            self._buffer_seqs.add(segment.seq_nr)
        else:
            raise Exception("Invariant")

        while self._buffer and self._buffer[0].seq_nr == self.seq_nr:
            buffered_segment = self._buffer.popleft()
            self.logger.debug('Processing buffered segment: seq_nr=%d', buffered_segment.seq_nr)
            self._buffer_seqs.remove(buffered_segment.seq_nr)
            self.seq_nr += 1
            yield buffered_segment.data
