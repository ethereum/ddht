import itertools

from eth_utils.toolz import take
from hypothesis import given, settings
from hypothesis import strategies as st

from ddht.v5_1.utp.collator import DataCollator, Segment
from ddht.tools.factories.content import ContentFactory


@settings(
    max_examples=20,
)
@given(
    segment_sizes=st.lists(
        st.integers(min_value=1, max_value=1024),
        min_size=1,
        max_size=6,
    ),
)
def test_data_collator_full_ordering(segment_sizes):
    data_length = sum(segment_sizes)
    data = ContentFactory(data_length)
    data_iter = iter(data)

    segments = tuple(
        Segment(idx, bytes(take(size, data_iter)))
        for idx, size in enumerate(segment_sizes)
    )

    assert bytes(data_iter) == b''

    for segment_ordering in itertools.permutations(segments):
        collator = DataCollator()
        results = tuple(itertools.chain(*(
            collator.collate(segment)
            for segment in segment_ordering
        )))
        result = bytes(itertools.chain(*(chunk for chunk in results)))
        assert result == data


def test_data_collator_duplicate_sequence_numbers():
    # TODO
    pass
