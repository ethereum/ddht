import itertools

from eth_utils.toolz import sliding_window
from hypothesis import given, settings
from hypothesis import strategies as st
import rlp

from ddht.constants import DISCOVERY_MAX_PACKET_SIZE, MAX_ENR_SIZE
from ddht.enr import ENRSedes, partition_enrs
from ddht.tools.factories.enr import ENRFactory


@settings(max_examples=50, deadline=1000)
@given(
    num_enr_records=st.integers(min_value=1, max_value=100),
    max_payload_size=st.integers(
        min_value=MAX_ENR_SIZE, max_value=DISCOVERY_MAX_PACKET_SIZE
    ),
)
def test_enr_partitioning(num_enr_records, max_payload_size):
    enrs = ENRFactory.create_batch(num_enr_records)
    batches = partition_enrs(enrs, max_payload_size)

    assert sum(len(batch) for batch in batches) == len(enrs)
    assert set(itertools.chain(*batches)) == set(enrs)

    for batch in batches:
        encoded_batch = rlp.encode(batch, sedes=rlp.sedes.CountableList(ENRSedes))
        assert len(encoded_batch) <= max_payload_size

    for batch, next_batch in sliding_window(2, batches):
        overfull_batch = tuple(batch) + (next_batch[0],)
        encoded_batch = rlp.encode(
            overfull_batch, sedes=rlp.sedes.CountableList(ENRSedes)
        )
        assert len(encoded_batch) > max_payload_size
