import io

from hypothesis import given, strategies as st
import pytest

from ddht.v5_1.utp.data_buffer import DataBuffer


@given(data=st.data())
@pytest.mark.trio
async def test_data_buffer(data):
    chunks = data.draw(
        st.lists(
            st.binary(min_size=1, max_size=16),
            min_size=1,
            max_size=16,
        ).map(tuple)
    )
    payload = b''.join(chunks)

    reads = []
    while True:
        remaining = len(payload) - sum(reads)
        assert remaining >= 0
        if remaining == 0:
            break
        size = data.draw(st.integers(min_value=1, max_value=remaining))
        reads.append(size)

    buffer = DataBuffer()

    await buffer.write(chunks)

    payload_io = io.BytesIO(payload)

    for size in reads:
        expected = payload_io.read(size)
        assert len(expected) == size
        actual = await buffer.receive_some(size)
        assert actual == expected
