import pytest

from eth_enr.tools.factories import ENRFactory
import trio

from ddht.tools.factories.content import ContentFactory
from ddht.tools.utp import connection_pair


@pytest.mark.trio
async def test_connection():
    enr_a = ENRFactory()
    enr_b = ENRFactory()
    async with connection_pair(enr_a, enr_b) as (conn_a, conn_b):
        await conn_a.send_all(b'test-payload')
        with trio.fail_after(2):
            result = await conn_b.receive_some(12)
        assert result == b'test-payload'

        content_b = ContentFactory(4096)
        await conn_a.send_all(content_b)

        # since this payload takes a few packets to transmit, we need to give
        # the loop a few rounds to buffer the data.
        for _ in range(50):
            await trio.lowlevel.checkpoint()

        with trio.fail_after(2):
            result_b = await conn_b.receive_some(4096)
        assert result_b == content_b


@pytest.mark.trio
async def test_connection_bitrate():
    enr_a = ENRFactory()
    enr_b = ENRFactory()
    async with trio.open_nursery() as nursery:
        async with connection_pair(enr_a, enr_b) as (conn_a, conn_b):
            content_size = 10 * 1024 * 1024
            content_10mb = ContentFactory(content_size)

            done = trio.Event()

            async def _do_send():
                await conn_a.send_all(content_10mb)

            async def _do_read():
                read_byte_count = 0
                while read_byte_count < content_size:
                    to_read = content_size - read_byte_count
                    data = await conn_b.receive_some(to_read)
                    read_byte_count += len(data)

                done.set()

            nursery.start_soon(_do_send)
            nursery.start_soon(_do_read)

            start_at = trio.current_time()
            await done.wait()
            end_at = trio.current_time()

            elapsed = end_at - start_at
            bitrate = (content_size * 8 / 1024 / 1024) / elapsed
            byterate = (content_size / 1024 / 1024) / elapsed
            conn_a.logger.info("RESULT: elapsed=%0.2f  mbps=%0.2f  mBps=%0.2f", elapsed, bitrate, byterate)
