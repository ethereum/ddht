import pytest

from eth_enr.tools.factories import ENRFactory
import trio

from ddht.tools.factories.content import ContentFactory
from ddht.tools.utp import connection_pair


async def _do_data_transfer(sender, receiver, payload, chunk_size=1024):
    await sender.send_all(payload)

    result = b''
    while len(result) < len(payload):
        with trio.fail_after(2):
            result += await receiver.receive_some(1024)
            assert result == payload[:len(result)]

    assert result == payload


@pytest.mark.trio
async def test_connection_data_transfer_outbound():
    enr_a = ENRFactory()
    enr_b = ENRFactory()
    async with connection_pair(enr_a, enr_b) as (conn_a, conn_b):
        await _do_data_transfer(conn_a, conn_b, ContentFactory(4096))


@pytest.mark.trio
async def test_connection_data_transfer_inbound():
    enr_a = ENRFactory()
    enr_b = ENRFactory()
    async with connection_pair(enr_a, enr_b) as (conn_a, conn_b):
        await _do_data_transfer(conn_b, conn_a, ContentFactory(4096))


@pytest.mark.trio
async def test_connection_data_transfer_bidirectional():
    enr_a = ENRFactory()
    enr_b = ENRFactory()
    async with connection_pair(enr_a, enr_b) as (conn_a, conn_b):
        with trio.fail_after(10):
            async with trio.open_nursery() as nursery:
                nursery.start_soon(_do_data_transfer, conn_b, conn_a, ContentFactory(32 * 1024))
                nursery.start_soon(_do_data_transfer, conn_a, conn_b, ContentFactory(32 * 1024))


@pytest.mark.trio
async def test_connection_retransmission_of_lost_packets():
    enr_a = ENRFactory()
    enr_b = ENRFactory()

    async with connection_pair(enr_a, enr_b) as (conn_a, conn_b):
        payload = ContentFactory(32 * 1024)
        await conn_a.send_all(payload)

        with trio.fail_after(10):
            result = await conn_b.receive_some(1024)

        with trio.fail_after(10):
            lost_packet = await conn_a.outbound_packet_receive.receive()
            conn_a.logger.info('LOST PACKET: packet=%s', lost_packet)

        with trio.fail_after(10):
            for _ in range(100):
                chunk = await conn_b.receive_some(1024)
                result += chunk

                assert result == payload[:len(result)]
                conn_a.logger.info("REMAINING: num-bytes=%d", len(payload) - len(result))
                if len(result) >= len(payload):
                    break

        assert result == payload


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
            conn_a.logger.info(
                "RESULT: elapsed=%0.2f  mbps=%0.2f  mBps=%0.2f",
                elapsed,
                bitrate,
                byterate,
            )
