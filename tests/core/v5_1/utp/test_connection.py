import pytest

from eth_enr.tools.factories import ENRFactory
import trio

from ddht.tools.factories.content import ContentFactory
from ddht.tools.utp import connection_pair
from ddht.v5_1.utp.connection import ConnectionStatus


@pytest.fixture
async def connections():
    enr_a = ENRFactory()
    enr_b = ENRFactory()
    async with connection_pair(enr_a, enr_b) as (conn_a, conn_b):
        yield (conn_a, conn_b)


async def _do_receive(receiver, size):
    result = b''
    while len(result) < size:
        with trio.fail_after(2):
            result += await receiver.receive_some(1024)
    return result


async def _do_data_transfer(sender, receiver, payload, chunk_size=1024):
    await sender.send_all(payload)

    result = await _do_receive(receiver, len(payload))

    assert result == payload


@pytest.mark.trio
async def test_connection_data_transfer_outbound(connections):
    conn_a, conn_b = connections

    await _do_data_transfer(conn_a, conn_b, ContentFactory(4096))


@pytest.mark.trio
async def test_connection_data_transfer_inbound(connections):
    conn_a, conn_b = connections

    await _do_data_transfer(conn_b, conn_a, ContentFactory(4096))


@pytest.mark.trio
async def test_connection_data_transfer_bidirectional(connections):
    conn_a, conn_b = connections

    with trio.fail_after(10):
        async with trio.open_nursery() as nursery:
            nursery.start_soon(_do_data_transfer, conn_b, conn_a, ContentFactory(32 * 1024))
            nursery.start_soon(_do_data_transfer, conn_a, conn_b, ContentFactory(32 * 1024))


@pytest.mark.trio
async def test_connection_retransmission_of_lost_packets(connections):
    conn_a, conn_b = connections

    payload = ContentFactory(32 * 1024)
    await conn_a.send_all(payload)

    with trio.fail_after(10):
        result = await conn_b.receive_some(1024)

    with trio.fail_after(10):
        lost_packet = await conn_a.outbound_packet_receive.receive()
        conn_a.logger.info('LOST PACKET: packet=%s', lost_packet)

    result += await _do_receive(conn_b, len(payload) - len(result))

    assert result == payload


async def _wait_status(conn, status, max_rounds=1024):
    for _ in range(max_rounds):
        if conn.status is status:
            break
        await trio.lowlevel.checkpoint()
    else:
        raise AssertionError(f"Connection never reached status: {status}")


@pytest.mark.trio
async def test_connection_finalization(connections):
    conn_a, conn_b = connections

    payload = ContentFactory(32 * 1024)
    await conn_a.send_all(payload)

    assert conn_a.status is ConnectionStatus.CONNECTED

    await conn_a.finalize()

    assert conn_a.status is ConnectionStatus.CLOSING

    result = await _do_receive(conn_b, len(payload))
    assert result == payload

    await _wait_status(conn_a, ConnectionStatus.CLOSED)
    await _wait_status(conn_b, ConnectionStatus.CLOSED)
