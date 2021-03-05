import pytest

from eth_enr.tools.factories import ENRFactory
import trio

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
