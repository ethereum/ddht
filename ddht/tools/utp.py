from contextlib import asynccontextmanager
import logging
import secrets
from typing import AsyncIterator, Tuple

from async_service import background_trio_service
from eth_enr import ENRAPI
import trio

from ddht.v5_1.utp.connection import Connection


logger = logging.getLogger('ddht.tools')


async def staple(name: str,
                 send_channel: trio.abc.SendChannel,
                 receive_channel: trio.abc.ReceiveChannel):
    async for packet in receive_channel:
        logger.debug('[%s]: packet=%s', name, packet)
        await send_channel.send(packet)


@asynccontextmanager
async def connection_pair(enr_a: ENRAPI,
                          enr_b: ENRAPI,
                          ) -> AsyncIterator[Tuple[Connection, Connection]]:
    async with trio.open_nursery() as nursery:
        base_connection_id = secrets.randbelow(65536)
        connection_a = Connection(
            node_id=enr_a.node_id,
            send_id=base_connection_id + 1,
            receive_id=base_connection_id,
        )
        connection_b = Connection(
            node_id=enr_b.node_id,
            send_id=connection_a.receive_id,
            receive_id=connection_a.send_id,
        )

        nursery.start_soon(
            staple,
            'b-to-a',
            connection_a.inbound_packet_send,
            connection_b.outbound_packet_receive,
        )
        nursery.start_soon(
            staple,
            'a-to-b',
            connection_b.inbound_packet_send,
            connection_a.outbound_packet_receive,
        )

        async with background_trio_service(connection_a):
            async with background_trio_service(connection_b):
                yield (connection_a, connection_b)

        nursery.cancel_scope.cancel()
