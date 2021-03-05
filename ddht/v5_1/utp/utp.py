from contextlib import asynccontextmanager
from typing import AsyncIterator, Dict, Tuple

from async_service import Service, background_trio_service
from eth_typing import NodeID
import trio

from ddht.exceptions import DecodingError
from ddht.v5_1.abc import NetworkAPI
from ddht.v5_1.messages import TalkRequestMessage
from ddht.v5_1.utp.abc import UTPAPI
from ddht.v5_1.utp.connection import Connection
from ddht.v5_1.utp.typing import ConnectionID
from ddht.v5_1.utp.packets import Packet


class UTP(Service, UTPAPI):
    protocol_id = b'utp'

    def __init__(self, network: NetworkAPI) -> None:
        self.network = network
        self._connections: Dict[Tuple[NodeID, ConnectionID], Connection] = {}

    @asynccontextmanager
    async def open_connection(self,
                              connection_id: int,
                              ) -> AsyncIterator[Connection]:
        ...

    @asynccontextmanager
    async def receive_connection(self,
                                 connection_id: int,
                                 ) -> AsyncIterator[Connection]:
        ...

    async def run(self) -> None:
        async with self.network.client.dispatcher.subscribe(
            TalkRequestMessage
        ) as subscription:
            connection: Connection

            async for request in subscription:
                if request.message.protocol != self.protocol_id:
                    continue

                try:
                    packet = Packet.decode(request.message.payload)
                except DecodingError:
                    self.logger.debug(
                        "Discarding Message: message=%s  reason=malformed",
                        request,
                    )
                    continue

                key = (request.sender_node_id, packet.header.connection_id)

                try:
                    connection = self._connections[key]
                except KeyError:
                    if packet.header.type is Packet.SYN:
                        connection = Connection(
                            node_id=request.sender_node_id,
                            send_id=packet.header.connection_id,
                            receive_id=packet.header.connection_id + 1,
                        )
                        self._connections[key] = connection
                        self.manager.run_task(self._manage_connection, connection)
                    else:
                        self.logger.debug(
                            "Discarding Packet: packet=%s  reason=no-connection",
                            packet,
                        )
                        continue

                try:
                    connection.inbound_packet_send.send_nowait(packet)
                except trio.WouldBlock:
                    self.logger.debug(
                        "Discarding Packet: packet=%s  reason=buffer-full",
                        packet,
                    )

    async def _manage_connection(self, connection: Connection) -> None:
        async with background_trio_service(connection):
            async with connection.outbound_packet_receive as outbound_packet_receive:
                async for packet in outbound_packet_receive:
                    payload = packet.encode()
                    endpoint = self.network.endpoint_for_node_id(connection.node_id)

                    await self.network.client.send_talk_request(
                        connection.node_id,
                        endpoint,
                        protocol_id=self.protocol_id,
                        payload=payload,
                    )
