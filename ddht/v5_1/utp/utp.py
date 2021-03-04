from contextlib import asynccontextmanager
import io
from typing import AsyncIterator, Dict

from async_service import Service
import trio

from ddht.exceptions import DecodingError
from ddht.v5_1.abc import NetworkAPI
from ddht.v5_1.messages import TalkRequestMessage
from ddht.v5_1.utp.abc import UTPAPI
from ddht.v5_1.utp.packets import Packet, PacketHeader, MAX_PACKET_DATA, PacketType


class Connection(Service):
    outbound_packet_receive: trio.abc.ReceiveChannel[Packet]
    inbound_packet_send: trio.abc.SendChannel[Packet]

    def __init__(self,
                 send_id: int,
                 receive_id: int,
                 outbound_packet_send: trio.abc.SendChannel,
                 inbound_packet_receive: trio.abc.SendChannel,
                 ) -> None:
        self.send_id = send_id
        self.receive_id = receive_id

        (
            self._outbound_packet_send,
            self.outbound_packet_receive,
        ) = trio.open_memory_channel[Packet](256)
        (
            self.inbound_packet_send,
            self._inbound_packet_receive,
        ) = trio.open_memory_channel[Packet](256)

        self.seq_nr = 1
        self.ack_nr = None

        (
            self._outbound_data_send,
            self._outbound_data_receive,
        ) = trio.open_memory_channel[Packet](0)

        (
            self.inbound_data_send,
            self._inbound_data_receive,
        ) = trio.open_memory_channel[bytes](256)
        self._inbound_data_buffer = io.BytesIO()

    async def receive_some(self, max_bytes: int) -> bytes:
        self._buffer.seek(0)

        data = self._buffer.read(max_bytes)

        while len(data) < max_bytes:
            try:
                data += self._inbound_receive.receive_nowait()
            except trio.WouldBlock:
                if data:
                    break
                else:
                    data += await self._inbound_receive.receive()

        if len(data) > max_bytes:
            remainder = data[max_bytes:]
            data = data[:max_bytes]

            # The only way we can end up with `remainder` data is if we read
            # new data in over the stream.  In this case, we can know that
            # we've read all information from the buffer and that any extra
            # information should be written to the front of the buffer, and the
            # buffer truncated down to the new size of however much remainder
            # data was left.
            self._buffer.seek(0)
            self._buffer.write(remainder)
            self._buffer.truncate(len(remainder))
            self._buffer.seek(0)

        return data

    async def send_all(self, data: bytes) -> None:
        await self._outbound_data_send.send(data)

    async def run(self) -> None:
        self.manager.run_daemon_task(self._handle_inbound_packets)
        self.manager.run_daemon_task(self._handle_outbound_packets)

        await self.manager.wait_finished()

    #
    # Long lived packet handlers
    #
    async def _handle_inbound_packets(self) -> None:
        async with self._inbound_packet_receive as packet_receive:
            async for packet in packet_receive:
                if packet.header.type is PacketType.DATA:
                    # data packet
                    ...
                elif packet.header.type is PacketType.FIN:
                    # last packet
                    ...
                elif packet.header.type is PacketType.STATE:
                    # ack packet
                    ...
                elif packet.header.type is PacketType.RESET:
                    # hard termination
                    ...
                elif packet.header.type is PacketType.SYN:
                    # initiation of the connection...  probably needs to be handled higher up...
                    ...
                else:
                    raise Exception("Invariant: unknown packet type")

    async def _handle_outbound_packets(self) -> None:
        buffer = io.BytesIO()
        async with self._outbound_packet_send as packet_send:
            async with self._outbound_data_receive as data_receive:
                while self.manager.is_running:
                    if not buffer.tell():
                        # block until there is data available
                        buffer.write(await data_receive.receive())
                    else:
                        # yield to the event loop at least once per iteration
                        await trio.lowlevel.checkpoint()

                    # if there is more data immediately available and there is
                    # room in the packet then we should include it.
                    while buffer.tell() < MAX_PACKET_DATA:
                        try:
                            buffer.write(data_receive.receive_nowait())
                        except trio.WouldBlock:
                            break

                    buffer.seek(0)
                    data = buffer.read(MAX_PACKET_DATA)

                    remainder = buffer.read()
                    buffer.seek(0)
                    buffer.write(remainder)
                    buffer.truncate()

                    header = PacketHeader(...)
                    packet = Packet(header=header, data=data)

                    # TODO: congestion control goes here...
                    await packet_send.send(packet)


class UTP(Service, UTPAPI):
    protocol_id = b'utp'

    def __init__(self, network: NetworkAPI) -> None:
        self.network = network
        self._connections: Dict[int, Connection] = {}

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
            async for request in subscription:
                if request.message.protocol != self.protocol_id:
                    continue

                try:
                    packet = Packet.decode(request.message.payload)
                except DecodingError:
                    pass

                # handle the packet

    async def _handle_packet(self, packet: Packet) -> None:
        ...

    async def _manage_connection(self, connection: Connection) -> None:
        ...
