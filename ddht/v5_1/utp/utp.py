from contextlib import asynccontextmanager
from enum import IntEnum
import io
import struct
from typing import NamedTuple, Tuple, AsyncIterator, Dict

from async_service import Service
import trio

from ddht.exceptions import DecodingError
from ddht.v5_1.abc import NetworkAPI
from ddht.v5_1.messages import TalkRequestMessage
from ddht.v5_1.utp.abc import UTPAPI, ExtensionAPI
from ddht.v5_1.utp.extensions import encode_extensions, decode_extensions


HEADER_BYTES = 20

MAX_PACKET_DATA = 1100


class PacketType(IntEnum):
    DATA = 0
    FIN = 1
    STATE = 2
    RESET = 3
    SYN = 4


class PacketHeader(NamedTuple):
    """
    0       4       8               16              24              32
    +-------+-------+---------------+---------------+---------------+
    | type  | ver   | extension     | connection_id                 |
    +-------+-------+---------------+---------------+---------------+
    | timestamp_microseconds                                        |
    +---------------+---------------+---------------+---------------+
    | timestamp_difference_microseconds                             |
    +---------------+---------------+---------------+---------------+
    | wnd_size                                                      |
    +---------------+---------------+---------------+---------------+
    | seq_nr                        | ack_nr                        |
    +---------------+---------------+---------------+---------------+
    """
    type: PacketType
    version: int
    extensions: Tuple[ExtensionAPI, ...]
    connection_id: bytes
    timestamp_microseconds: int
    timestamp_difference_microseconds: int
    wnd_size: int
    seq_nr: int
    ack_nr: int

    def encode(self) -> bytes:
        packet_type_and_version = self.type ^ (self.version << 4)

        if self.extensions:
            first_extension_type = self.extensions[0].id
            extensions_tail = encode_extensions(self.extensions)
        else:
            first_extension_type = 0
            extensions_tail = b''

        main_header = struct.pack(
            '>BBHLLLHH',
            packet_type_and_version,
            first_extension_type,
            self.connection_id,
            self.timestamp_microseconds,
            self.timestamp_difference_microseconds,
            self.wnd_size,
            self.seq_nr,
            self.ack_nr,
        )
        return main_header + extensions_tail

    @classmethod
    def decode(cls, payload: bytes) -> Tuple['PacketHeader', int]:
        if len(payload) < HEADER_BYTES:
            raise DecodingError("Too short for header")

        packet_type_and_version = payload[0]
        packet_type = PacketType(packet_type_and_version & 0x0f)
        version = packet_type_and_version >> 4

        #
        # Manual Unpacking
        #
        # extension = payload[1:2]
        # connection_id = int.from_bytes(payload[2:4], 'big')
        # timestamp_microseconds = int.from_bytes(payload[4:8], 'big')
        # timestamp_difference_microseconds = int.from_bytes(payload[8:12], 'big')
        # wnd_size = int.from_bytes(payload[12:16], 'big')
        # seq_nr = int.from_bytes(payload[16:18], 'big')
        # ack_nr = int.from_bytes(payload[18:20], 'big')

        #
        # Struct based unpacking
        #
        (
            first_extension_type,
            connection_id,
            timestamp_microseconds,
            timestamp_difference_microseconds,
            wnd_size,
            seq_nr,
            ack_nr,
        ) = struct.unpack(
            '>BHLLLHH',
            payload[1:20],
        )

        if first_extension_type == 0:
            extensions = ()
            data_offset = HEADER_BYTES
        else:
            extensions, extensions_length = decode_extensions(
                first_extension_type,
                payload[HEADER_BYTES:],
            )
            data_offset = HEADER_BYTES + extensions_length

        header = PacketHeader(
            type=packet_type,
            version=version,
            extensions=extensions,
            connection_id=connection_id,
            timestamp_microseconds=timestamp_microseconds,
            timestamp_difference_microseconds=timestamp_difference_microseconds,
            wnd_size=wnd_size,
            seq_nr=seq_nr,
            ack_nr=ack_nr,
        )
        return header, data_offset


class Packet(NamedTuple):
    header: PacketHeader
    data: bytes

    def encode(self) -> bytes:
        header_bytes = self.header.encode()
        return header_bytes + self.data

    @classmethod
    def decode(cls, payload: bytes) -> 'Packet':
        header, data_offset = PacketHeader.decode(payload)
        data = payload[data_offset:]

        return cls(header, data)


class Connection(Service):
    def __init__(self,
                 send_id: int,
                 receive_id: int,
                 outbound_packet_send: trio.abc.SendChannel,
                 inbound_packet_receive: trio.abc.SendChannel,
                 ) -> None:
        self.send_id = send_id
        self.receive_id = receive_id

        self._outbound_packet_send = outbound_packet_send
        self._inbound_packet_receive = inbound_packet_receive

        self.seq_nr = 1
        self.ack_nr = None

        (
            self._outbound_data_send,
            self._outbound_data_receive,
        ) = trio.open_memory_channel[Packet](0)

        (
            self.inbound_data_send,
            self._inbound_data_receive,
        ) = trio.open_memory_channel[Packet](256)
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
