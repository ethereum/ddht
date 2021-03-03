from abc import ABC, abstractmethod
from enum import IntEnum
import io
import struct
from typing import NamedTuple, Iterable, Tuple, Sequence, Any, Optional

from async_service import Service
from eth_utils import to_tuple
from eth_utils.toolz import sliding_window
import trio

from ddht._utils import caboose
from ddht.exceptions import DecodingError
from ddht.v5_1.utp.abc import UTPAPI


HEADER_BYTES = 20


class Extension(ABC):
    id: int
    data: bytes

    @property
    @abstractmethod
    def length(self) -> int:
        ...

    @property
    @abstractmethod
    def __eq__(self, other: Any) -> bool:
        ...


class SelectiveAck(Extension):
    id: int = 1

    def __init__(self, data: bytes) -> None:
        self.data = data

    @property
    def length(self) -> int:
        return len(self.data)

    def __eq__(self, other: Any) -> bool:
        if type(self) is not type(other):
            return False
        return self.data == other.data


class UnknownExtension(Extension):
    id: int

    def __init__(self, extension_id: int, data: bytes) -> None:
        self.id = extension_id
        self.data = data

    @property
    def length(self) -> int:
        return len(self.data)

    def __eq__(self, other: Any) -> bool:
        if type(self) is not type(other):
            return False
        return self.data == other.data and self.id == other.id


def decode_extensions(first_extension_type: int,
                      payload: bytes,
                      ) -> Tuple[Tuple[Extension, ...], int]:
    extensions = _decode_extensions(first_extension_type, payload)
    offset = sum(2 + extension.length for extension in extensions)
    return extensions, offset


def encode_extensions(extensions: Sequence[Extension]) -> bytes:
    return b''.join(_encode_extensions(extensions))


def _encode_extensions(extensions: Sequence[Extension]) -> Iterable[bytes]:
    if not extensions:
        return

    for extension, next_extension in sliding_window(2, caboose(extensions, None)):
        if next_extension is None:
            next_extension_id = 0
        else:
            next_extension_id = next_extension.id

        yield b''.join((
            next_extension_id.to_bytes(1, 'big'),
            extension.length.to_bytes(1, 'big'),
            extension.data,
        ))


@to_tuple
def _decode_extensions(first_extension_type: int, payload: bytes) -> Iterable[Extension]:
    extension_type = first_extension_type
    offset = 0

    while extension_type != 0:
        next_extension_type = payload[offset]
        extension_length = payload[offset + 1]
        extension_data = payload[offset + 2:offset + 2 + extension_length]

        if extension_type == 1:
            yield SelectiveAck(extension_data)
        else:
            yield UnknownExtension(extension_type, extension_data)

        extension_type = next_extension_type
        offset += (2 + extension_length)


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
    extensions: Tuple[Extension, ...]
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


class ConnectionInfo(NamedTuple):
    send_id: int
    receive_id: int


class UTPSendStream(trio.abc.SendStream):
    outbound_receive: trio.abc.ReceiveChannel

    def __init__(self) -> None:
        (
            self._outbound_send,
            self.outbound_receive,
        ) = trio.open_memory_channel[Packet](0)

    async def send_all(self, data: bytes) -> None:
        await self._outbound_send.send(data)


class UTPReceiveStream(trio.abc.ReceiveStream):
    inbound_send: trio.abc.SendChannel

    def __init__(self) -> None:
        self._buffer = io.BytesIO()

        (
            self.inbound_send,
            self._inbound_receive,
        ) = trio.open_memory_channel[Packet](256)

    async def receive_some(self, max_bytes: Optional[int] = None) -> bytes:
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


async def run_send_stream(stream: UTPSendStream,
                          packet_send: trio.abc.SendChannel[Packet],
                          ) -> None:
    sequence_number = 0
    last_acked_number = 0

    async with stream.outbound_receive as outbound_receive:
        async for payload in outbound_receive:
            pass


async def run_receive_stream(stream: UTPReceiveStream,
                             packet_receive: trio.abc.ReceiveStream[Packet],
                             ) -> None:
    sequence_number = 0
    last_acked_number = 0

    async with stream.inbound_send as inbound_send:
        async for packet in packet_receive:
            if packet
            if packet.header.type is PacketType.DATA:



async def run_stream(stream: trio.StapledStream) -> None:
    async with trio.open_nursery() as nursery:
        nursery.start_soon(run_send_stream, stream.send_stream)
        nursery.start_soon(run_receive_stream, stream.receive_stream)


class UTP(Service, UTPAPI):
    protocol_id = b'utp'

    def __init__(self, network: NetworkAPI) -> None:
        self.network = network
        self._connections: Dict[int, UTPStream] = {}

    @asynccontextmanager
    async def open_connection(self,
                              connection_id: int,
                              ) -> AsyncIterator[trio.abc.HalfCloseableStream]:
        ...

    @asynccontextmanager
    async def receive_connection(self,
                                 connection_id: int,
                                 ) -> AsyncContextManager[trio.abc.HalfCloseableStream]:
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

    async def _handle_packet(self, packet: Packet) -> None:
        ...
