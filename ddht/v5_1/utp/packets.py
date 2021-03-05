from enum import IntEnum
import struct
from typing import Iterable, NamedTuple, Tuple

from eth_utils import to_tuple

from ddht.exceptions import DecodingError
from ddht.v5_1.utp.abc import ExtensionAPI
from ddht.v5_1.utp.extensions import encode_extensions, decode_extensions, SelectiveAck


HEADER_BYTES = 20

MAX_PACKET_DATA = 1100


class PacketType(IntEnum):
    # data packet
    DATA = 0

    # signal last packet and close connection
    FIN = 1

    # ack packets
    STATE = 2

    # force-close a connection
    RESET = 3

    # initiate a new connection
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

    @property
    @to_tuple
    def acks(self) -> Iterable[int]:
        yield self.ack_nr

        for extension in self.extensions:
            if isinstance(extension, SelectiveAck):
                yield from extension.get_acks(self.ack_nr)

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

    def __str__(self) -> str:
        return (
            f"Packet(conn_id={self.header.connection_id}, "
            f"seq_nr={self.header.seq_nr}, ack_nr={self.header.ack_nr})"
        )

    def encode(self) -> bytes:
        header_bytes = self.header.encode()
        return header_bytes + self.data

    @classmethod
    def decode(cls, payload: bytes) -> 'Packet':
        header, data_offset = PacketHeader.decode(payload)
        data = payload[data_offset:]

        return cls(header, data)
