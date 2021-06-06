import pytest

from ddht.v5_1.utp.utp import (
    Packet,
    PacketHeader,
    PacketType,
    SelectiveAck,
    UnknownExtension,
)


@pytest.mark.parametrize(
    'data,extensions',
    (
        (b'', ()),
        (b'test-data', ()),
        (b'', (UnknownExtension(2, b''), )),
        (b'', (UnknownExtension(2, b'test-extension-data'),)),
        (b'', (SelectiveAck(b'\x01\x02'),)),
        (b'', (SelectiveAck(b'\x02\x01'), UnknownExtension(2, b'test-extension-data'))),
        (b'', (UnknownExtension(2, b'test-extension-data'), SelectiveAck(b'\x02\x01'))),
    )
)
def test_utp_packet_encoding_and_decoding(data, extensions):
    packet = Packet(
        header=PacketHeader(
            type=PacketType.DATA,
            version=1,
            extensions=extensions,
            connection_id=1234,
            timestamp_microseconds=1234567890,
            timestamp_difference_microseconds=54321,
            wnd_size=65536,
            seq_nr=0,
            ack_nr=1,
        ),
        data=data,
    )

    encoded_packet = packet.encode()
    result = Packet.decode(encoded_packet)

    assert result == packet
