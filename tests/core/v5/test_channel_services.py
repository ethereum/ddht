from async_service import background_trio_service
import pytest
import trio

from ddht.tools.factories.discovery import AuthTagPacketFactory, EndpointFactory
from ddht.v5.channel_services import (
    IncomingDatagram,
    OutgoingPacket,
    PacketDecoder,
    PacketEncoder,
)


@pytest.mark.trio
async def test_packet_decoder():
    datagram_send_channel, datagram_receive_channel = trio.open_memory_channel(1)
    packet_send_channel, packet_receive_channel = trio.open_memory_channel(1)

    service = PacketDecoder(datagram_receive_channel, packet_send_channel)
    async with background_trio_service(service):
        packet = AuthTagPacketFactory()
        sender_endpoint = EndpointFactory()
        await datagram_send_channel.send(
            IncomingDatagram(
                datagram=packet.to_wire_bytes(), sender_endpoint=sender_endpoint,
            )
        )

        with trio.fail_after(0.5):
            incoming_packet = await packet_receive_channel.receive()

        assert incoming_packet.packet == packet
        assert incoming_packet.sender_endpoint.ip_address == sender_endpoint.ip_address
        assert incoming_packet.sender_endpoint.port == sender_endpoint.port


@pytest.mark.trio
async def test_packet_decoder_error():
    datagram_send_channel, datagram_receive_channel = trio.open_memory_channel(1)
    packet_send_channel, packet_receive_channel = trio.open_memory_channel(1)

    service = PacketDecoder(datagram_receive_channel, packet_send_channel)
    async with background_trio_service(service):
        # send invalid packet
        await datagram_send_channel.send(
            IncomingDatagram(
                datagram=b"not a valid packet", sender_endpoint=EndpointFactory(),
            )
        )

        # send valid packet
        packet = AuthTagPacketFactory()
        sender_endpoint = EndpointFactory()
        await datagram_send_channel.send(
            IncomingDatagram(
                datagram=packet.to_wire_bytes(), sender_endpoint=sender_endpoint,
            )
        )

        # ignore the invalid one, only receive the valid one
        with trio.fail_after(0.5):
            incoming_packet = await packet_receive_channel.receive()

        assert incoming_packet.packet == packet
        assert incoming_packet.sender_endpoint.ip_address == sender_endpoint.ip_address
        assert incoming_packet.sender_endpoint.port == sender_endpoint.port


@pytest.mark.trio
async def test_packet_encoder():
    packet_send_channel, packet_receive_channel = trio.open_memory_channel(1)
    datagram_send_channel, datagram_receive_channel = trio.open_memory_channel(1)

    service = PacketEncoder(packet_receive_channel, datagram_send_channel)
    async with background_trio_service(service):
        receiver_endpoint = EndpointFactory()
        outgoing_packet = OutgoingPacket(
            packet=AuthTagPacketFactory(), receiver_endpoint=receiver_endpoint,
        )
        await packet_send_channel.send(outgoing_packet)

        with trio.fail_after(0.5):
            outgoing_datagram = await datagram_receive_channel.receive()

        assert outgoing_datagram.datagram == outgoing_packet.packet.to_wire_bytes()
        assert (
            outgoing_datagram.receiver_endpoint.ip_address
            == receiver_endpoint.ip_address
        )
        assert outgoing_datagram.receiver_endpoint.port == receiver_endpoint.port
