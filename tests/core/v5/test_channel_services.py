from async_service import background_trio_service
import pytest
import trio

from ddht.tools.factories.discovery import AuthTagPacketFactory
from ddht.tools.factories.endpoint import EndpointFactory
from ddht.v5.channel_services import (
    InboundDatagram,
    OutboundPacket,
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
            InboundDatagram(
                datagram=packet.to_wire_bytes(), sender_endpoint=sender_endpoint,
            )
        )

        with trio.fail_after(0.5):
            inbound_packet = await packet_receive_channel.receive()

        assert inbound_packet.packet == packet
        assert inbound_packet.sender_endpoint.ip_address == sender_endpoint.ip_address
        assert inbound_packet.sender_endpoint.port == sender_endpoint.port


@pytest.mark.trio
async def test_packet_decoder_error():
    datagram_send_channel, datagram_receive_channel = trio.open_memory_channel(1)
    packet_send_channel, packet_receive_channel = trio.open_memory_channel(1)

    service = PacketDecoder(datagram_receive_channel, packet_send_channel)
    async with background_trio_service(service):
        # send invalid packet
        await datagram_send_channel.send(
            InboundDatagram(
                datagram=b"not a valid packet", sender_endpoint=EndpointFactory(),
            )
        )

        # send valid packet
        packet = AuthTagPacketFactory()
        sender_endpoint = EndpointFactory()
        await datagram_send_channel.send(
            InboundDatagram(
                datagram=packet.to_wire_bytes(), sender_endpoint=sender_endpoint,
            )
        )

        # ignore the invalid one, only receive the valid one
        with trio.fail_after(0.5):
            inbound_packet = await packet_receive_channel.receive()

        assert inbound_packet.packet == packet
        assert inbound_packet.sender_endpoint.ip_address == sender_endpoint.ip_address
        assert inbound_packet.sender_endpoint.port == sender_endpoint.port


@pytest.mark.trio
async def test_packet_encoder():
    packet_send_channel, packet_receive_channel = trio.open_memory_channel(1)
    datagram_send_channel, datagram_receive_channel = trio.open_memory_channel(1)

    service = PacketEncoder(packet_receive_channel, datagram_send_channel)
    async with background_trio_service(service):
        receiver_endpoint = EndpointFactory()
        outbound_packet = OutboundPacket(
            packet=AuthTagPacketFactory(), receiver_endpoint=receiver_endpoint,
        )
        await packet_send_channel.send(outbound_packet)

        with trio.fail_after(0.5):
            outbound_datagram = await datagram_receive_channel.receive()

        assert outbound_datagram.datagram == outbound_packet.packet.to_wire_bytes()
        assert (
            outbound_datagram.receiver_endpoint.ip_address
            == receiver_endpoint.ip_address
        )
        assert outbound_datagram.receiver_endpoint.port == receiver_endpoint.port
