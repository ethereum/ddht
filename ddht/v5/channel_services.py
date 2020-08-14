import logging
from typing import NamedTuple

from async_service import ManagerAPI, as_service
from eth_utils import ValidationError
from trio.abc import ReceiveChannel, SendChannel

from ddht.datagram import InboundDatagram, OutboundDatagram
from ddht.endpoint import Endpoint
from ddht.v5.packets import Packet, decode_packet


#
# Data structures
#
class InboundPacket(NamedTuple):
    packet: Packet
    sender_endpoint: Endpoint

    def __str__(self) -> str:
        return f"{self.__class__.__name__}[{self.packet.__class__.__name__}]"


class OutboundPacket(NamedTuple):
    packet: Packet
    receiver_endpoint: Endpoint

    def __str__(self) -> str:
        return f"{self.__class__.__name__}[{self.packet.__class__.__name__}]"


#
# Packet encoding/decoding
#
@as_service
async def PacketDecoder(
    manager: ManagerAPI,
    inbound_datagram_receive_channel: ReceiveChannel[InboundDatagram],
    inbound_packet_send_channel: SendChannel[InboundPacket],
) -> None:
    """Decodes inbound datagrams to packet objects."""
    logger = logging.getLogger("ddht.v5.channel_services.PacketDecoder")

    async with inbound_datagram_receive_channel, inbound_packet_send_channel:
        async for datagram, endpoint in inbound_datagram_receive_channel:
            try:
                packet = decode_packet(datagram)
                logger.debug(
                    f"Successfully decoded {packet.__class__.__name__} from {endpoint}"
                )
            except ValidationError:
                logger.warning(
                    f"Failed to decode a packet from {endpoint}", exc_info=True
                )
            else:
                await inbound_packet_send_channel.send(InboundPacket(packet, endpoint))


@as_service
async def PacketEncoder(
    manager: ManagerAPI,
    outbound_packet_receive_channel: ReceiveChannel[OutboundPacket],
    outbound_datagram_send_channel: SendChannel[OutboundDatagram],
) -> None:
    """Encodes outbound packets to datagrams."""
    logger = logging.getLogger("ddht.v5.channel_services.PacketEncoder")

    async with outbound_packet_receive_channel, outbound_datagram_send_channel:
        async for packet, endpoint in outbound_packet_receive_channel:
            outbound_datagram = OutboundDatagram(packet.to_wire_bytes(), endpoint)
            logger.debug(f"Encoded {packet.__class__.__name__} for {endpoint}")
            await outbound_datagram_send_channel.send(outbound_datagram)
