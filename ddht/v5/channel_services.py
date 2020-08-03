import logging
from typing import NamedTuple

from async_service import ManagerAPI, as_service
from eth_utils import ValidationError
from trio.abc import ReceiveChannel, SendChannel

from ddht.datagram import IncomingDatagram, OutgoingDatagram
from ddht.endpoint import Endpoint
from ddht.v5.packets import Packet, decode_packet


#
# Data structures
#
class IncomingPacket(NamedTuple):
    packet: Packet
    sender_endpoint: Endpoint

    def __str__(self) -> str:
        return f"{self.__class__.__name__}[{self.packet.__class__.__name__}]"


class OutgoingPacket(NamedTuple):
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
    incoming_datagram_receive_channel: ReceiveChannel[IncomingDatagram],
    incoming_packet_send_channel: SendChannel[IncomingPacket],
) -> None:
    """Decodes incoming datagrams to packet objects."""
    logger = logging.getLogger("ddht.v5.channel_services.PacketDecoder")

    async with incoming_datagram_receive_channel, incoming_packet_send_channel:
        async for datagram, endpoint in incoming_datagram_receive_channel:
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
                await incoming_packet_send_channel.send(
                    IncomingPacket(packet, endpoint)
                )


@as_service
async def PacketEncoder(
    manager: ManagerAPI,
    outgoing_packet_receive_channel: ReceiveChannel[OutgoingPacket],
    outgoing_datagram_send_channel: SendChannel[OutgoingDatagram],
) -> None:
    """Encodes outgoing packets to datagrams."""
    logger = logging.getLogger("ddht.v5.channel_services.PacketEncoder")

    async with outgoing_packet_receive_channel, outgoing_datagram_send_channel:
        async for packet, endpoint in outgoing_packet_receive_channel:
            outgoing_datagram = OutgoingDatagram(packet.to_wire_bytes(), endpoint)
            logger.debug(f"Encoded {packet.__class__.__name__} for {endpoint}")
            await outgoing_datagram_send_channel.send(outgoing_datagram)
