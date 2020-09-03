import logging
from typing import NamedTuple

from async_service import Service
from eth_typing import NodeID
from eth_utils import ValidationError
import trio
from trio.abc import ReceiveChannel, SendChannel

from ddht.datagram import InboundDatagram, OutboundDatagram
from ddht.endpoint import Endpoint
from ddht.exceptions import DecodingError, DecryptionError
from ddht.v5_1.packets import AnyPacket, decode_packet


#
# Data structures
#
class InboundEnvelope(NamedTuple):
    packet: AnyPacket
    sender_endpoint: Endpoint

    def __str__(self) -> str:
        return (
            f"{self.__class__.__name__}"
            f"(packet={self.packet}, sender={self.sender_endpoint})"
        )


class OutboundEnvelope(NamedTuple):
    packet: AnyPacket
    receiver_endpoint: Endpoint

    def __str__(self) -> str:
        return (
            f"{self.__class__.__name__}"
            f"(packet={self.packet}, receiver={self.receiver_endpoint})"
        )


#
# Envelope encoding/decoding
#
class EnvelopeDecoder(Service):
    """Decodes inbound datagrams to packet objects."""

    logger = logging.getLogger("ddht.EnvelopeDecoder")

    def __init__(
        self,
        inbound_datagram_receive_channel: ReceiveChannel[InboundDatagram],
        inbound_envelope_send_channel: SendChannel[InboundEnvelope],
        local_node_id: NodeID,
    ) -> None:
        self._inbound_datagram_receive_channel = inbound_datagram_receive_channel
        self._inbound_envelope_send_channel = inbound_envelope_send_channel
        self._local_node_id = local_node_id

    async def run(self) -> None:
        async with self._inbound_datagram_receive_channel:
            async with self._inbound_envelope_send_channel:
                packet: AnyPacket
                async for datagram, endpoint in self._inbound_datagram_receive_channel:
                    try:
                        packet = decode_packet(datagram, self._local_node_id)
                        self.logger.debug(
                            "Successfully decoded %s from %s",
                            packet.__class__.__name__,
                            endpoint,
                        )
                    except (DecryptionError, DecodingError, ValidationError):
                        self.logger.debug(
                            "Failed to decode datagram %s from %s",
                            datagram.hex(),
                            endpoint,
                        )
                    else:
                        try:
                            await self._inbound_envelope_send_channel.send(
                                InboundEnvelope(packet, endpoint)
                            )
                        except trio.BrokenResourceError:
                            self.logger.debug(
                                "EnvelopeDecoder exiting due to `trio.BrokenResourceError`"
                            )
                            self.manager.cancel()
                            return


class EnvelopeEncoder(Service):
    """Encodes outbound packets to datagrams."""

    logger = logging.getLogger("ddht.EnvelopeEncoder")

    def __init__(
        self,
        outbound_envelope_receive_channel: ReceiveChannel[OutboundEnvelope],
        outbound_datagram_send_channel: SendChannel[OutboundDatagram],
    ) -> None:
        self._outbound_envelope_receive_channel = outbound_envelope_receive_channel
        self._outbound_datagram_send_channel = outbound_datagram_send_channel

    async def run(self) -> None:
        async with self._outbound_envelope_receive_channel:
            async with self._outbound_datagram_send_channel:
                async for packet, endpoint in self._outbound_envelope_receive_channel:
                    outbound_datagram = OutboundDatagram(
                        packet.to_wire_bytes(), endpoint
                    )
                    self.logger.debug(
                        "Encoded %s for %s", packet.__class__.__name__, endpoint,
                    )
                    try:
                        await self._outbound_datagram_send_channel.send(
                            outbound_datagram
                        )
                    except trio.BrokenResourceError:
                        self.logger.debug(
                            "EnvelopeEncoder exiting due to `trio.BrokenResourceError`"
                        )
                        self.manager.cancel()
                        return
