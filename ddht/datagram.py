import logging
from socket import inet_aton, inet_ntoa
from typing import NamedTuple

from async_service import ManagerAPI, as_service
import trio
from trio.abc import ReceiveChannel, SendChannel
from trio.socket import SocketType

from ddht.constants import DISCOVERY_DATAGRAM_BUFFER_SIZE
from ddht.endpoint import Endpoint


#
# Data structures
#
class InboundDatagram(NamedTuple):
    datagram: bytes
    sender_endpoint: Endpoint


class OutboundDatagram(NamedTuple):
    datagram: bytes
    receiver_endpoint: Endpoint


#
# UDP
#
@as_service
async def DatagramReceiver(
    manager: ManagerAPI,
    sock: SocketType,
    inbound_datagram_send_channel: SendChannel[InboundDatagram],
) -> None:
    """Read datagrams from a socket and send them to a channel."""
    logger = logging.getLogger("ddht.DatagramReceiver")

    async with inbound_datagram_send_channel:
        while manager.is_running:
            datagram, (ip_address, port) = await sock.recvfrom(
                DISCOVERY_DATAGRAM_BUFFER_SIZE
            )
            endpoint = Endpoint(inet_aton(ip_address), port)
            logger.debug("Received %d bytes from %s", len(datagram), endpoint)
            inbound_datagram = InboundDatagram(datagram, endpoint)
            try:
                await inbound_datagram_send_channel.send(inbound_datagram)
            except trio.BrokenResourceError:
                logger.debug(
                    "DatagramReceiver exiting due to `trio.BrokenResourceError`"
                )
                manager.cancel()
                return


@as_service
async def DatagramSender(
    manager: ManagerAPI,
    outbound_datagram_receive_channel: ReceiveChannel[OutboundDatagram],
    sock: SocketType,
) -> None:
    """Take datagrams from a channel and send them via a socket to their designated receivers."""
    logger = logging.getLogger("ddht.DatagramSender")

    async with outbound_datagram_receive_channel:
        async for datagram, endpoint in outbound_datagram_receive_channel:
            logger.debug("Sending %d bytes to %s", len(datagram), endpoint)
            await sock.sendto(datagram, (inet_ntoa(endpoint.ip_address), endpoint.port))
