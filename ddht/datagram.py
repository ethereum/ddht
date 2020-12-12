from socket import inet_aton, inet_ntoa
from typing import NamedTuple

from async_service import ManagerAPI, as_service
from eth_utils import get_extended_debug_logger
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
    logger = get_extended_debug_logger("ddht.DatagramReceiver")

    async with inbound_datagram_send_channel:
        while manager.is_running:
            datagram, (ip_address, port) = await sock.recvfrom(
                DISCOVERY_DATAGRAM_BUFFER_SIZE
            )
            endpoint = Endpoint(inet_aton(ip_address), port)
            logger.debug2("Received %d bytes from %s", len(datagram), endpoint)
            inbound_datagram = InboundDatagram(datagram, endpoint)
            try:
                await inbound_datagram_send_channel.send(inbound_datagram)
            except trio.BrokenResourceError:
                logger.debug(
                    "DatagramReceiver exiting due to `trio.BrokenResourceError`"
                )
                manager.cancel()
                return


async def send_datagram(sock: SocketType, datagram: bytes, endpoint: Endpoint) -> None:
    await sock.sendto(datagram, (inet_ntoa(endpoint.ip_address), endpoint.port))


@as_service
async def DatagramSender(
    manager: ManagerAPI,
    outbound_datagram_receive_channel: ReceiveChannel[OutboundDatagram],
    sock: SocketType,
) -> None:
    """Take datagrams from a channel and send them via a socket to their designated receivers."""
    logger = get_extended_debug_logger("ddht.DatagramSender")

    async with outbound_datagram_receive_channel:
        async for datagram, endpoint in outbound_datagram_receive_channel:
            await send_datagram(sock, datagram, endpoint)
            logger.debug2("Sending %d bytes to %s", len(datagram), endpoint)
