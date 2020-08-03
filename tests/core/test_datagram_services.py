from socket import inet_aton

from async_service import background_trio_service
import pytest
import trio

from ddht.datagram import DatagramReceiver, DatagramSender, OutgoingDatagram
from ddht.endpoint import Endpoint


@pytest.mark.trio
async def test_datagram_receiver(socket_pair):
    sending_socket, receiving_socket = socket_pair
    receiver_address = receiving_socket.getsockname()
    sender_address = sending_socket.getsockname()

    send_channel, receive_channel = trio.open_memory_channel(1)
    async with background_trio_service(
        DatagramReceiver(receiving_socket, send_channel)
    ):
        data = b"some packet"

        await sending_socket.sendto(data, receiver_address)
        with trio.fail_after(0.5):
            received_datagram = await receive_channel.receive()

        assert received_datagram.datagram == data
        assert received_datagram.sender_endpoint.ip_address == inet_aton(
            sender_address[0]
        )
        assert received_datagram.sender_endpoint.port == sender_address[1]


@pytest.mark.trio
async def test_datagram_sender(socket_pair):
    sending_socket, receiving_socket = socket_pair
    receiver_endpoint = receiving_socket.getsockname()
    sender_endpoint = sending_socket.getsockname()

    send_channel, receive_channel = trio.open_memory_channel(1)
    async with background_trio_service(DatagramSender(receive_channel, sending_socket)):
        outgoing_datagram = OutgoingDatagram(
            b"some packet",
            Endpoint(inet_aton(receiver_endpoint[0]), receiver_endpoint[1]),
        )
        await send_channel.send(outgoing_datagram)

        with trio.fail_after(0.5):
            data, sender = await receiving_socket.recvfrom(1024)
        assert data == outgoing_datagram.datagram
        assert sender == sender_endpoint
