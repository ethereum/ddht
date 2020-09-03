from async_service import background_trio_service
from eth_enr import ENRDB
from eth_enr.tools.factories import ENRFactory
import pytest
import pytest_trio
import trio

from ddht.base_message import InboundMessage
from ddht.tools.factories.discovery import PingMessageFactory
from ddht.tools.factories.endpoint import EndpointFactory
from ddht.tools.factories.keys import PrivateKeyFactory
from ddht.v5.message_dispatcher import MessageDispatcher
from ddht.v5.messages import FindNodeMessage, NodesMessage, PingMessage


@pytest.fixture
def private_key():
    return PrivateKeyFactory().to_bytes()


@pytest.fixture
def remote_private_key():
    return PrivateKeyFactory().to_bytes()


@pytest.fixture
def endpoint():
    return EndpointFactory()


@pytest.fixture
def remote_endpoint():
    return EndpointFactory()


@pytest.fixture
def enr(private_key, endpoint):
    return ENRFactory(
        private_key=private_key,
        custom_kv_pairs={b"ip": endpoint.ip_address, b"udp": endpoint.port},
    )


@pytest.fixture
def remote_enr(remote_private_key, remote_endpoint):
    return ENRFactory(
        private_key=remote_private_key,
        custom_kv_pairs={
            b"ip": remote_endpoint.ip_address,
            b"udp": remote_endpoint.port,
        },
    )


@pytest_trio.trio_fixture
async def enr_db(enr, remote_enr):
    db = ENRDB({})
    db.set_enr(enr)
    db.set_enr(remote_enr)
    return db


@pytest.fixture
def inbound_message_channels():
    return trio.open_memory_channel(0)


@pytest.fixture
def outbound_message_channels():
    return trio.open_memory_channel(0)


@pytest_trio.trio_fixture
async def message_dispatcher(
    enr_db, inbound_message_channels, outbound_message_channels
):
    message_dispatcher = MessageDispatcher(
        enr_db=enr_db,
        inbound_message_receive_channel=inbound_message_channels[1],
        outbound_message_send_channel=outbound_message_channels[0],
    )
    async with background_trio_service(message_dispatcher):
        yield message_dispatcher


@pytest.mark.trio
async def test_request_handling(
    message_dispatcher, inbound_message_channels, remote_enr, remote_endpoint
):
    ping_send_channel, ping_receive_channel = trio.open_memory_channel(0)

    async with message_dispatcher.add_request_handler(
        PingMessage
    ) as request_subscription:

        inbound_message = InboundMessage(
            message=PingMessageFactory(),
            sender_endpoint=remote_endpoint,
            sender_node_id=remote_enr.node_id,
        )
        await inbound_message_channels[0].send(inbound_message)

        with trio.fail_after(1):
            handled_inbound_message = await request_subscription.receive()
        assert handled_inbound_message == inbound_message


@pytest.mark.trio
async def test_response_handling(
    message_dispatcher, remote_enr, inbound_message_channels
):
    request_id = message_dispatcher.get_free_request_id(remote_enr.node_id)
    async with message_dispatcher.add_response_handler(
        remote_enr.node_id, request_id,
    ) as response_subscription:

        inbound_message = InboundMessage(
            message=PingMessageFactory(request_id=request_id,),
            sender_endpoint=remote_endpoint,
            sender_node_id=remote_enr.node_id,
        )
        await inbound_message_channels[0].send(inbound_message)

        with trio.fail_after(1):
            handled_response = await response_subscription.receive()
        assert handled_response == inbound_message


@pytest.mark.trio
async def test_request(
    message_dispatcher,
    remote_enr,
    remote_endpoint,
    inbound_message_channels,
    outbound_message_channels,
    nursery,
):
    request_id = message_dispatcher.get_free_request_id(remote_enr.node_id)
    request = PingMessageFactory(request_id=request_id)
    response = PingMessageFactory(request_id=request_id)

    async def handle_request_on_remote():
        async for outbound_message in outbound_message_channels[1]:
            assert outbound_message.message == request
            assert outbound_message.receiver_endpoint == remote_endpoint
            assert outbound_message.receiver_node_id == remote_enr.node_id

            await inbound_message_channels[0].send(
                InboundMessage(
                    message=response,
                    sender_endpoint=remote_endpoint,
                    sender_node_id=remote_enr.node_id,
                )
            )

    nursery.start_soon(handle_request_on_remote)

    received_response = await message_dispatcher.request(remote_enr.node_id, request)

    assert received_response.message == response
    assert received_response.sender_endpoint == remote_endpoint
    assert received_response.sender_node_id == remote_enr.node_id

    received_response_with_explicit_endpoint = await message_dispatcher.request(
        remote_enr.node_id, request, endpoint=remote_endpoint,
    )
    assert received_response_with_explicit_endpoint == received_response


@pytest.mark.trio
async def test_request_nodes(
    message_dispatcher,
    remote_enr,
    remote_endpoint,
    inbound_message_channels,
    outbound_message_channels,
    nursery,
):
    request_id = message_dispatcher.get_free_request_id(remote_enr.node_id)
    request = FindNodeMessage(request_id=request_id, distance=3,)
    enrs_per_message = [[ENRFactory() for _ in range(2)] for _ in range(3)]
    response_messages = [
        NodesMessage(request_id=request_id, total=len(enrs_per_message), enrs=enrs)
        for enrs in enrs_per_message
    ]

    async def handle_request_on_remote():
        async for outbound_message in outbound_message_channels[1]:
            assert outbound_message.message == request
            assert outbound_message.receiver_endpoint == remote_endpoint
            assert outbound_message.receiver_node_id == remote_enr.node_id

            for response in response_messages:
                await inbound_message_channels[0].send(
                    InboundMessage(
                        message=response,
                        sender_endpoint=remote_endpoint,
                        sender_node_id=remote_enr.node_id,
                    )
                )

    nursery.start_soon(handle_request_on_remote)

    with trio.fail_after(3):
        received_responses = await message_dispatcher.request_nodes(
            remote_enr.node_id, request
        )
    assert len(received_responses) == len(response_messages)
    for received_response, expected_response_message in zip(
        received_responses, response_messages
    ):
        assert received_response.sender_endpoint == remote_endpoint
        assert received_response.sender_node_id == remote_enr.node_id
        assert received_response.message == expected_response_message
