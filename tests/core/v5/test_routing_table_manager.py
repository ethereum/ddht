from async_service import background_trio_service
from eth.db.backends.memory import MemoryDB
import pytest
import pytest_trio
import trio
from trio.testing import wait_all_tasks_blocked

from ddht.base_message import InboundMessage
from ddht.identity_schemes import default_identity_scheme_registry
from ddht.kademlia import KademliaRoutingTable, compute_distance, compute_log_distance
from ddht.node_db import NodeDB
from ddht.tools.factories.discovery import (
    FindNodeMessageFactory,
    InboundMessageFactory,
    PingMessageFactory,
)
from ddht.tools.factories.endpoint import EndpointFactory
from ddht.tools.factories.enr import ENRFactory
from ddht.tools.factories.node_id import NodeIDFactory
from ddht.v5.constants import ROUTING_TABLE_PING_INTERVAL
from ddht.v5.message_dispatcher import MessageDispatcher
from ddht.v5.messages import FindNodeMessage, NodesMessage, PingMessage, PongMessage
from ddht.v5.routing_table_manager import (
    FindNodeHandlerService,
    PingHandlerService,
    PingSenderService,
    iter_closest_nodes,
)


@pytest.fixture
def inbound_message_channels():
    return trio.open_memory_channel(0)


@pytest.fixture
def outbound_message_channels():
    return trio.open_memory_channel(0)


@pytest.fixture
def endpoint_vote_channels():
    return trio.open_memory_channel(0)


@pytest.fixture
def local_enr():
    return ENRFactory()


@pytest.fixture
def remote_enr(remote_endpoint):
    return ENRFactory(
        custom_kv_pairs={
            b"ip": remote_endpoint.ip_address,
            b"udp": remote_endpoint.port,
        },
    )


@pytest.fixture
def remote_endpoint():
    return EndpointFactory()


@pytest.fixture
def empty_routing_table(local_enr):
    routing_table = KademliaRoutingTable(local_enr.node_id, 16)
    return routing_table


@pytest.fixture
def routing_table(empty_routing_table, remote_enr):
    empty_routing_table.update(remote_enr.node_id)
    return empty_routing_table


@pytest_trio.trio_fixture
async def filled_routing_table(routing_table, node_db):
    # add entries until the first bucket is full
    while len(routing_table.get_nodes_at_log_distance(255)) < routing_table.bucket_size:
        enr = ENRFactory()
        routing_table.update(enr.node_id)
        node_db.set_enr(enr)
    return routing_table


@pytest_trio.trio_fixture
async def node_db(local_enr, remote_enr):
    node_db = NodeDB(default_identity_scheme_registry, MemoryDB())
    node_db.set_enr(local_enr)
    node_db.set_enr(remote_enr)
    return node_db


@pytest_trio.trio_fixture
async def message_dispatcher(
    node_db, inbound_message_channels, outbound_message_channels
):
    message_dispatcher = MessageDispatcher(
        node_db=node_db,
        inbound_message_receive_channel=inbound_message_channels[1],
        outbound_message_send_channel=outbound_message_channels[0],
    )
    async with background_trio_service(message_dispatcher):
        yield message_dispatcher


@pytest_trio.trio_fixture
async def ping_handler_service(
    local_enr,
    routing_table,
    message_dispatcher,
    node_db,
    inbound_message_channels,
    outbound_message_channels,
):
    ping_handler_service = PingHandlerService(
        local_node_id=local_enr.node_id,
        routing_table=routing_table,
        message_dispatcher=message_dispatcher,
        node_db=node_db,
        outbound_message_send_channel=outbound_message_channels[0],
    )
    async with background_trio_service(ping_handler_service):
        yield ping_handler_service


@pytest_trio.trio_fixture
async def find_node_handler_service(
    local_enr,
    routing_table,
    message_dispatcher,
    node_db,
    inbound_message_channels,
    outbound_message_channels,
    endpoint_vote_channels,
):
    find_node_handler_service = FindNodeHandlerService(
        local_node_id=local_enr.node_id,
        routing_table=routing_table,
        message_dispatcher=message_dispatcher,
        node_db=node_db,
        outbound_message_send_channel=outbound_message_channels[0],
    )
    async with background_trio_service(find_node_handler_service):
        yield find_node_handler_service


@pytest_trio.trio_fixture
async def ping_sender_service(
    local_enr,
    routing_table,
    message_dispatcher,
    node_db,
    inbound_message_channels,
    endpoint_vote_channels,
):
    ping_sender_service = PingSenderService(
        local_node_id=local_enr.node_id,
        routing_table=routing_table,
        message_dispatcher=message_dispatcher,
        node_db=node_db,
        endpoint_vote_send_channel=endpoint_vote_channels[0],
    )
    async with background_trio_service(ping_sender_service):
        yield ping_sender_service


@pytest.mark.trio
async def test_ping_handler_sends_pong(
    ping_handler_service,
    inbound_message_channels,
    outbound_message_channels,
    local_enr,
):
    ping = PingMessageFactory()
    inbound_message = InboundMessageFactory(message=ping)
    await inbound_message_channels[0].send(inbound_message)
    await wait_all_tasks_blocked()

    outbound_message = outbound_message_channels[1].receive_nowait()
    assert isinstance(outbound_message.message, PongMessage)
    assert outbound_message.message.request_id == ping.request_id
    assert outbound_message.message.enr_seq == local_enr.sequence_number
    assert outbound_message.receiver_endpoint == inbound_message.sender_endpoint
    assert outbound_message.receiver_node_id == inbound_message.sender_node_id


@pytest.mark.trio
async def test_ping_handler_updates_routing_table(
    ping_handler_service,
    inbound_message_channels,
    outbound_message_channels,
    local_enr,
    remote_enr,
    routing_table,
):
    distance = compute_log_distance(remote_enr.node_id, local_enr.node_id)
    other_node_id = NodeIDFactory.at_log_distance(local_enr.node_id, distance)
    routing_table.update(other_node_id)
    assert routing_table.get_nodes_at_log_distance(distance) == (
        other_node_id,
        remote_enr.node_id,
    )

    ping = PingMessageFactory()
    inbound_message = InboundMessageFactory(
        message=ping, sender_node_id=remote_enr.node_id,
    )
    await inbound_message_channels[0].send(inbound_message)
    await wait_all_tasks_blocked()

    assert routing_table.get_nodes_at_log_distance(distance) == (
        remote_enr.node_id,
        other_node_id,
    )


@pytest.mark.trio
async def test_ping_handler_requests_updated_enr(
    ping_handler_service,
    inbound_message_channels,
    outbound_message_channels,
    local_enr,
    remote_enr,
    routing_table,
):
    ping = PingMessageFactory(enr_seq=remote_enr.sequence_number + 1)
    inbound_message = InboundMessageFactory(message=ping)
    await inbound_message_channels[0].send(inbound_message)

    await wait_all_tasks_blocked()
    first_outbound_message = outbound_message_channels[1].receive_nowait()
    await wait_all_tasks_blocked()
    second_outbound_message = outbound_message_channels[1].receive_nowait()

    assert {
        first_outbound_message.message.__class__,
        second_outbound_message.message.__class__,
    } == {PongMessage, FindNodeMessage}

    outbound_find_node = (
        first_outbound_message
        if isinstance(first_outbound_message.message, FindNodeMessage)
        else second_outbound_message
    )

    assert outbound_find_node.message.distance == 0
    assert outbound_find_node.receiver_endpoint == inbound_message.sender_endpoint
    assert outbound_find_node.receiver_node_id == inbound_message.sender_node_id


@pytest.mark.trio
async def test_find_node_handler_sends_nodes(
    find_node_handler_service,
    inbound_message_channels,
    outbound_message_channels,
    local_enr,
):
    find_node = FindNodeMessageFactory(distance=0)
    inbound_message = InboundMessageFactory(message=find_node)
    await inbound_message_channels[0].send(inbound_message)
    await wait_all_tasks_blocked()

    outbound_message = outbound_message_channels[1].receive_nowait()
    assert isinstance(outbound_message.message, NodesMessage)
    assert outbound_message.message.request_id == find_node.request_id
    assert outbound_message.message.total == 1
    assert outbound_message.message.enrs == (local_enr,)


@pytest.mark.trio
async def test_find_node_handler_sends_remote_enrs(
    find_node_handler_service,
    inbound_message_channels,
    outbound_message_channels,
    local_enr,
    remote_enr,
):
    distance = compute_log_distance(local_enr.node_id, remote_enr.node_id)
    find_node = FindNodeMessageFactory(distance=distance)
    inbound_message = InboundMessageFactory(message=find_node)
    await inbound_message_channels[0].send(inbound_message)
    await wait_all_tasks_blocked()

    outbound_message = outbound_message_channels[1].receive_nowait()
    assert isinstance(outbound_message.message, NodesMessage)
    assert outbound_message.message.request_id == find_node.request_id
    assert outbound_message.message.total == 1
    assert outbound_message.message.enrs == (remote_enr,)


@pytest.mark.trio
async def test_find_node_handler_sends_many_remote_enrs(
    find_node_handler_service,
    inbound_message_channels,
    outbound_message_channels,
    filled_routing_table,
    node_db,
):
    distance = 255
    node_ids = filled_routing_table.get_nodes_at_log_distance(distance)
    assert len(node_ids) == filled_routing_table.bucket_size
    enrs = [node_db.get_enr(node_id) for node_id in node_ids]

    find_node = FindNodeMessageFactory(distance=distance)
    inbound_message = InboundMessageFactory(message=find_node)
    await inbound_message_channels[0].send(inbound_message)

    outbound_messages = []
    while True:
        await wait_all_tasks_blocked()
        try:
            outbound_messages.append(outbound_message_channels[1].receive_nowait())
        except trio.WouldBlock:
            break

    for outbound_message in outbound_messages:
        assert isinstance(outbound_message.message, NodesMessage)
        assert outbound_message.message.request_id == find_node.request_id
        assert outbound_message.message.total == len(outbound_messages)
        assert outbound_message.message.enrs
    sent_enrs = [
        enr
        for outbound_message in outbound_messages
        for enr in outbound_message.message.enrs
    ]
    assert sent_enrs == enrs


@pytest.mark.trio
async def test_find_node_handler_sends_empty(
    find_node_handler_service,
    inbound_message_channels,
    outbound_message_channels,
    routing_table,
    node_db,
):
    distance = 5
    assert len(routing_table.get_nodes_at_log_distance(distance)) == 0

    find_node = FindNodeMessageFactory(distance=distance)
    inbound_message = InboundMessageFactory(message=find_node)
    await inbound_message_channels[0].send(inbound_message)

    await wait_all_tasks_blocked()
    outbound_message = outbound_message_channels[1].receive_nowait()

    assert isinstance(outbound_message.message, NodesMessage)
    assert outbound_message.message.request_id == find_node.request_id
    assert outbound_message.message.total == 1
    assert not outbound_message.message.enrs


@pytest.mark.trio
async def test_send_ping(
    ping_sender_service,
    routing_table,
    inbound_message_channels,
    outbound_message_channels,
    local_enr,
    remote_enr,
    remote_endpoint,
):
    with trio.fail_after(ROUTING_TABLE_PING_INTERVAL):
        outbound_message = await outbound_message_channels[1].receive()

    assert isinstance(outbound_message.message, PingMessage)
    assert outbound_message.receiver_endpoint == remote_endpoint
    assert outbound_message.receiver_node_id == remote_enr.node_id


@pytest.mark.trio
async def test_send_endpoint_vote(
    ping_sender_service,
    routing_table,
    inbound_message_channels,
    outbound_message_channels,
    endpoint_vote_channels,
    local_enr,
    remote_enr,
    remote_endpoint,
):
    # wait for ping
    with trio.fail_after(ROUTING_TABLE_PING_INTERVAL):
        outbound_message = await outbound_message_channels[1].receive()
    ping = outbound_message.message

    # respond with pong
    fake_local_endpoint = EndpointFactory()
    pong = PongMessage(
        request_id=ping.request_id,
        enr_seq=0,
        packet_ip=fake_local_endpoint.ip_address,
        packet_port=fake_local_endpoint.port,
    )
    inbound_message = InboundMessage(
        message=pong,
        sender_endpoint=outbound_message.receiver_endpoint,
        sender_node_id=outbound_message.receiver_node_id,
    )
    await inbound_message_channels[0].send(inbound_message)
    await wait_all_tasks_blocked()

    # receive endpoint vote
    endpoint_vote = endpoint_vote_channels[1].receive_nowait()
    assert endpoint_vote.endpoint == fake_local_endpoint
    assert endpoint_vote.node_id == inbound_message.sender_node_id


def test_closest_nodes_empty(empty_routing_table):
    target = NodeIDFactory()
    assert list(iter_closest_nodes(target, empty_routing_table, [])) == []


def test_closest_nodes_only_routing(empty_routing_table):
    target = NodeIDFactory()
    nodes = [NodeIDFactory() for _ in range(10)]
    for node in nodes:
        empty_routing_table.update(node)

    closest_nodes = list(iter_closest_nodes(target, empty_routing_table, []))
    assert closest_nodes == sorted(
        nodes, key=lambda node: compute_distance(target, node)
    )


def test_closest_nodes_only_additional(empty_routing_table):
    target = NodeIDFactory()
    nodes = [NodeIDFactory() for _ in range(10)]
    closest_nodes = list(iter_closest_nodes(target, empty_routing_table, nodes))
    assert closest_nodes == sorted(
        nodes, key=lambda node: compute_distance(target, node)
    )


def test_lookup_generator_mixed(empty_routing_table):
    target = NodeIDFactory()
    nodes = sorted(
        [NodeIDFactory() for _ in range(10)],
        key=lambda node: compute_distance(node, target),
    )
    nodes_in_routing_table = nodes[:3] + nodes[6:8]
    nodes_in_additional = nodes[3:6] + nodes[8:]
    for node in nodes_in_routing_table:
        empty_routing_table.update(node)
    closest_nodes = list(
        iter_closest_nodes(target, empty_routing_table, nodes_in_additional)
    )
    assert closest_nodes == nodes
