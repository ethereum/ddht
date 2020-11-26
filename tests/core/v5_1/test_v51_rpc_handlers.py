import collections
from contextlib import AsyncExitStack
import ipaddress
import secrets
from socket import inet_ntoa
import sqlite3

from async_service import background_trio_service
from eth_enr import ENR, ENRManager, OldSequenceNumber, QueryableENRDB
from eth_enr.tools.factories import ENRFactory
from eth_utils import decode_hex, encode_hex
import pytest
import trio
from web3 import IPCProvider, Web3

from ddht.kademlia import compute_log_distance
from ddht.rpc import RPCServer
from ddht.tools.factories.keys import PrivateKeyFactory
from ddht.tools.factories.node_id import NodeIDFactory
from ddht.tools.web3 import DiscoveryV5Module
from ddht.v5_1.abc import TalkProtocolAPI
from ddht.v5_1.messages import (
    FindNodeMessage,
    FoundNodesMessage,
    PingMessage,
    PongMessage,
    TalkRequestMessage,
    TalkResponseMessage,
)
from ddht.v5_1.rpc_handlers import get_v51_rpc_handlers


@pytest.fixture
async def rpc_server(ipc_path, alice):
    async with alice.network() as network:
        server = RPCServer(ipc_path, get_v51_rpc_handlers(network))
        async with background_trio_service(server):
            await server.wait_serving()
            yield server


@pytest.fixture
def w3(rpc_server, ipc_path):
    return Web3(IPCProvider(ipc_path), modules={"discv5": (DiscoveryV5Module,)})


@pytest.fixture
async def bob_network(bob):
    async with bob.network() as network:
        yield network


@pytest.fixture(params=("nodeid", "enode", "enr"))
def bob_node_id_param(request, alice, bob, bob_network):
    try:
        alice.enr_db.set_enr(bob.enr)
    except OldSequenceNumber:
        pass

    if request.param == "nodeid":
        return bob.node_id.hex()
    elif request.param == "enode":
        return f"enode://{bob.node_id.hex()}@{bob.endpoint}"
    elif request.param == "enr":
        return repr(bob.enr)
    else:
        raise Exception(f"Unhandled param: {request.param}")


@pytest.fixture(
    params=(
        "unknown-endpoint",
        "too-short",
        "too-long",
        "enode-missing-scheme",
        "enode-missing-endpoint",
        "enode-bad-nodeid",
        "enr-without-prefix",
        "enr-without-endpoint",
    )
)
def invalid_node_id(request, bob, bob_network):
    if request.param == "unknown-endpoint":
        return NodeIDFactory().hex()
    elif request.param == "too-short":
        return (b"\x01" * 31).hex()
    elif request.param == "too-long":
        return (b"\x01" * 33).hex()
    elif request.param == "enode-missing-scheme":
        return f"{bob.node_id.hex()}@{bob.endpoint}"
    elif request.param == "enode-missing-endpoint":
        return f"enode://{bob.node_id.hex()}@"
    elif request.param == "enode-bad-nodeid":
        too_short_nodeid = b"\x01" * 31
        return f"enode://{too_short_nodeid.hex()}@{bob.endpoint}"
    elif request.param == "enr-without-prefix":
        return repr(bob.enr)[4:]
    elif request.param == "enr-without-endpoint":
        bob_network.enr_manager.update(
            (b"ip", None), (b"udp", None), (b"tcp", None),
        )
        return repr(bob_network.enr_manager.enr)
    else:
        raise Exception(f"Unhandled param: {request.param}")


@pytest.fixture(params=("nodeid", "nodeid-hex", "enode", "enr", "enr-repr"))
def bob_node_id_param_w3(request, alice, bob, bob_network):
    try:
        alice.enr_db.set_enr(bob.enr)
    except OldSequenceNumber:
        pass

    if request.param == "nodeid":
        return bob.node_id
    elif request.param == "nodeid-hex":
        return encode_hex(bob.node_id)
    elif request.param == "enode":
        return f"enode://{bob.node_id.hex()}@{bob.endpoint}"
    elif request.param == "enr":
        return bob.enr
    elif request.param == "enr-repr":
        return repr(bob.enr)
    else:
        raise Exception(f"Unhandled param: {request.param}")


@pytest.fixture
def new_enr_manager():
    enr_db = QueryableENRDB(sqlite3.connect(":memory:"))
    private_key = PrivateKeyFactory()
    base_enr = ENRFactory(
        private_key=private_key.to_bytes(), sequence_number=secrets.randbelow(100) + 1,
    )
    enr_db.set_enr(base_enr)
    return ENRManager(private_key, enr_db)


@pytest.fixture
def new_enr(new_enr_manager):
    return new_enr_manager.enr


@pytest.mark.trio
async def test_v51_rpc_ping(make_request, bob_node_id_param, alice, bob):
    pong = await make_request("discv5_ping", [bob_node_id_param])
    assert pong["enr_seq"] == bob.enr.sequence_number
    assert pong["packet_ip"] == inet_ntoa(alice.endpoint.ip_address)
    assert pong["packet_port"] == alice.endpoint.port


@pytest.mark.parametrize(
    "endpoint",
    (
        "discv5_bond",
        "discv5_deleteENR",
        "discv5_getENR",
        "discv5_lookupENR",
        "discv5_ping",
        "discv5_sendPing",
    ),
)
@pytest.mark.trio
async def test_v51_rpc_invalid_node_id(make_request, invalid_node_id, endpoint):
    with pytest.raises(Exception, match="'error':"):
        await make_request(endpoint, [invalid_node_id])


@pytest.mark.parametrize(
    "endpoint",
    (
        "discv5_bond",
        "discv5_deleteENR",
        "discv5_getENR",
        "discv5_lookupENR",
        "discv5_ping",
        "discv5_recursiveFindNodes",
        "discv5_setENR",
        "discv5_sendPing",
    ),
)
@pytest.mark.trio
async def test_v51_rpc_missing_node_id(make_request, endpoint):
    with pytest.raises(Exception, match="'error':"):
        await make_request(endpoint, [])


@pytest.mark.trio
async def test_v51_rpc_ping_web3(bob_node_id_param_w3, alice, bob, w3):
    pong = await trio.to_thread.run_sync(w3.discv5.ping, bob_node_id_param_w3)
    assert pong.enr_seq == bob.enr.sequence_number
    assert pong.packet_ip == ipaddress.ip_address(alice.endpoint.ip_address)
    assert pong.packet_port == alice.endpoint.port


@pytest.mark.trio
async def test_v51_rpc_get_enr(make_request, bob, bob_node_id_param):
    bob_response = await make_request("discv5_getENR", [bob_node_id_param])
    assert bob_response["enr_repr"] == repr(bob.enr)


@pytest.mark.trio
async def test_v51_rpc_get_enr_with_unseen_node_id(make_request, new_enr):
    with pytest.raises(Exception, match="'error':"):
        await make_request("discv5_getENR", [repr(new_enr)])


@pytest.mark.trio
async def test_v51_rpc_get_enr_web3(bob, bob_node_id_param_w3, w3):
    response = await trio.to_thread.run_sync(w3.discv5.get_enr, bob_node_id_param_w3)
    assert response.enr == bob.enr


@pytest.mark.trio
async def test_v51_rpc_get_enr_web3_unseen_node_id(new_enr, w3):
    with pytest.raises(Exception, match="Unexpected Error"):
        await trio.to_thread.run_sync(w3.discv5.get_enr, repr(new_enr))


@pytest.mark.trio
async def test_v51_rpc_set_enr(make_request, new_enr):
    set_enr_response = await make_request("discv5_setENR", [repr(new_enr)])
    assert set_enr_response is None

    get_enr_response = await make_request("discv5_getENR", [new_enr.node_id.hex()])
    assert get_enr_response["enr_repr"] == repr(new_enr)


@pytest.mark.trio
async def test_v51_rpc_set_enr_invalid_enr(make_request, bob):
    invalid_enr = repr(bob.enr)[4:]

    with pytest.raises(Exception, match="'error':"):
        await make_request("discv5_setENR", [invalid_enr])


@pytest.mark.trio
async def test_v51_rpc_set_enr_web3(w3, new_enr):
    response = await trio.to_thread.run_sync(w3.discv5.set_enr, repr(new_enr))
    assert response is None

    response_two = await trio.to_thread.run_sync(w3.discv5.get_enr, repr(new_enr))
    assert response_two.enr == new_enr


@pytest.mark.trio
async def test_v51_rpc_set_enr_web3_with_invalid_sequence_number(w3, new_enr_manager):
    # grab the "old" version
    old_enr = new_enr_manager.enr
    await trio.to_thread.run_sync(w3.discv5.set_enr, repr(new_enr_manager.enr))

    # update the ENR so the old version will have an old sequence number
    new_enr_manager.update((b"test", b"value"))
    assert new_enr_manager.enr.sequence_number == old_enr.sequence_number + 1

    # ensure that the database has the new version
    response = await trio.to_thread.run_sync(
        w3.discv5.set_enr, repr(new_enr_manager.enr)
    )
    assert response is None

    # setting the old one should throw an error
    with pytest.raises(Exception, match="Invalid ENR"):
        await trio.to_thread.run_sync(w3.discv5.set_enr, repr(old_enr))


@pytest.mark.trio
async def test_v51_rpc_delete_enr(make_request, bob, bob_node_id_param):
    bob_response = await make_request("discv5_getENR", [bob_node_id_param])
    assert bob_response["enr_repr"] == repr(bob.enr)

    response_one = await make_request("discv5_deleteENR", [bob_node_id_param])
    assert response_one is None

    with pytest.raises(Exception, match="'error':"):
        await make_request("discv5_getENR", [bob_node_id_param])


@pytest.mark.trio
async def test_v51_rpc_delete_enr_unknown_node_id(make_request, new_enr):
    with pytest.raises(Exception, match="'error':"):
        await make_request("discv5_deleteENR", [repr(new_enr)])


@pytest.mark.trio
async def test_v51_rpc_delete_enr_web3(w3, bob, bob_node_id_param_w3):
    response = await trio.to_thread.run_sync(w3.discv5.get_enr, bob_node_id_param_w3)
    assert response.enr == bob.enr

    response = await trio.to_thread.run_sync(w3.discv5.delete_enr, repr(bob.enr))
    assert response is None

    with pytest.raises(Exception):
        await trio.to_thread.run_sync(w3.discv5.get_enr, bob_node_id_param_w3)


@pytest.mark.trio
async def test_v51_rpc_lookup_enr(make_request, bob, bob_node_id_param):
    bob_response = await make_request("discv5_lookupENR", [bob_node_id_param])
    assert bob_response["enr_repr"] == repr(bob.enr)


@pytest.mark.trio
async def test_v51_rpc_lookup_enr_with_sequence_number(
    make_request, bob, bob_node_id_param
):
    bob_response = await make_request("discv5_lookupENR", [bob_node_id_param, 101])
    assert bob_response["enr_repr"] == repr(bob.enr)


@pytest.mark.trio
async def test_v51_rpc_lookup_enr_web3(bob, bob_node_id_param_w3, w3):
    response = await trio.to_thread.run_sync(w3.discv5.lookup_enr, bob_node_id_param_w3)
    assert response.enr == bob.enr


@pytest.mark.trio
async def test_v51_rpc_lookup_enr_web3_with_sequence_number(
    bob, bob_node_id_param_w3, w3
):
    response = await trio.to_thread.run_sync(
        w3.discv5.lookup_enr, bob_node_id_param_w3, 101
    )
    assert response.enr == bob.enr


@pytest.mark.trio
async def test_v51_rpc_lookup_enr_with_unseen_node_id(make_request, bob):
    await make_request("discv5_deleteENR", [repr(bob.enr)])
    with pytest.raises(Exception):
        await make_request("discv5_lookupENR", [repr(bob.enr)])


@pytest.mark.trio
async def test_v51_rpc_lookup_enr_web3_unseen_node_id(bob, w3):
    await trio.to_thread.run_sync(w3.discv5.delete_enr, repr(bob.enr))
    with pytest.raises(Exception, match="Unexpected Error"):
        await trio.to_thread.run_sync(w3.discv5.lookup_enr, repr(bob.enr))


@pytest.mark.trio
async def test_v51_rpc_send_ping(make_request, bob_node_id_param, bob_network):
    async with bob_network.client.dispatcher.subscribe(PingMessage) as subscription:
        response = await make_request("discv5_sendPing", [bob_node_id_param])
        with trio.fail_after(2):
            request = await subscription.receive()
        assert encode_hex(request.message.request_id) == response["request_id"]


@pytest.mark.trio
async def test_v51_rpc_send_ping_web3(w3, bob_network, bob_node_id_param_w3):
    async with bob_network.client.dispatcher.subscribe(PingMessage) as subscription:
        response = await trio.to_thread.run_sync(
            w3.discv5.send_ping, bob_node_id_param_w3
        )
        with trio.fail_after(2):
            request = await subscription.receive()
    assert encode_hex(request.message.request_id) == response.request_id


@pytest.mark.trio
async def test_v51_rpc_send_pong(make_request, bob_network, bob_node_id_param):
    request_id = encode_hex(secrets.token_bytes(4))

    async with bob_network.client.dispatcher.subscribe(PongMessage) as subscription:
        response = await make_request(
            "discv5_sendPong", [bob_node_id_param, request_id]
        )

        with trio.fail_after(2):
            request = await subscription.receive()

        assert response is None
        assert encode_hex(request.message.request_id) == request_id


@pytest.mark.trio
async def test_v51_rpc_send_pong_invalid_node_id(make_request, invalid_node_id):
    with pytest.raises(Exception, match="'error':"):
        await make_request("discv5_sendPong", [invalid_node_id])


@pytest.mark.trio
async def test_v51_rpc_send_pong_missing_node_id(make_request):
    with pytest.raises(Exception, match="'error':"):
        await make_request("discv5_sendPong", [])


@pytest.mark.trio
async def test_v51_rpc_send_pong_web3(
    w3, bob_network, bob_node_id_param_w3,
):
    request_id = encode_hex(secrets.token_bytes(4))

    async with bob_network.client.dispatcher.subscribe(PongMessage) as subscription:
        response = await trio.to_thread.run_sync(
            w3.discv5.send_pong, bob_node_id_param_w3, request_id
        )
        with trio.fail_after(2):
            request = await subscription.receive()

        assert response is None
        assert encode_hex(request.message.request_id) == request_id


@pytest.mark.trio
async def test_v51_rpc_findNodes(make_request, bob_node_id_param, bob):
    distances = set()

    for _ in range(10):
        enr = ENRFactory()
        distances.add(compute_log_distance(bob.node_id, enr.node_id))
        bob.enr_db.set_enr(enr)

    # request with positional single distance
    enrs_at_0 = await make_request("discv5_findNodes", [bob_node_id_param, 0])

    # verify that all of the returned ENR records can be parsed as valid ENRs
    for enr_repr in enrs_at_0:
        ENR.from_repr(enr_repr)

    # request with multiple distances
    enrs_at_some_distance = await make_request(
        "discv5_findNodes", [bob_node_id_param, tuple(distances)],
    )

    # verify that all of the returned ENR records can be parsed as valid ENRs
    for enr_repr in enrs_at_some_distance:
        ENR.from_repr(enr_repr)


@pytest.mark.parametrize("endpoint", ("discv5_findNodes", "discv5_sendFindNodes",))
@pytest.mark.trio
async def test_v51_rpc_findNodes_invalid_params(
    make_request, invalid_node_id, bob, endpoint
):
    # bad node_id
    with pytest.raises(Exception, match="'error':"):
        await make_request(endpoint, [invalid_node_id, 0])
    with pytest.raises(Exception, match="'error':"):
        await make_request(endpoint, [invalid_node_id, [0]])

    # invalid distances
    with pytest.raises(Exception, match="'error':"):
        await make_request(endpoint, [bob.node_id.hex(), -1])
    with pytest.raises(Exception, match="'error':"):
        await make_request(endpoint, [bob.node_id.hex(), 257])
    with pytest.raises(Exception, match="'error':"):
        await make_request(endpoint, [bob.node_id.hex(), 1.2])
    with pytest.raises(Exception, match="'error':"):
        await make_request(endpoint, [bob.node_id.hex(), []])
    with pytest.raises(Exception, match="'error':"):
        await make_request(endpoint, [bob.node_id.hex(), "1"])
    with pytest.raises(Exception, match="'error':"):
        await make_request(endpoint, [bob.node_id.hex(), [1, "2"]])

    # wrong params count
    with pytest.raises(Exception, match="'error':"):
        await make_request(endpoint, [])
    with pytest.raises(Exception, match="'error':"):
        await make_request(endpoint, [bob.node_id.hex()])
    with pytest.raises(Exception, match="'error':"):
        await make_request(endpoint, [bob.node_id.hex(), 0, "extra"])


@pytest.mark.trio
async def test_v51_rpc_sendFoundNodes_invalid_params(
    make_request, invalid_node_id, bob
):
    enrs = set()
    for _ in range(10):
        enr = ENRFactory()
        bob.enr_db.set_enr(enr)
        enrs.add(repr(enr))
    single_enr = next(iter(enrs))

    # bad node_id
    with pytest.raises(Exception, match="'error':"):
        await make_request("discv5_sendFoundNodes", [invalid_node_id, tuple(enrs), 0])
    with pytest.raises(Exception, match="'error':"):
        await make_request("discv5_sendFoundNodes", [invalid_node_id, tuple(enrs), [0]])

    # invalid enr
    with pytest.raises(Exception, match="'error':"):
        await make_request(
            "discv5_sendFoundNodes", [invalid_node_id, (single_enr[4:],), 0]
        )

    # invalid distances
    with pytest.raises(Exception, match="'error':"):
        await make_request(
            "discv5_sendFoundNodes", [bob.node_id.hex(), tuple(enrs), -1]
        )
    with pytest.raises(Exception, match="'error':"):
        await make_request(
            "discv5_sendFoundNodes", [bob.node_id.hex(), tuple(enrs), 257]
        )
    with pytest.raises(Exception, match="'error':"):
        await make_request(
            "discv5_sendFoundNodes", [bob.node_id.hex(), tuple(enrs), 1.2]
        )
    with pytest.raises(Exception, match="'error':"):
        await make_request(
            "discv5_sendFoundNodes", [bob.node_id.hex(), tuple(enrs), []]
        )
    with pytest.raises(Exception, match="'error':"):
        await make_request(
            "discv5_sendFoundNodes", [bob.node_id.hex(), tuple(enrs), "xyz"]
        )
    with pytest.raises(Exception, match="'error':"):
        await make_request(
            "discv5_sendFoundNodes", [bob.node_id.hex(), tuple(enrs), [1, "2"]]
        )

    # wrong params count
    with pytest.raises(Exception, match="'error':"):
        await make_request("discv5_sendFoundNodes", [])
    with pytest.raises(Exception, match="'error':"):
        await make_request("discv5_sendFoundNodes", [bob.node_id.hex(), tuple(enrs)])
    with pytest.raises(Exception, match="'error':"):
        await make_request(
            "discv5_sendFoundNodes", [bob.node_id.hex(), tuple(enrs), 0, "extra"]
        )


@pytest.mark.trio
async def test_v51_rpc_findNodes_w3(bob_node_id_param, bob, w3):
    distances = set()

    for _ in range(10):
        enr = ENRFactory()
        distances.add(compute_log_distance(bob.node_id, enr.node_id))
        bob.enr_db.set_enr(enr)

    # request with positional single distance
    enrs_at_0 = await trio.to_thread.run_sync(
        w3.discv5.find_nodes, bob_node_id_param, 0
    )
    assert all(isinstance(enr, ENR) for enr in enrs_at_0)

    # request with multiple distances
    enrs_at_some_distance = await trio.to_thread.run_sync(
        w3.discv5.find_nodes, bob_node_id_param, tuple(distances),
    )

    # verify that all of the returned ENR records can be parsed as valid ENRs
    for enr_repr in enrs_at_some_distance:
        ENR.from_repr(enr_repr)


@pytest.mark.trio
async def test_v51_rpc_sendFindNodes(make_request, bob_node_id_param, bob, bob_network):
    distances = set()

    for _ in range(10):
        enr = ENRFactory()
        distances.add(compute_log_distance(bob.node_id, enr.node_id))
        bob.enr_db.set_enr(enr)

    async with bob_network.client.dispatcher.subscribe(FindNodeMessage) as subscription:
        single_response = await make_request(
            "discv5_sendFindNodes", [bob_node_id_param, 0]
        )
        with trio.fail_after(2):
            first_receipt = await subscription.receive()

        assert encode_hex(first_receipt.message.request_id) == single_response
        assert first_receipt.message.distances == (0,)

        # request with multiple distances
        multiple_response = await make_request(
            "discv5_sendFindNodes", [bob_node_id_param, tuple(distances)],
        )

        with trio.fail_after(2):
            second_receipt = await subscription.receive()
        assert encode_hex(second_receipt.message.request_id) == multiple_response
        assert second_receipt.message.distances == tuple(distances)


@pytest.mark.trio
async def test_v51_rpc_sendFindNodes_web3(bob_node_id_param_w3, bob, bob_network, w3):
    distances = set()

    for _ in range(10):
        enr = ENRFactory()
        distances.add(compute_log_distance(bob.node_id, enr.node_id))
        bob.enr_db.set_enr(enr)

    async with bob_network.client.dispatcher.subscribe(FindNodeMessage) as subscription:
        first_response = await trio.to_thread.run_sync(
            w3.discv5.send_find_nodes, bob_node_id_param_w3, 0
        )
        with trio.fail_after(2):
            first_receipt = await subscription.receive()

        assert encode_hex(first_receipt.message.request_id) == first_response.value
        assert first_receipt.message.distances == (0,)

        # request with multiple distances
        second_response = await trio.to_thread.run_sync(
            w3.discv5.send_find_nodes, bob_node_id_param_w3, tuple(distances)
        )

        with trio.fail_after(2):
            second_receipt = await subscription.receive()
        assert encode_hex(second_receipt.message.request_id) == second_response.value
        assert second_receipt.message.distances == tuple(distances)


@pytest.mark.trio
async def test_v51_rpc_sendFoundNodes(
    make_request, bob_node_id_param, bob, bob_network
):
    distances = set()
    enrs = set()

    for _ in range(10):
        enr = ENRFactory()
        distances.add(compute_log_distance(bob.node_id, enr.node_id))
        bob.enr_db.set_enr(enr)
        enrs.add(repr(enr))

    request_id = encode_hex(secrets.token_bytes(4))
    single_enr = next(iter(enrs))

    async with bob_network.client.dispatcher.subscribe(
        FoundNodesMessage
    ) as subscription:
        first_response = await make_request(
            "discv5_sendFoundNodes", [bob_node_id_param, (single_enr,), request_id]
        )
        with trio.fail_after(2):
            first_receipt = await subscription.receive()

        assert first_receipt.message.total == first_response
        assert encode_hex(first_receipt.message.request_id) == request_id

        # request with multiple enrs
        second_response = await make_request(
            "discv5_sendFoundNodes", [bob_node_id_param, tuple(enrs), request_id],
        )

        with trio.fail_after(2):
            second_receipt = await subscription.receive()

        assert second_receipt.message.total == second_response
        assert encode_hex(second_receipt.message.request_id) == request_id


@pytest.mark.trio
async def test_v51_rpc_sendFoundNodes_web3(bob_node_id_param_w3, bob, bob_network, w3):
    distances = set()
    enrs = set()

    for _ in range(10):
        enr = ENRFactory()
        distances.add(compute_log_distance(bob.node_id, enr.node_id))
        bob.enr_db.set_enr(enr)
        enrs.add(repr(enr))

    request_id = encode_hex(secrets.token_bytes(4))
    single_enr = next(iter(enrs))

    async with bob_network.client.dispatcher.subscribe(
        FoundNodesMessage
    ) as subscription:

        first_response = await trio.to_thread.run_sync(
            w3.discv5.send_found_nodes, bob_node_id_param_w3, (single_enr,), request_id
        )
        with trio.fail_after(2):
            first_receipt = await subscription.receive()

        assert first_receipt.message.total == first_response.value
        assert encode_hex(first_receipt.message.request_id) == request_id

        # request with multiple enrs
        second_response = await trio.to_thread.run_sync(
            w3.discv5.send_found_nodes, bob_node_id_param_w3, tuple(enrs), request_id
        )

        with trio.fail_after(2):
            second_receipt = await subscription.receive()

        assert second_receipt.message.total == second_response.value
        assert encode_hex(second_receipt.message.request_id) == request_id


@pytest.mark.trio
async def test_v51_rpc_sendTalkRequest(make_request, bob_node_id_param, bob_network):
    async with bob_network.client.dispatcher.subscribe(
        TalkRequestMessage
    ) as subscription:
        response = await make_request(
            "discv5_sendTalkRequest", [bob_node_id_param, "0x1234", "0x1234"]
        )
        with trio.fail_after(2):
            receipt = await subscription.receive()

        assert encode_hex(receipt.request_id) == response


@pytest.mark.trio
async def test_v51_rpc_sendTalkRequest_web3(
    make_request, bob_node_id_param_w3, bob_network, w3
):
    async with bob_network.client.dispatcher.subscribe(
        TalkRequestMessage
    ) as subscription:
        response = await trio.to_thread.run_sync(
            w3.discv5.send_talk_request, bob_node_id_param_w3, "0x1234", "0x1234",
        )
        with trio.fail_after(2):
            receipt = await subscription.receive()

        assert encode_hex(receipt.request_id) == response.value


@pytest.mark.trio
async def test_v51_rpc_sendTalkResponse(make_request, bob_node_id_param, bob_network):
    async with bob_network.client.dispatcher.subscribe(
        TalkResponseMessage
    ) as subscription:
        response = await make_request(
            "discv5_sendTalkResponse", [bob_node_id_param, "0x1234", "0x1234"]
        )
        with trio.fail_after(2):
            receipt = await subscription.receive()

        assert response is None
        assert encode_hex(receipt.request_id) == "0x1234"


@pytest.mark.trio
async def test_v51_rpc_sendTalkResponse_web3(
    make_request, bob_node_id_param_w3, bob_network, w3
):
    async with bob_network.client.dispatcher.subscribe(
        TalkResponseMessage
    ) as subscription:
        response = await trio.to_thread.run_sync(
            w3.discv5.send_talk_response, bob_node_id_param_w3, "0x1234", "0x1234",
        )
        with trio.fail_after(2):
            receipt = await subscription.receive()

        assert response is None
        assert encode_hex(receipt.request_id) == "0x1234"


@pytest.mark.trio
async def test_v51_rpc_talk(make_request, bob_network, bob_node_id_param):
    class ProtocolTest(TalkProtocolAPI):
        protocol_id = b"test"

    async def _do_talk_response(network):
        network.add_talk_protocol(ProtocolTest())

        async with network.dispatcher.subscribe(TalkRequestMessage) as subscription:
            request = await subscription.receive()
            await network.client.send_talk_response(
                request.sender_node_id,
                request.sender_endpoint,
                payload=b"test-response-payload",
                request_id=request.message.request_id,
            )

    async with trio.open_nursery() as nursery:
        nursery.start_soon(_do_talk_response, bob_network)

        with trio.fail_after(2):
            response = await make_request(
                "discv5_talk", [bob_node_id_param, encode_hex(b"test"), "0x1234"]
            )

    assert decode_hex(response) == b"test-response-payload"


@pytest.mark.trio
async def test_v51_rpc_talk_web3(make_request, bob_network, bob_node_id_param_w3, w3):
    class ProtocolTest(TalkProtocolAPI):
        protocol_id = b"test"

    async def _do_talk_response(network):
        network.add_talk_protocol(ProtocolTest())

        async with network.dispatcher.subscribe(TalkRequestMessage) as subscription:
            request = await subscription.receive()
            await network.client.send_talk_response(
                request.sender_node_id,
                request.sender_endpoint,
                payload=b"test-response-payload",
                request_id=request.message.request_id,
            )

    async with trio.open_nursery() as nursery:
        nursery.start_soon(_do_talk_response, bob_network)

        with trio.fail_after(2):

            response = await trio.to_thread.run_sync(
                w3.discv5.talk, bob_node_id_param_w3, encode_hex(b"test"), "0x1234",
            )

    assert decode_hex(response.value) == b"test-response-payload"


@pytest.mark.trio
async def test_v51_rpc_talk_with_unsupported_protocol(make_request, bob):
    async with bob.network() as bob_network:
        with pytest.raises(Exception, match="'error':"):
            await make_request(
                "discv5_talk", [bob_network.local_node_id.hex(), "0x1234", "0x1234"]
            )


@pytest.mark.trio
async def test_v51_rpc_talk_web3_with_unsupported_protocol(make_request, bob, w3):
    async with bob.network() as bob_network:
        with pytest.raises(Exception, match="Unexpected Error"):
            await trio.to_thread.run_sync(
                w3.discv5.talk,
                bob_network.local_node_id.hex(),
                encode_hex(b"test"),
                "0x1234",
            )


@pytest.mark.parametrize(
    "endpoint", ("discv5_sendTalkRequest", "discv5_sendTalkResponse", "discv5_talk")
)
@pytest.mark.trio
async def test_v51_rpc_talk_invalid_params(
    make_request, invalid_node_id, bob, endpoint
):
    # bad node_id
    with pytest.raises(Exception, match="'error':"):
        await make_request(endpoint, [invalid_node_id, "0x12", "0x12"])

    # invalid 1st bytes arg
    with pytest.raises(Exception, match="'error':"):
        await make_request(endpoint, [bob.node_id.hex(), 1, "0x12"])
    with pytest.raises(Exception, match="'error':"):
        await make_request(endpoint, [bob.node_id.hex(), 257, "0x12"])
    with pytest.raises(Exception, match="'error':"):
        await make_request(endpoint, [bob.node_id.hex(), 1.2, "0x12"])
    with pytest.raises(Exception, match="'error':"):
        await make_request(endpoint, [bob.node_id.hex(), [], "0x12"])
    with pytest.raises(Exception, match="'error':"):
        await make_request(endpoint, [bob.node_id.hex(), "xyz", "0x12"])
    with pytest.raises(Exception, match="'error':"):
        await make_request(endpoint, [bob.node_id.hex(), [1, "2"], "0x12"])

    # invalid 2nd bytes arg
    with pytest.raises(Exception, match="'error':"):
        await make_request(endpoint, [bob.node_id.hex(), "0x12", 1])
    with pytest.raises(Exception, match="'error':"):
        await make_request(endpoint, [bob.node_id.hex(), "0x12", 257])
    with pytest.raises(Exception, match="'error':"):
        await make_request(endpoint, [bob.node_id.hex(), "0x12", 1.2])
    with pytest.raises(Exception, match="'error':"):
        await make_request(endpoint, [bob.node_id.hex(), "0x12", []])
    with pytest.raises(Exception, match="'error':"):
        await make_request(endpoint, [bob.node_id.hex(), "0x12", "xyz"])
    with pytest.raises(Exception, match="'error':"):
        await make_request(endpoint, [bob.node_id.hex(), "0x12", [1, "2"]])

    # wrong params count
    with pytest.raises(Exception, match="'error':"):
        await make_request(endpoint, [])
    with pytest.raises(Exception, match="'error':"):
        await make_request(endpoint, [bob.node_id.hex()])
    with pytest.raises(Exception, match="'error':"):
        await make_request(endpoint, [bob.node_id.hex(), "0x12"])
    with pytest.raises(Exception, match="'error':"):
        await make_request(endpoint, [bob.node_id.hex(), "0x12", "0x12", "0x12"])


@pytest.mark.trio
async def test_v51_rpc_recursiveFindNodes(tester, bob, make_request):
    async with AsyncExitStack() as stack:
        await stack.enter_async_context(bob.network())
        bootnodes = collections.deque((bob.enr,), maxlen=4)
        nodes = [bob]
        target_node_id = None
        for _ in range(8):
            node = tester.node()
            nodes.append(node)
            await stack.enter_async_context(node.network(bootnodes=bootnodes))
            bootnodes.append(node.enr)
            if (
                not target_node_id
                and compute_log_distance(node.node_id, bob.node_id) < 256
            ):
                target_node_id = node.node_id

        # give the the network some time to interconnect.
        with trio.fail_after(60):
            for _ in range(1000):
                await trio.lowlevel.checkpoint()

        await make_request("discv5_bond", [bob.node_id.hex()])

        with trio.fail_after(60):
            found_enrs = await make_request(
                "discv5_recursiveFindNodes", [target_node_id.hex()]
            )

        found_enrs = tuple(ENR.from_repr(enr_repr).node_id for enr_repr in found_enrs)
        assert len(found_enrs) > 0


@pytest.mark.trio
async def test_v51_rpc_recursiveFindNodes_web3(tester, bob, w3):
    async with AsyncExitStack() as stack:
        await stack.enter_async_context(bob.network())
        bootnodes = collections.deque((bob.enr,), maxlen=4)
        nodes = [bob]
        target_node_id = None
        for _ in range(8):
            node = tester.node()
            nodes.append(node)
            await stack.enter_async_context(node.network(bootnodes=bootnodes))
            bootnodes.append(node.enr)
            if (
                not target_node_id
                and compute_log_distance(node.node_id, bob.node_id) < 256
            ):
                target_node_id = node.node_id

        # give the the network some time to interconnect.
        with trio.fail_after(60):
            for _ in range(1000):
                await trio.lowlevel.checkpoint()

        await trio.to_thread.run_sync(
            w3.discv5.bond, bob.node_id.hex(),
        )

        with trio.fail_after(60):
            found_enrs = await trio.to_thread.run_sync(
                w3.discv5.recursive_find_nodes, target_node_id
            )

        # Ensure that one of the three closest node ids was in the returned node ids
        assert len(found_enrs) > 0
        assert isinstance(found_enrs[0], ENR)


@pytest.mark.trio
async def test_v51_rpc_recursive_find_nodes_with_invalid_node_id(
    request, make_request, invalid_node_id
):
    if "unknown-endpoint" not in repr(request):
        with pytest.raises(Exception, match="'error':"):
            await make_request("discv5_recursiveFindNodes", [invalid_node_id])


@pytest.mark.trio
async def test_v51_rpc_bond(make_request, bob_node_id_param):
    bob_response = await make_request("discv5_bond", [bob_node_id_param])

    assert bob_response is True


@pytest.mark.trio
async def test_v51_rpc_bond_web3(bob_node_id_param_w3, w3):
    response = await trio.to_thread.run_sync(w3.discv5.bond, bob_node_id_param_w3,)

    assert response.value is True
