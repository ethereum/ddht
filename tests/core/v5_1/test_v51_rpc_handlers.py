import ipaddress
from socket import inet_ntoa

from async_service import background_trio_service
from eth_enr import ENR
from eth_enr.tools.factories import ENRFactory
from eth_utils import encode_hex
import pytest
import trio
from web3 import IPCProvider, Web3

from ddht.kademlia import compute_log_distance
from ddht.rpc import RPCServer
from ddht.tools.factories.node_id import NodeIDFactory
from ddht.tools.web3 import DiscoveryV5Module
from ddht.v5_1.messages import PingMessage
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
    alice.enr_db.set_enr(bob.enr)

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
def invalid_node_id(request, alice, bob, bob_network):
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
    alice.enr_db.set_enr(bob.enr)

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


@pytest.mark.trio
async def test_v51_rpc_ping(make_request, bob_node_id_param, alice, bob):
    pong = await make_request("discv5_ping", [bob_node_id_param])
    assert pong["enr_seq"] == bob.enr.sequence_number
    assert pong["packet_ip"] == inet_ntoa(alice.endpoint.ip_address)
    assert pong["packet_port"] == alice.endpoint.port


@pytest.mark.trio
async def test_v51_rpc_ping_invalid_node_id(make_request, invalid_node_id):
    with pytest.raises(Exception, match="'error':"):
        await make_request("discv5_ping", [invalid_node_id])


@pytest.mark.trio
async def test_v51_rpc_ping_missing_node_id(make_request):
    with pytest.raises(Exception, match="'error':"):
        await make_request("discv5_ping", [])


@pytest.mark.trio
async def test_v51_rpc_ping_web3(make_request, bob_node_id_param_w3, alice, bob, w3):
    pong = await trio.to_thread.run_sync(w3.discv5.ping, bob_node_id_param_w3)
    assert pong.enr_seq == bob.enr.sequence_number
    assert pong.packet_ip == ipaddress.ip_address(alice.endpoint.ip_address)
    assert pong.packet_port == alice.endpoint.port


@pytest.mark.trio
async def test_v51_rpc_send_ping(make_request, bob_node_id_param, bob_network):
    async with bob_network.client.dispatcher.subscribe(PingMessage) as subscription:
        response = await make_request("discv5_sendPing", [bob_node_id_param])
        with trio.fail_after(2):
            request = await subscription.receive()
        assert encode_hex(request.message.request_id) == response["request_id"]


@pytest.mark.trio
async def test_v51_rpc_send_ping_invalid_node_id(make_request, invalid_node_id):
    with pytest.raises(Exception, match="'error':"):
        await make_request("discv5_sendPing", [invalid_node_id])


@pytest.mark.trio
async def test_v51_rpc_send_ping_missing_node_id(make_request):
    with pytest.raises(Exception, match="'error':"):
        await make_request("discv5_sendPing", [])


@pytest.mark.trio
async def test_v51_rpc_send_ping_web3(
    make_request, w3, bob_network, bob_node_id_param_w3
):
    async with bob_network.client.dispatcher.subscribe(PingMessage) as subscription:
        response = await trio.to_thread.run_sync(
            w3.discv5.send_ping, bob_node_id_param_w3
        )
        with trio.fail_after(2):
            request = await subscription.receive()
    assert encode_hex(request.message.request_id) == response.request_id


@pytest.mark.trio
async def test_v51_rpc_findNodes(make_request, bob_node_id_param, alice, bob):
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


@pytest.mark.trio
async def test_v51_rpc_findNodes_invalid_params(
    make_request, invalid_node_id, alice, bob
):
    # bad node_id
    with pytest.raises(Exception, match="'error':"):
        await make_request("discv5_findNodes", [invalid_node_id, 0])
    with pytest.raises(Exception, match="'error':"):
        await make_request("discv5_findNodes", [invalid_node_id, [0]])

    # invalid distances
    with pytest.raises(Exception, match="'error':"):
        await make_request("discv5_findNodes", [bob.node_id.hex(), -1])
    with pytest.raises(Exception, match="'error':"):
        await make_request("discv5_findNodes", [bob.node_id.hex(), 257])
    with pytest.raises(Exception, match="'error':"):
        await make_request("discv5_findNodes", [bob.node_id.hex(), 1.2])
    with pytest.raises(Exception, match="'error':"):
        await make_request("discv5_findNodes", [bob.node_id.hex(), []])
    with pytest.raises(Exception, match="'error':"):
        await make_request("discv5_findNodes", [bob.node_id.hex(), "1"])
    with pytest.raises(Exception, match="'error':"):
        await make_request("discv5_findNodes", [bob.node_id.hex(), [1, "2"]])

    # wrong params count
    with pytest.raises(Exception, match="'error':"):
        await make_request("discv5_findNodes", [])
    with pytest.raises(Exception, match="'error':"):
        await make_request("discv5_findNodes", [bob.node_id.hex()])
    with pytest.raises(Exception, match="'error':"):
        await make_request("discv5_findNodes", [bob.node_id.hex(), 0, "extra"])


@pytest.mark.trio
async def test_v51_rpc_findNodes_w3(make_request, bob_node_id_param, alice, bob, w3):
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
