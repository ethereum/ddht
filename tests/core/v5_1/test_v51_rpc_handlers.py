import ipaddress
from socket import inet_ntoa

from async_service import background_trio_service
from eth_utils import encode_hex
import pytest
import trio
from web3 import IPCProvider, Web3

from ddht.rpc import RPCServer
from ddht.tools.web3 import DiscoveryV5Module
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
def ping_params(request, alice, bob, bob_network):
    alice.enr_db.set_enr(bob.enr)

    if request.param == "nodeid":
        return [bob.node_id.hex()]
    elif request.param == "enode":
        return [f"enode://{bob.node_id.hex()}@{bob.endpoint}"]
    elif request.param == "enr":
        return [repr(bob.enr)]
    else:
        raise Exception(f"Unhandled param: {request.param}")


@pytest.mark.trio
async def test_rpc_ping(make_request, ping_params, alice, bob):
    pong = await make_request("discv5_ping", ping_params)
    assert pong["enr_seq"] == bob.enr.sequence_number
    assert pong["packet_ip"] == inet_ntoa(alice.endpoint.ip_address)
    assert pong["packet_port"] == alice.endpoint.port


@pytest.fixture(params=("nodeid", "nodeid-hex", "enode", "enr", "enr-repr"))
def ping_param_w3(request, alice, bob, bob_network):
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
async def test_rpc_ping_web3(make_request, ping_param_w3, alice, bob, w3):
    pong = await trio.to_thread.run_sync(w3.discv5.ping, ping_param_w3)
    assert pong.enr_seq == bob.enr.sequence_number
    assert pong.packet_ip == ipaddress.ip_address(alice.endpoint.ip_address)
    assert pong.packet_port == alice.endpoint.port
