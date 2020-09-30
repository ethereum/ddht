import json

from async_service import background_trio_service
from eth_enr.tools.factories import ENRFactory
from eth_utils import decode_hex
import pytest
import trio
from web3 import IPCProvider, Web3

from ddht.constants import ROUTING_TABLE_BUCKET_SIZE
from ddht.kademlia import KademliaRoutingTable
from ddht.rpc import MAXIMUM_RPC_PAYLOAD_SIZE, RPCServer
from ddht.rpc_handlers import get_core_rpc_handlers
from ddht.tools.factories.node_id import NodeIDFactory
from ddht.tools.web3 import DiscoveryV5Module


@pytest.fixture
def enr():
    return ENRFactory()


@pytest.fixture
def routing_table(enr):
    return KademliaRoutingTable(enr.node_id, ROUTING_TABLE_BUCKET_SIZE)


@pytest.fixture
async def rpc_server(ipc_path, routing_table, enr):
    server = RPCServer(ipc_path, get_core_rpc_handlers(enr, routing_table))
    async with background_trio_service(server):
        await server.wait_serving()
        yield server


@pytest.fixture
def w3(rpc_server, ipc_path):
    return Web3(IPCProvider(ipc_path), modules={"discv5": (DiscoveryV5Module,)})


@pytest.mark.trio
async def test_rpc_nodeInfo(make_request, enr):
    node_info = await make_request("discv5_nodeInfo")
    assert decode_hex(node_info["node_id"]) == enr.node_id
    assert node_info["enr"] == repr(enr)


@pytest.mark.parametrize(
    "raw_request", ("just-a-raw-string",),
)
@pytest.mark.trio
async def test_rpc_closes_connection_on_bad_data(make_raw_request, raw_request):
    response = await make_raw_request(raw_request)
    assert "error" in response

    with pytest.raises(ConnectionResetError):
        try:
            await make_raw_request("should-not-work")
        except trio.TooSlowError as err:
            raise ConnectionResetError(str(err))


@pytest.mark.parametrize(
    "raw_request", ("just-a-raw-string",),
)
@pytest.mark.trio
async def test_rpc_closes_connection_on_too_large_data(make_raw_request, raw_request):
    too_long_string = "too-long-string:" + "0" * MAXIMUM_RPC_PAYLOAD_SIZE
    response = await make_raw_request(json.dumps({"key": too_long_string}))
    assert "error" in response

    with pytest.raises(ConnectionResetError):
        try:
            await make_raw_request("should-not-work")
        except trio.TooSlowError as err:
            raise ConnectionResetError(str(err))


@pytest.mark.trio
async def test_rpc_nodeInfo_web3(w3, enr, rpc_server):
    with trio.fail_after(2):
        node_info = await trio.to_thread.run_sync(w3.discv5.get_node_info)
    assert node_info.node_id == enr.node_id
    assert node_info.enr == enr


@pytest.mark.trio
async def test_rpc_tableInfo(make_request, routing_table):
    local_node_id = routing_table.center_node_id
    # 16/16 at furthest distance
    for _ in range(routing_table.bucket_size * 2):
        routing_table.update(NodeIDFactory.at_log_distance(local_node_id, 256))
    # 16/8 at next bucket
    for _ in range(int(routing_table.bucket_size * 1.5)):
        routing_table.update(NodeIDFactory.at_log_distance(local_node_id, 255))
    # 16/4 at next bucket
    for _ in range(int(routing_table.bucket_size * 1.25)):
        routing_table.update(NodeIDFactory.at_log_distance(local_node_id, 254))
    # 16 in this one
    for _ in range(int(routing_table.bucket_size)):
        routing_table.update(NodeIDFactory.at_log_distance(local_node_id, 253))
    # 8 in this one
    for _ in range(int(routing_table.bucket_size // 2)):
        routing_table.update(NodeIDFactory.at_log_distance(local_node_id, 252))
    # 4 in this one
    for _ in range(int(routing_table.bucket_size // 4)):
        routing_table.update(NodeIDFactory.at_log_distance(local_node_id, 251))

    table_info = await make_request("discv5_routingTableInfo")
    assert decode_hex(table_info["center_node_id"]) == routing_table.center_node_id
    assert table_info["bucket_size"] == routing_table.bucket_size
    assert table_info["num_buckets"] == routing_table.num_buckets
    assert len(table_info["buckets"]) == 6

    bucket_256 = table_info["buckets"]["256"]
    bucket_255 = table_info["buckets"]["255"]
    bucket_254 = table_info["buckets"]["254"]
    bucket_253 = table_info["buckets"]["253"]
    bucket_252 = table_info["buckets"]["252"]
    bucket_251 = table_info["buckets"]["251"]

    assert bucket_256["idx"] == 256
    assert bucket_256["is_full"] is True
    assert len(bucket_256["nodes"]) == routing_table.bucket_size
    assert len(bucket_256["replacement_cache"]) == routing_table.bucket_size

    assert bucket_255["idx"] == 255
    assert bucket_255["is_full"] is True
    assert len(bucket_255["nodes"]) == routing_table.bucket_size
    assert len(bucket_255["replacement_cache"]) == routing_table.bucket_size // 2

    assert bucket_254["idx"] == 254
    assert bucket_254["is_full"] is True
    assert len(bucket_254["nodes"]) == routing_table.bucket_size
    assert len(bucket_254["replacement_cache"]) == routing_table.bucket_size // 4

    assert bucket_253["idx"] == 253
    assert bucket_253["is_full"] is True
    assert len(bucket_253["nodes"]) == routing_table.bucket_size
    assert not bucket_253["replacement_cache"]

    assert bucket_252["idx"] == 252
    assert bucket_252["is_full"] is False
    assert len(bucket_252["nodes"]) == routing_table.bucket_size // 2
    assert not bucket_252["replacement_cache"]

    assert bucket_251["idx"] == 251
    assert bucket_251["is_full"] is False
    assert len(bucket_251["nodes"]) == routing_table.bucket_size // 4
    assert not bucket_251["replacement_cache"]


@pytest.mark.trio
async def test_rpc_tableInfo_web3(w3, routing_table, rpc_server):
    local_node_id = routing_table.center_node_id
    # 16/16 at furthest distance
    for _ in range(routing_table.bucket_size * 2):
        routing_table.update(NodeIDFactory.at_log_distance(local_node_id, 256))
    # 16/8 at next bucket
    for _ in range(int(routing_table.bucket_size * 1.5)):
        routing_table.update(NodeIDFactory.at_log_distance(local_node_id, 255))
    # 16/4 at next bucket
    for _ in range(int(routing_table.bucket_size * 1.25)):
        routing_table.update(NodeIDFactory.at_log_distance(local_node_id, 254))
    # 16 in this one
    for _ in range(int(routing_table.bucket_size)):
        routing_table.update(NodeIDFactory.at_log_distance(local_node_id, 253))
    # 8 in this one
    for _ in range(int(routing_table.bucket_size // 2)):
        routing_table.update(NodeIDFactory.at_log_distance(local_node_id, 252))
    # 4 in this one
    for _ in range(int(routing_table.bucket_size // 4)):
        routing_table.update(NodeIDFactory.at_log_distance(local_node_id, 251))

    table_info = await trio.to_thread.run_sync(w3.discv5.get_routing_table_info)
    assert table_info.center_node_id == routing_table.center_node_id
    assert table_info.bucket_size == routing_table.bucket_size
    assert table_info.num_buckets == routing_table.num_buckets
    assert len(table_info.buckets) == 6
    bucket_256 = table_info.buckets[256]
    bucket_255 = table_info.buckets[255]
    bucket_254 = table_info.buckets[254]
    bucket_253 = table_info.buckets[253]
    bucket_252 = table_info.buckets[252]
    bucket_251 = table_info.buckets[251]

    assert bucket_256.idx == 256
    assert bucket_256.is_full is True
    assert len(bucket_256.nodes) == routing_table.bucket_size
    assert len(bucket_256.replacement_cache) == routing_table.bucket_size

    assert bucket_255.idx == 255
    assert bucket_255.is_full is True
    assert len(bucket_255.nodes) == routing_table.bucket_size
    assert len(bucket_255.replacement_cache) == routing_table.bucket_size // 2

    assert bucket_254.idx == 254
    assert bucket_254.is_full is True
    assert len(bucket_254.nodes) == routing_table.bucket_size
    assert len(bucket_254.replacement_cache) == routing_table.bucket_size // 4

    assert bucket_253.idx == 253
    assert bucket_253.is_full is True
    assert len(bucket_253.nodes) == routing_table.bucket_size
    assert not bucket_253.replacement_cache

    assert bucket_252.idx == 252
    assert bucket_252.is_full is False
    assert len(bucket_252.nodes) == routing_table.bucket_size // 2
    assert not bucket_253.replacement_cache

    assert bucket_251.idx == 251
    assert bucket_251.is_full is False
    assert len(bucket_251.nodes) == routing_table.bucket_size // 4
    assert not bucket_253.replacement_cache
