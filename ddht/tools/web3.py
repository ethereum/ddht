from typing import Callable, Mapping, NamedTuple, Tuple

try:
    import web3  # noqa: F401
except ImportError:
    raise ImportError("The web3.py library is required")


from eth_enr import ENR, ENRAPI
from eth_typing import NodeID
from eth_utils import decode_hex
from web3.method import Method
from web3.module import ModuleV2
from web3.types import RPCEndpoint

from ddht.rpc_handlers import BucketInfo as BucketInfoDict
from ddht.rpc_handlers import NodeInfoResponse, TableInfoResponse


class NodeInfo(NamedTuple):
    node_id: NodeID
    enr: ENRAPI

    @classmethod
    def from_rpc_response(cls, response: NodeInfoResponse) -> "NodeInfo":
        return cls(
            node_id=NodeID(decode_hex(response["node_id"])),
            enr=ENR.from_repr(response["enr"]),
        )


class BucketInfo(NamedTuple):
    idx: int
    nodes: Tuple[NodeID, ...]
    replacement_cache: Tuple[NodeID, ...]
    is_full: bool

    @classmethod
    def from_rpc_response(cls, response: BucketInfoDict) -> "BucketInfo":
        return cls(
            idx=response["idx"],
            nodes=tuple(
                NodeID(decode_hex(node_id_hex)) for node_id_hex in response["nodes"]
            ),
            replacement_cache=tuple(
                NodeID(decode_hex(node_id_hex))
                for node_id_hex in response["replacement_cache"]
            ),
            is_full=response["is_full"],
        )


class TableInfo(NamedTuple):
    center_node_id: NodeID
    num_buckets: int
    bucket_size: int
    buckets: Mapping[int, BucketInfo]

    @classmethod
    def from_rpc_response(cls, response: TableInfoResponse) -> "TableInfo":
        return cls(
            center_node_id=NodeID(decode_hex(response["center_node_id"])),
            num_buckets=response["num_buckets"],
            bucket_size=response["bucket_size"],
            buckets={
                int(idx): BucketInfo.from_rpc_response(bucket_stats)
                for idx, bucket_stats in response["buckets"].items()
            },
        )


class RPC:
    nodeInfo = RPCEndpoint("discv5_nodeInfo")
    routingTableInfo = RPCEndpoint("discv5_routingTableInfo")


# TODO: why does mypy think ModuleV2 is of `Any` type?
class DiscoveryV5Module(ModuleV2):  # type: ignore
    get_node_info: Method[Callable[[], NodeInfo]] = Method(
        RPC.nodeInfo, result_formatters=lambda method: NodeInfo.from_rpc_response,
    )
    get_routing_table_info: Method[Callable[[], TableInfo]] = Method(
        RPC.routingTableInfo,
        result_formatters=lambda method: TableInfo.from_rpc_response,
    )
