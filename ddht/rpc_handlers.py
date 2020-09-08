from typing import Iterator, Mapping, Tuple, TypedDict

from eth_enr import ENRAPI
from eth_typing import HexStr
from eth_utils import encode_hex, to_dict

from ddht.abc import RoutingTableAPI, RPCHandlerAPI, RPCRequest
from ddht.rpc import RPCError, RPCHandler


class BucketInfo(TypedDict):
    idx: int
    nodes: Tuple[HexStr, ...]
    replacement_cache: Tuple[HexStr, ...]
    is_full: bool


class TableInfoResponse(TypedDict):
    center_node_id: HexStr
    num_buckets: int
    bucket_size: int
    buckets: Mapping[int, BucketInfo]


class RoutingTableInfoHandler(RPCHandler[None, TableInfoResponse]):
    def __init__(self, routing_table: RoutingTableAPI) -> None:
        self._routing_table = routing_table

    def extract_params(self, request: RPCRequest) -> None:
        if request.get("params"):
            raise RPCError(f"Unexpected RPC params: {request['params']}",)
        return None

    async def do_call(self, params: None) -> TableInfoResponse:
        stats = TableInfoResponse(
            center_node_id=encode_hex(self._routing_table.center_node_id),
            num_buckets=len(self._routing_table.buckets),
            bucket_size=self._routing_table.bucket_size,
            buckets=self._bucket_stats(),
        )
        return stats

    @to_dict
    def _bucket_stats(self) -> Iterator[Tuple[int, BucketInfo]]:
        buckets_and_replacement_caches = zip(
            self._routing_table.buckets, self._routing_table.replacement_caches,
        )
        for idx, (bucket, replacement_cache) in enumerate(
            buckets_and_replacement_caches, 1
        ):
            if bucket:
                yield (
                    idx,
                    BucketInfo(
                        idx=idx,
                        nodes=tuple(encode_hex(node_id) for node_id in bucket),
                        replacement_cache=tuple(
                            encode_hex(node_id) for node_id in replacement_cache
                        ),
                        is_full=(len(bucket) >= self._routing_table.bucket_size),
                    ),
                )


class NodeInfoResponse(TypedDict):
    node_id: HexStr
    enr: str


class NodeInfoHandler(RPCHandler[None, NodeInfoResponse]):
    _node_id_hex: HexStr

    def __init__(self, enr: ENRAPI) -> None:
        self._enr = enr

    def extract_params(self, request: RPCRequest) -> None:
        if request.get("params"):
            raise RPCError(f"Unexpected RPC params: {request['params']}")
        return None

    async def do_call(self, params: None) -> NodeInfoResponse:
        return NodeInfoResponse(
            node_id=encode_hex(self._enr.node_id), enr=repr(self._enr),
        )


@to_dict
def get_core_rpc_handlers(
    enr: ENRAPI, routing_table: RoutingTableAPI
) -> Iterator[Tuple[str, RPCHandlerAPI]]:
    yield ("discv5_routingTableInfo", RoutingTableInfoHandler(routing_table))
    yield ("discv5_nodeInfo", NodeInfoHandler(enr))
