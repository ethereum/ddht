from typing import Any, Iterable, Iterator, Mapping, Optional, Tuple, TypedDict, Union

from eth_enr import ENRAPI
from eth_enr.abc import ENRManagerAPI
from eth_enr.typing import ENR_KV
from eth_typing import HexStr
from eth_utils import encode_hex, is_integer, is_text, to_bytes, to_dict, to_tuple

from ddht.abc import RoutingTableAPI, RPCHandlerAPI, RPCRequest
from ddht.rpc import RPCError, RPCHandler
from ddht.v5_1.rpc_handlers import extract_params


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
            buckets_and_replacement_caches, start=1
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


@to_tuple
def normalize_and_validate_kv_pairs(
    params: Any,
) -> Iterable[Tuple[bytes, Optional[bytes]]]:
    if not params:
        raise RPCError("Missing parameters.")

    for kv_pair in params:
        if len(kv_pair) != 2:
            raise RPCError(f"Invalid kv_pair length: {len(kv_pair)}.")

        if not is_text(kv_pair[0]):
            raise RPCError(
                f"Key: {kv_pair[0]} is type: {type(kv_pair[0])}. Keys be text."
            )
        key = to_bytes(text=kv_pair[0])

        value: Union[bytes, None]
        if is_text(kv_pair[1]):
            value = to_bytes(text=kv_pair[1])
        elif is_integer(kv_pair[1]):
            value = to_bytes(kv_pair[1])
        elif kv_pair[1] is None:
            value = None
        else:
            raise RPCError(
                f"Value: {kv_pair[1]} is type: {type(kv_pair[1])}. "
                "Values must be text, integer, or None."
            )

        yield key, value


class UpdateNodeInfoHandler(RPCHandler[Tuple[ENR_KV, ...], None]):
    _node_id_hex: HexStr

    def __init__(self, enr_manager: ENRManagerAPI) -> None:
        self._enr_manager = enr_manager

    def extract_params(self, request: RPCRequest) -> Tuple[ENR_KV, ...]:
        raw_params = extract_params(request)
        kv_pairs = normalize_and_validate_kv_pairs(raw_params)
        return kv_pairs

    async def do_call(self, params: Tuple[ENR_KV, ...]) -> None:
        self._enr_manager.update(*params)
        return None


@to_dict
def get_core_rpc_handlers(
    enr_manager: ENRManagerAPI, routing_table: RoutingTableAPI
) -> Iterator[Tuple[str, RPCHandlerAPI]]:
    yield ("discv5_routingTableInfo", RoutingTableInfoHandler(routing_table))
    yield ("discv5_nodeInfo", NodeInfoHandler(enr_manager.enr))
    yield ("discv5_updateNodeInfo", UpdateNodeInfoHandler(enr_manager))
