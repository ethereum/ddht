from typing import Any, Iterable, Iterator, Mapping, Optional, Tuple, TypedDict, Union

from eth_enr import ENRAPI
from eth_enr.abc import ENRManagerAPI
from eth_enr.typing import ENR_KV
from eth_typing import HexStr
from eth_utils import (
    encode_hex,
    is_hex,
    is_integer,
    is_text,
    to_bytes,
    to_dict,
    to_tuple,
)

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

        raw_key, raw_value = kv_pair
        if not is_hex(raw_key):
            raise RPCError(
                f"Key: {raw_key} is type: {type(raw_key)}. Keys must be hex-encoded strings."
            )
        key = to_bytes(hexstr=raw_key)

        value: Union[bytes, None]
        if not raw_value:
            value = None
        elif is_integer(raw_value):
            value = to_bytes(raw_value)
        elif is_text(raw_value) and is_hex(raw_value):
            value = to_bytes(hexstr=raw_value)
        else:
            raise RPCError(
                f"Value: {raw_value} is type: {type(raw_value)}. "
                "Values must be hex-str, integer, or None."
            )

        yield key, value


class UpdateNodeInfoHandler(RPCHandler[Tuple[ENR_KV, ...], NodeInfoResponse]):
    _node_id_hex: HexStr

    def __init__(self, enr_manager: ENRManagerAPI) -> None:
        self._enr_manager = enr_manager

    def extract_params(self, request: RPCRequest) -> Tuple[ENR_KV, ...]:
        raw_params = extract_params(request)
        kv_pairs = normalize_and_validate_kv_pairs(raw_params)
        return kv_pairs

    async def do_call(self, params: Tuple[ENR_KV, ...]) -> NodeInfoResponse:
        self._enr_manager.update(*params)
        return NodeInfoResponse(
            node_id=encode_hex(self._enr_manager.enr.node_id),
            enr=repr(self._enr_manager.enr),
        )


@to_dict
def get_core_rpc_handlers(
    enr_manager: ENRManagerAPI, routing_table: RoutingTableAPI
) -> Iterator[Tuple[str, RPCHandlerAPI]]:
    yield ("discv5_routingTableInfo", RoutingTableInfoHandler(routing_table))
    yield ("discv5_nodeInfo", NodeInfoHandler(enr_manager.enr))
    yield ("discv5_updateNodeInfo", UpdateNodeInfoHandler(enr_manager))
