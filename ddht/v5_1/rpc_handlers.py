import ipaddress
from socket import inet_ntoa
from typing import Any, Iterable, List, Optional, Tuple, TypedDict

from eth_enr import ENR
from eth_typing import HexStr, NodeID
from eth_utils import (
    decode_hex,
    is_hex,
    is_integer,
    is_list_like,
    remove_0x_prefix,
    to_dict,
    to_int,
)

from ddht.abc import RPCHandlerAPI
from ddht.endpoint import Endpoint
from ddht.rpc import RPCError, RPCHandler, RPCRequest
from ddht.v5_1.abc import NetworkAPI


class PongResponse(TypedDict):
    enr_seq: int
    packet_ip: str
    packet_port: int


class SendPingResponse(TypedDict):
    request_id: int


def extract_params(request: RPCRequest) -> List[Any]:
    try:
        params = request["params"]
    except KeyError:
        raise RPCError("Request missing `params` key")

    if not is_list_like(params):
        raise RPCError(
            f"Params must be list-like: params-type={type(params)} params={params}"
        )

    return params


def validate_params_length(params: List[Any], length: int) -> None:
    if len(params) != length:
        raise RPCError(
            f"Endpoint expects {length} params: length={len(params)} "
            f"params={params}"
        )


def is_hex_node_id(value: Any) -> bool:
    return (
        isinstance(value, str)
        and is_hex(value)
        and len(remove_0x_prefix(HexStr(value))) == 64
    )


def validate_hex_node_id(value: Any) -> None:
    if not is_hex_node_id(value):
        raise RPCError(f"Invalid NodeID: {value}")


def is_endpoint(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    ip_address, _, port = value.rpartition(":")
    try:
        ipaddress.ip_address(ip_address)
    except ValueError:
        return False

    if not port.isdigit():
        return False

    return True


def validate_endpoint(value: Any) -> None:
    if not is_endpoint(value):
        raise RPCError(f"Invalid Endpoint: {value}")


def validate_and_extract_destination(value: Any) -> Tuple[NodeID, Optional[Endpoint]]:
    node_id: NodeID
    endpoint: Optional[Endpoint]

    if is_hex_node_id(value):
        node_id = NodeID(decode_hex(value))
        endpoint = None
    elif value.startswith("enode://"):
        raw_node_id, _, raw_endpoint = value[8:].partition("@")

        validate_hex_node_id(raw_node_id)
        validate_endpoint(raw_endpoint)

        node_id = NodeID(decode_hex(raw_node_id))

        raw_ip_address, _, raw_port = raw_endpoint.partition(":")
        ip_address = ipaddress.ip_address(raw_ip_address)
        port = int(raw_port)
        endpoint = Endpoint(ip_address.packed, port)
    elif value.startswith("enr:"):
        enr = ENR.from_repr(value)
        node_id = enr.node_id
        endpoint = Endpoint.from_enr(enr)
    else:
        raise RPCError(f"Unrecognized node identifier: {value}")

    return node_id, endpoint


class PingHandler(RPCHandler[Tuple[NodeID, Optional[Endpoint]], PongResponse]):
    def __init__(self, network: NetworkAPI) -> None:
        self._network = network

    def extract_params(self, request: RPCRequest) -> Tuple[NodeID, Optional[Endpoint]]:
        raw_params = extract_params(request)

        validate_params_length(raw_params, 1)

        raw_destination = raw_params[0]

        node_id, endpoint = validate_and_extract_destination(raw_destination)

        return node_id, endpoint

    async def do_call(self, params: Tuple[NodeID, Optional[Endpoint]]) -> PongResponse:
        node_id, endpoint = params
        pong = await self._network.ping(node_id, endpoint=endpoint)
        return PongResponse(
            enr_seq=pong.enr_seq,
            packet_ip=inet_ntoa(pong.packet_ip),
            packet_port=pong.packet_port,
        )


class SendPingHandler(RPCHandler[Tuple[NodeID, Optional[Endpoint]], SendPingResponse]):
    def __init__(self, network: NetworkAPI) -> None:
        self._network = network

    def extract_params(self, request: RPCRequest) -> Tuple[NodeID, Optional[Endpoint]]:
        raw_params = extract_params(request)
        validate_params_length(raw_params, 1)
        raw_destination = raw_params[0]
        node_id, endpoint = validate_and_extract_destination(raw_destination)
        return node_id, endpoint

    async def do_call(
        self, params: Tuple[NodeID, Optional[Endpoint]]
    ) -> SendPingResponse:
        node_id, endpoint = params
        if endpoint is None:
            enr = await self._network.lookup_enr(node_id)
            endpoint = Endpoint.from_enr(enr)
        request_id = await self._network.client.send_ping(node_id, endpoint)
        return SendPingResponse(request_id=to_int(request_id))


def _is_valid_distance(value: Any) -> bool:
    return is_integer(value) and 0 <= value <= 256


def validate_and_normalize_distances(value: Any) -> Tuple[int, ...]:
    if _is_valid_distance(value):
        return (value,)
    elif (
        is_list_like(value)
        and value
        and all(_is_valid_distance(element) for element in value)
    ):
        return tuple(value)
    else:
        raise RPCError(
            f"Distances must be either a single integer distance or a non-empty list of "
            f"distances: {value}"
        )


FindNodesRPCParams = Tuple[NodeID, Optional[Endpoint], Tuple[int, ...]]


class FindNodesHandler(RPCHandler[FindNodesRPCParams, Tuple[str, ...]]):
    def __init__(self, network: NetworkAPI) -> None:
        self._network = network

    def extract_params(self, request: RPCRequest) -> FindNodesRPCParams:
        raw_params = extract_params(request)

        if len(raw_params) != 2:
            raise RPCError(
                f"`discv5_findNodes` endpoint expects two parameter: "
                f"Got {len(raw_params)} params: {raw_params}"
            )

        raw_destination, raw_distances = raw_params

        node_id, endpoint = validate_and_extract_destination(raw_destination)
        distances = validate_and_normalize_distances(raw_distances)

        return node_id, endpoint, distances

    async def do_call(self, params: FindNodesRPCParams) -> Tuple[str, ...]:
        node_id, endpoint, distances = params
        enrs = await self._network.find_nodes(node_id, *distances, endpoint=endpoint)
        return tuple(repr(enr) for enr in enrs)


@to_dict
def get_v51_rpc_handlers(network: NetworkAPI) -> Iterable[Tuple[str, RPCHandlerAPI]]:
    yield ("discv5_ping", PingHandler(network))
    yield ("discv5_findNodes", FindNodesHandler(network))
    yield ("discv5_sendPing", SendPingHandler(network))
