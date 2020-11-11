from socket import inet_ntoa
from typing import Any, Iterable, List, Optional, Tuple, TypedDict

from eth_typing import HexStr, NodeID
from eth_utils import encode_hex, is_list_like, to_dict

from ddht.abc import RPCHandlerAPI
from ddht.endpoint import Endpoint
from ddht.rpc import RPCError, RPCHandler, RPCRequest
from ddht.v5_1.abc import NetworkAPI
from ddht.validation import (
    validate_and_extract_destination,
    validate_and_normalize_distances,
    validate_params_length,
)


class PongResponse(TypedDict):
    enr_seq: int
    packet_ip: str
    packet_port: int


class SendPingResponse(TypedDict):
    request_id: HexStr


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
        return SendPingResponse(request_id=encode_hex(request_id))


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
