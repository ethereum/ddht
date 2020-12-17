from socket import inet_ntoa
from typing import Any, Iterable, List, Optional, Sequence, Tuple, TypedDict

from eth_enr import ENR
from eth_enr.abc import ENRAPI
from eth_enr.exceptions import OldSequenceNumber
from eth_typing import HexStr, NodeID
from eth_utils import (
    ValidationError,
    decode_hex,
    encode_hex,
    is_list_like,
    to_bytes,
    to_dict,
)

from ddht.abc import RPCHandlerAPI
from ddht.endpoint import Endpoint
from ddht.kademlia import compute_distance
from ddht.rpc import RPCError, RPCHandler, RPCRequest
from ddht.v5_1.abc import NetworkAPI
from ddht.validation import (
    validate_and_convert_hexstr,
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


class GetENRResponse(TypedDict):
    enr_repr: str


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


class SendPongHandler(RPCHandler[Tuple[NodeID, Optional[Endpoint], HexStr], None]):
    def __init__(self, network: NetworkAPI) -> None:
        self._network = network

    def extract_params(
        self, request: RPCRequest
    ) -> Tuple[NodeID, Optional[Endpoint], HexStr]:
        raw_params = extract_params(request)
        validate_params_length(raw_params, 2)
        raw_destination, request_id = raw_params
        node_id, endpoint = validate_and_extract_destination(raw_destination)
        return node_id, endpoint, request_id

    async def do_call(self, params: Tuple[NodeID, Optional[Endpoint], HexStr]) -> None:
        node_id, endpoint, request_id = params
        if endpoint is None:
            enr = await self._network.lookup_enr(node_id)
            endpoint = Endpoint.from_enr(enr)
        response = await self._network.client.send_pong(
            node_id, endpoint, request_id=decode_hex(request_id)
        )
        return response


FindNodesRPCParams = Tuple[NodeID, Optional[Endpoint], Tuple[int, ...]]


class FindNodesHandler(RPCHandler[FindNodesRPCParams, Tuple[str, ...]]):
    def __init__(self, network: NetworkAPI) -> None:
        self._network = network

    def extract_params(self, request: RPCRequest) -> FindNodesRPCParams:
        raw_params = extract_params(request)
        validate_params_length(raw_params, 2)
        raw_destination, raw_distances = raw_params
        node_id, endpoint = validate_and_extract_destination(raw_destination)
        distances = validate_and_normalize_distances(raw_distances)

        return node_id, endpoint, distances

    async def do_call(self, params: FindNodesRPCParams) -> Tuple[str, ...]:
        node_id, endpoint, distances = params
        enrs = await self._network.find_nodes(node_id, *distances, endpoint=endpoint)
        return tuple(repr(enr) for enr in enrs)


class SendFindNodesHandler(RPCHandler[FindNodesRPCParams, HexStr]):
    def __init__(self, network: NetworkAPI) -> None:
        self._network = network

    def extract_params(self, request: RPCRequest) -> FindNodesRPCParams:
        raw_params = extract_params(request)
        validate_params_length(raw_params, 2)
        raw_destination, raw_distances = raw_params
        node_id, endpoint = validate_and_extract_destination(raw_destination)
        distances = validate_and_normalize_distances(raw_distances)
        return node_id, endpoint, distances

    async def do_call(self, params: FindNodesRPCParams) -> HexStr:
        node_id, endpoint, distances = params
        if endpoint is None:
            enr = await self._network.lookup_enr(node_id)
            endpoint = Endpoint.from_enr(enr)
        request_id = await self._network.client.send_find_nodes(
            node_id, endpoint, distances=distances
        )
        return encode_hex(request_id)


SendFoundNodesRPCParams = Tuple[NodeID, Optional[Endpoint], Sequence[ENRAPI], bytes]


class SendFoundNodesHandler(RPCHandler[SendFoundNodesRPCParams, int]):
    def __init__(self, network: NetworkAPI) -> None:
        self._network = network

    def extract_params(self, request: RPCRequest) -> SendFoundNodesRPCParams:
        raw_params = extract_params(request)
        validate_params_length(raw_params, 3)
        raw_destination, raw_enrs, raw_request_id = raw_params
        node_id, endpoint = validate_and_extract_destination(raw_destination)
        enrs = [ENR.from_repr(enr) for enr in raw_enrs]
        request_id = to_bytes(hexstr=raw_request_id)
        return node_id, endpoint, enrs, request_id

    async def do_call(self, params: SendFoundNodesRPCParams) -> int:
        node_id, endpoint, enrs, request_id = params
        if endpoint is None:
            enr = await self._network.lookup_enr(node_id)
            endpoint = Endpoint.from_enr(enr)
        num_batches = await self._network.client.send_found_nodes(
            node_id, endpoint, enrs=enrs, request_id=request_id
        )
        return num_batches


TalkRPCParams = Tuple[NodeID, Optional[Endpoint], bytes, bytes]


class SendTalkRequestHandler(RPCHandler[TalkRPCParams, HexStr]):
    def __init__(self, network: NetworkAPI) -> None:
        self._network = network

    def extract_params(self, request: RPCRequest) -> TalkRPCParams:
        raw_params = extract_params(request)
        validate_params_length(raw_params, 3)
        raw_destination, raw_protocol, raw_payload = raw_params
        node_id, endpoint = validate_and_extract_destination(raw_destination)
        protocol, payload = validate_and_convert_hexstr(raw_protocol, raw_payload)
        return (
            node_id,
            endpoint,
            protocol,
            payload,
        )

    async def do_call(self, params: TalkRPCParams) -> HexStr:
        node_id, endpoint, protocol, payload = params
        if endpoint is None:
            enr = await self._network.lookup_enr(node_id)
            endpoint = Endpoint.from_enr(enr)
        message_request_id = await self._network.client.send_talk_request(
            node_id, endpoint, protocol=protocol, payload=payload,
        )
        return encode_hex(message_request_id)


class SendTalkResponseHandler(RPCHandler[TalkRPCParams, None]):
    def __init__(self, network: NetworkAPI) -> None:
        self._network = network

    def extract_params(self, request: RPCRequest) -> TalkRPCParams:
        raw_params = extract_params(request)
        validate_params_length(raw_params, 3)
        raw_destination, raw_payload, raw_request_id = raw_params
        node_id, endpoint = validate_and_extract_destination(raw_destination)
        payload, request_id = validate_and_convert_hexstr(raw_payload, raw_request_id)
        return (
            node_id,
            endpoint,
            payload,
            request_id,
        )

    async def do_call(self, params: TalkRPCParams) -> None:
        node_id, endpoint, payload, request_id = params
        if endpoint is None:
            enr = await self._network.lookup_enr(node_id)
            endpoint = Endpoint.from_enr(enr)
        response = await self._network.client.send_talk_response(
            node_id, endpoint, payload=payload, request_id=request_id,
        )
        return response


class TalkHandler(RPCHandler[TalkRPCParams, HexStr]):
    def __init__(self, network: NetworkAPI) -> None:
        self._network = network

    def extract_params(self, request: RPCRequest) -> TalkRPCParams:
        raw_params = extract_params(request)
        validate_params_length(raw_params, 3)
        raw_destination, raw_protocol, raw_payload = raw_params
        node_id, endpoint = validate_and_extract_destination(raw_destination)
        protocol, payload = validate_and_convert_hexstr(raw_protocol, raw_payload)
        return (
            node_id,
            endpoint,
            protocol,
            payload,
        )

    async def do_call(self, params: TalkRPCParams) -> HexStr:
        node_id, endpoint, protocol, payload = params
        if endpoint is None:
            enr = await self._network.lookup_enr(node_id)
            endpoint = Endpoint.from_enr(enr)
        response = await self._network.talk(
            node_id, protocol=protocol, payload=payload, endpoint=endpoint,
        )
        return encode_hex(response)


class RecursiveFindNodesHandler(RPCHandler[NodeID, Tuple[str, ...]]):
    def __init__(self, network: NetworkAPI) -> None:
        self._network = network

    def extract_params(self, request: RPCRequest) -> NodeID:
        raw_params = extract_params(request)
        validate_params_length(raw_params, 1)
        raw_destination = raw_params[0]
        node_id, _ = validate_and_extract_destination(raw_destination)
        return node_id

    async def do_call(self, params: NodeID) -> Tuple[str, ...]:
        node_id = params
        async with self._network.recursive_find_nodes(node_id) as enr_aiter:
            found_nodes = tuple(
                sorted(
                    [enr async for enr in enr_aiter],
                    key=lambda enr: compute_distance(node_id, enr.node_id),
                )
            )
        return tuple(repr(node) for node in found_nodes)


class BondHandler(RPCHandler[Tuple[NodeID, Optional[Endpoint]], int]):
    def __init__(self, network: NetworkAPI) -> None:
        self._network = network

    def extract_params(self, request: RPCRequest) -> Tuple[NodeID, Optional[Endpoint]]:
        raw_params = extract_params(request)
        validate_params_length(raw_params, 1)
        raw_destination = raw_params[0]
        node_id, endpoint = validate_and_extract_destination(raw_destination)
        return node_id, endpoint

    async def do_call(self, params: Tuple[NodeID, Optional[Endpoint]]) -> bool:
        node_id, endpoint = params
        return await self._network.bond(node_id, endpoint=endpoint)


class GetENRHandler(RPCHandler[NodeID, GetENRResponse]):
    def __init__(self, network: NetworkAPI) -> None:
        self._network = network

    def extract_params(self, request: RPCRequest) -> NodeID:
        raw_params = extract_params(request)
        validate_params_length(raw_params, 1)
        raw_destination = raw_params[0]
        node_id, _ = validate_and_extract_destination(raw_destination)
        return node_id

    async def do_call(self, params: NodeID) -> GetENRResponse:
        response = self._network.enr_db.get_enr(params)
        return GetENRResponse(enr_repr=repr(response))


class SetENRHandler(RPCHandler[ENRAPI, None]):
    def __init__(self, network: NetworkAPI) -> None:
        self._network = network

    def extract_params(self, request: RPCRequest) -> ENRAPI:
        raw_params = extract_params(request)
        validate_params_length(raw_params, 1)
        enr_repr = raw_params[0]
        try:
            enr = ENR.from_repr(enr_repr)
        except ValidationError:
            raise RPCError(f"Invalid ENR repr: {enr_repr}")
        return enr

    async def do_call(self, params: ENRAPI) -> None:
        try:
            self._network.enr_db.set_enr(params)
        except OldSequenceNumber as exc:
            raise RPCError(f"Invalid ENR, outdated sequence number: {exc}.")
        return None


class DeleteENRHandler(RPCHandler[NodeID, None]):
    def __init__(self, network: NetworkAPI) -> None:
        self._network = network

    def extract_params(self, request: RPCRequest) -> NodeID:
        raw_params = extract_params(request)
        validate_params_length(raw_params, 1)
        raw_destination = raw_params[0]
        node_id, _ = validate_and_extract_destination(raw_destination)
        return node_id

    async def do_call(self, params: NodeID) -> None:
        self._network.enr_db.delete_enr(params)
        return None


class LookupENRHandler(
    RPCHandler[Tuple[NodeID, Optional[Endpoint], int], GetENRResponse]
):
    def __init__(self, network: NetworkAPI) -> None:
        self._network = network

    def extract_params(
        self, request: RPCRequest
    ) -> Tuple[NodeID, Optional[Endpoint], int]:
        raw_params = extract_params(request)
        if len(raw_params) == 1:
            raw_destination = raw_params[0]
            raw_sequence = 0
        elif len(raw_params) == 2:
            raw_destination, raw_sequence = raw_params
        else:
            raise RPCError("Invalid params for discv5_lookupENR request.")
        node_id, endpoint = validate_and_extract_destination(raw_destination)
        sequence_number = raw_sequence if raw_sequence else 0
        return node_id, endpoint, sequence_number

    async def do_call(
        self, params: Tuple[NodeID, Optional[Endpoint], int]
    ) -> GetENRResponse:
        node_id, endpoint, sequence_number = params
        response = await self._network.lookup_enr(
            node_id, enr_seq=sequence_number, endpoint=endpoint
        )
        return GetENRResponse(enr_repr=repr(response))


@to_dict
def get_v51_rpc_handlers(network: NetworkAPI) -> Iterable[Tuple[str, RPCHandlerAPI]]:
    yield ("discv5_bond", BondHandler(network))
    yield ("discv5_deleteENR", DeleteENRHandler(network))
    yield ("discv5_findNodes", FindNodesHandler(network))
    yield ("discv5_getENR", GetENRHandler(network))
    yield ("discv5_lookupENR", LookupENRHandler(network))
    yield ("discv5_ping", PingHandler(network))
    yield ("discv5_recursiveFindNodes", RecursiveFindNodesHandler(network))
    yield ("discv5_sendFindNodes", SendFindNodesHandler(network))
    yield ("discv5_sendFoundNodes", SendFoundNodesHandler(network))
    yield ("discv5_sendPing", SendPingHandler(network))
    yield ("discv5_sendPong", SendPongHandler(network))
    yield ("discv5_sendTalkRequest", SendTalkRequestHandler(network))
    yield ("discv5_sendTalkResponse", SendTalkResponseHandler(network))
    yield ("discv5_setENR", SetENRHandler(network))
    yield ("discv5_talk", TalkHandler(network))
