import ipaddress
from socket import inet_ntoa
from typing import Any, Iterable, Optional, Tuple, TypedDict

from eth_enr import ENR
from eth_typing import HexStr, NodeID
from eth_utils import decode_hex, is_hex, remove_0x_prefix, to_dict

from ddht.abc import RPCHandlerAPI
from ddht.endpoint import Endpoint
from ddht.rpc import RPCError, RPCHandler, RPCRequest
from ddht.v5_1.abc import NetworkAPI


class PongResponse(TypedDict):
    enr_seq: int
    packet_ip: str
    packet_port: int


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


class PingHandler(RPCHandler[Tuple[NodeID, Optional[Endpoint]], PongResponse]):
    def __init__(self, network: NetworkAPI) -> None:
        self._network = network

    def extract_params(self, request: RPCRequest) -> Tuple[NodeID, Optional[Endpoint]]:
        try:
            raw_params = request["params"]
        except KeyError as err:
            raise RPCError(f"Missiing call params: {err}")

        if len(raw_params) != 1:
            raise RPCError(
                f"`ddht_ping` endpoint expects a single parameter: "
                f"Got {len(raw_params)} params: {raw_params}"
            )

        value = raw_params[0]

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

    async def do_call(self, params: Tuple[NodeID, Optional[Endpoint]]) -> PongResponse:
        node_id, endpoint = params
        pong = await self._network.ping(node_id, endpoint=endpoint)
        return PongResponse(
            enr_seq=pong.enr_seq,
            packet_ip=inet_ntoa(pong.packet_ip),
            packet_port=pong.packet_port,
        )


@to_dict
def get_v51_rpc_handlers(network: NetworkAPI) -> Iterable[Tuple[str, RPCHandlerAPI]]:
    yield ("discv5_ping", PingHandler(network))
