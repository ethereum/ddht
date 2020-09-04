import ipaddress
from typing import Iterable, Optional, Tuple, TypedDict

from eth_typing import NodeID
from eth_utils import decode_hex, to_dict

from ddht.abc import RPCHandlerAPI
from ddht.endpoint import Endpoint
from ddht.rpc import RPCError, RPCHandler, RPCRequest
from ddht.v5_1.abc import NetworkAPI


class PongResponse(TypedDict):
    enr_seq: int
    packet_ip: str
    packet_port: int


class PingHandler(RPCHandler[Tuple[NodeID, Optional[Endpoint]], PongResponse]):
    def __init__(self, network: NetworkAPI) -> None:
        self._network = network

    def extract_params(self, request: RPCRequest) -> Tuple[NodeID, Optional[Endpoint]]:
        """
        - is sequence
        - is length 1 or 2
        - item 1 is hex-string of lenght ?66?
        - item 2 if present is: <ip-address>:<port>
            - ip-address can be parsed by `ipaddress.ip_address`
            - port is numeric, cast to integer in range 0-65536
        """
        try:
            raw_params = request["params"]
        except KeyError as err:
            raise RPCError(f"Missiing call params: {err}")

        if len(raw_params) == 1:
            raw_node_id = raw_params[0]
            raw_endpoint = None
        elif len(raw_params) == 2:
            raw_node_id, raw_endpoint = raw_params
        else:
            raise RPCError(
                f"`ddht_ping` endpoint expects either one or two parameters. "
                f"Got {len(raw_params)} params: {raw_params}"
            )

        endpoint: Optional[Endpoint]

        node_id = NodeID(decode_hex(raw_node_id))

        if raw_endpoint is not None:
            raw_ip_address, _, raw_port = raw_endpoint.partition(":")
            ip_address = ipaddress.ip_address(raw_ip_address)
            port = int(raw_port)
            endpoint = Endpoint(ip_address.packed, port)
        else:
            endpoint = None

        return node_id, endpoint

    async def do_call(self, params: Tuple[NodeID, Optional[Endpoint]]) -> PongResponse:
        node_id, endpoint = params
        pong = await self._network.ping(node_id, endpoint=endpoint)
        return PongResponse(
            enr_seq=pong.message.enr_seq,
            packet_ip=str(pong.message.packet_ip),
            packet_port=pong.message.packet_port,
        )


@to_dict
def get_v51_rpc_handlers(network: NetworkAPI) -> Iterable[Tuple[str, RPCHandlerAPI]]:
    yield ("ddht_ping", PingHandler(network))
