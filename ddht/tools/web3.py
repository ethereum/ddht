import ipaddress
from typing import (
    Any,
    Callable,
    List,
    Mapping,
    NamedTuple,
    Optional,
    Sequence,
    Tuple,
    TypedDict,
    Union,
)

from eth_utils import add_0x_prefix, encode_hex, remove_0x_prefix

try:
    import web3  # noqa: F401
except ImportError:
    raise ImportError("The web3.py library is required")


from eth_enr import ENR, ENRAPI
from eth_enr.typing import ENR_KV
from eth_typing import HexStr, NodeID
from eth_utils import decode_hex
from web3.method import Method
from web3.module import ModuleV2
from web3.types import RPCEndpoint

from ddht.endpoint import Endpoint
from ddht.rpc_handlers import BucketInfo as BucketInfoDict
from ddht.rpc_handlers import NodeInfoResponse, TableInfoResponse
from ddht.typing import AnyIPAddress
from ddht.v5_1.rpc_handlers import GetENRResponse, PongResponse, SendPingResponse


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


class PongPayload(NamedTuple):
    enr_seq: int
    packet_ip: AnyIPAddress
    packet_port: int

    @classmethod
    def from_rpc_response(cls, response: PongResponse) -> "PongPayload":
        return cls(
            enr_seq=response["enr_seq"],
            packet_ip=ipaddress.ip_address(response["packet_ip"]),
            packet_port=response["packet_port"],
        )


class SendPingPayload(NamedTuple):
    request_id: HexStr

    @classmethod
    def from_rpc_response(cls, response: SendPingResponse) -> "SendPingPayload":
        return cls(request_id=response["request_id"],)


class GetENRPayload(NamedTuple):
    enr: ENRAPI

    @classmethod
    def from_rpc_response(cls, response: GetENRResponse) -> "GetENRPayload":
        return cls(enr=ENR.from_repr(response["enr_repr"]))


class UpdateENRPayload(NamedTuple):
    enr: ENRAPI

    @classmethod
    def from_rpc_response(cls, response: NodeInfoResponse) -> "UpdateENRPayload":
        return cls(enr=ENR.from_repr(response["enr"]))


class EmptyResponse(TypedDict):
    pass


class EmptyPayload(NamedTuple):
    @classmethod
    def from_rpc_response(cls, response: EmptyResponse) -> None:
        return None


class RPC:
    nodeInfo = RPCEndpoint("discv5_nodeInfo")
    updateNodeInfo = RPCEndpoint("discv5_updateNodeInfo")
    routingTableInfo = RPCEndpoint("discv5_routingTableInfo")
    getENR = RPCEndpoint("discv5_getENR")
    setENR = RPCEndpoint("discv5_setENR")
    deleteENR = RPCEndpoint("discv5_deleteENR")
    lookupENR = RPCEndpoint("discv5_lookupENR")

    ping = RPCEndpoint("discv5_ping")
    sendPing = RPCEndpoint("discv5_sendPing")
    sendPong = RPCEndpoint("discv5_sendPong")
    findNodes = RPCEndpoint("discv5_findNodes")


NodeIDIdentifier = Union[ENRAPI, str, bytes, NodeID, HexStr]


def normalize_node_id_identifier(identifier: NodeIDIdentifier) -> str:
    """
    Normalizes any of the following inputs into the appropriate payload for
    representing a `NodeID` over a JSON-RPC API endpoint.

    - An ENR object
    - The string representation of an ENR
    - A NodeID in the form of a bytestring
    - A NodeID in the form of a hex string
    - An ENode URI

    Throws a ``ValueError`` if the input cannot be matched to one of these
    formats.
    """
    if isinstance(identifier, ENRAPI):
        return repr(identifier)
    elif isinstance(identifier, bytes):
        if len(identifier) == 32:
            return encode_hex(identifier)
        raise ValueError(f"Unrecognized node identifier: {identifier!r}")
    elif isinstance(identifier, str):
        if identifier.startswith("enode://") or identifier.startswith("enr:"):
            return identifier
        elif len(remove_0x_prefix(HexStr(identifier))) == 64:
            return add_0x_prefix(HexStr(identifier))
        else:
            raise ValueError(f"Unrecognized node identifier: {identifier}")
    else:
        raise ValueError(f"Unrecognized node identifier: {identifier}")


#
# Mungers
# See: https://github.com/ethereum/web3.py/blob/002151020cecd826a694ded2fdc10cc70e73e636/web3/method.py#L77  # noqa: E501
#


def kv_pair_munger(module: Any, *kv_pairs: ENR_KV) -> Tuple[ENR_KV, ...]:
    """
    Normalizes the inputs for `discv5_updateNodeInfo` JSON-RPC endpoints:
    """
    return kv_pairs


def node_identifier_munger(module: Any, identifier: NodeIDIdentifier,) -> List[str]:
    """
    Normalizes the inputs for the following JSON-RPC endpoints:
    - `discv5_ping`
    - `discv5_getENR`
    - `discv5_setENR`
    - `discv5_deleteENR`
    - `discv5_sendPing`
    """
    return [normalize_node_id_identifier(identifier)]


def node_identifier_and_endpoint_munger(
    module: Any, identifier: NodeIDIdentifier, endpoint: Optional[Endpoint] = None
) -> Tuple[str, Optional[Endpoint]]:
    """
    See: https://github.com/ethereum/web3.py/blob/002151020cecd826a694ded2fdc10cc70e73e636/web3/method.py#L77  # noqa: E501

    Normalizes the inputs for the following JSON-RPC endpoints:
    - `discv5_lookupENR`
    """
    return (
        normalize_node_id_identifier(identifier),
        endpoint,
    )


def send_pong_munger(
    module: Any, identifier: NodeIDIdentifier, request_id: HexStr
) -> Tuple[str, HexStr]:
    """
    Normalizes the inputs for the `discv5_sendPong` JSON-RPC endpoints
    """
    return (
        normalize_node_id_identifier(identifier),
        request_id,
    )


def find_nodes_munger(
    module: Any,
    identifier: NodeIDIdentifier,
    distance_or_distances: Union[int, Sequence[int]],
) -> Tuple[str, Union[int, Sequence[int]]]:
    """
    Normalizes the inputs for the `discv5_findNodes` JSON-RPC endpoint
    """
    return (
        normalize_node_id_identifier(identifier),
        distance_or_distances,
    )


def find_nodes_response_formatter(enr_reprs: Sequence[str]) -> Tuple[ENRAPI, ...]:
    return tuple(ENR.from_repr(enr_repr) for enr_repr in enr_reprs)


# TODO: why does mypy think ModuleV2 is of `Any` type?
class DiscoveryV5Module(ModuleV2):  # type: ignore
    """
    A web3.py module that exposes high level APIs for interacting with the
    discovery v5 network.
    """

    get_node_info: Method[Callable[[], NodeInfo]] = Method(
        RPC.nodeInfo, result_formatters=lambda method: NodeInfo.from_rpc_response,
    )
    update_node_info: Method[Callable[[], NodeInfo]] = Method(
        RPC.updateNodeInfo,
        result_formatters=lambda method: UpdateENRPayload.from_rpc_response,
        mungers=[kv_pair_munger],
    )
    get_routing_table_info: Method[Callable[[], TableInfo]] = Method(
        RPC.routingTableInfo,
        result_formatters=lambda method: TableInfo.from_rpc_response,
    )
    get_enr: Method[Callable[[NodeIDIdentifier], GetENRPayload]] = Method(
        RPC.getENR,
        result_formatters=lambda method: GetENRPayload.from_rpc_response,
        mungers=[node_identifier_munger],
    )
    set_enr: Method[Callable[[NodeIDIdentifier], EmptyPayload]] = Method(
        RPC.setENR,
        result_formatters=lambda method: EmptyPayload.from_rpc_response,
        mungers=[node_identifier_munger],
    )
    delete_enr: Method[Callable[[NodeIDIdentifier], EmptyPayload]] = Method(
        RPC.deleteENR,
        result_formatters=lambda method: EmptyPayload.from_rpc_response,
        mungers=[node_identifier_munger],
    )
    lookup_enr: Method[Callable[[NodeIDIdentifier], GetENRPayload]] = Method(
        RPC.lookupENR,
        result_formatters=lambda method: GetENRPayload.from_rpc_response,
        mungers=[node_identifier_and_endpoint_munger],
    )
    ping: Method[Callable[[NodeIDIdentifier], PongPayload]] = Method(
        RPC.ping,
        result_formatters=lambda method: PongPayload.from_rpc_response,
        mungers=[node_identifier_munger],
    )
    send_ping: Method[Callable[[NodeIDIdentifier], SendPingPayload]] = Method(
        RPC.sendPing,
        result_formatters=lambda method: SendPingPayload.from_rpc_response,
        mungers=[node_identifier_munger],
    )
    send_pong: Method[Callable[[NodeIDIdentifier], EmptyPayload]] = Method(
        RPC.sendPong,
        result_formatters=lambda method: EmptyPayload.from_rpc_response,
        mungers=[send_pong_munger],
    )
    find_nodes: Method[
        Callable[[NodeIDIdentifier, Union[int, Sequence[int]]], Tuple[ENRAPI, ...]]
    ] = Method(
        RPC.findNodes,
        result_formatters=lambda method: find_nodes_response_formatter,
        mungers=[find_nodes_munger],
    )
