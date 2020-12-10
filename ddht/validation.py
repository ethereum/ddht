import ipaddress
from typing import Any, Collection, Iterable, List, Optional, Tuple

from eth_enr import ENR, ENRAPI
from eth_typing import HexStr, NodeID
from eth_utils import (
    ValidationError,
    decode_hex,
    is_hex,
    is_integer,
    is_list_like,
    remove_0x_prefix,
    to_bytes,
    to_tuple,
)

from ddht.endpoint import Endpoint
from ddht.kademlia import compute_log_distance
from ddht.rpc import RPCError


def validate_length(value: Collection[Any], length: int, title: str = "Value") -> None:
    if not len(value) == length:
        raise ValidationError(
            "{title} must be of length {0}.  Got {1} of length {2}".format(
                length, value, len(value), title=title,
            )
        )


def validate_length_lte(
    value: Collection[Any], maximum_length: int, title: str = "Value"
) -> None:
    if len(value) > maximum_length:
        raise ValidationError(
            "{title} must be of length less than or equal to {0}.  "
            "Got {1} of length {2}".format(
                maximum_length, value, len(value), title=title,
            )
        )


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


@to_tuple
def validate_and_convert_hexstr(*args: Any) -> Iterable[bytes]:
    for value in args:
        try:
            yield to_bytes(hexstr=value)
        except TypeError:
            raise RPCError(f"Invalid hexstr parameter: {value}")


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


def validate_found_nodes_distances(
    enrs: Collection[ENRAPI], local_node_id: NodeID, distances: Collection[int],
) -> None:
    for enr in enrs:
        if enr.node_id == local_node_id:
            if 0 not in distances:
                raise ValidationError(
                    f"Invalid response: distance=0  expected={distances}"
                )
        else:
            distance = compute_log_distance(enr.node_id, local_node_id)
            if distance not in distances:
                raise ValidationError(
                    f"Invalid response: distance={distance}  expected={distances}"
                )
