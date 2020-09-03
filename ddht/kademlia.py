import collections
import functools
import ipaddress
import itertools
import logging
import math
import random
import struct
from typing import (
    Any,
    Collection,
    Deque,
    Iterator,
    List,
    Optional,
    Set,
    Tuple,
    Type,
    cast,
)

from eth_typing import NodeID
from eth_utils import big_endian_to_int, encode_hex

from ddht.abc import AddressAPI, RoutingTableAPI, TAddress
from ddht.constants import NUM_ROUTING_TABLE_BUCKETS


def check_relayed_addr(sender: AddressAPI, addr: AddressAPI) -> bool:
    """
    Check if an address relayed by the given sender is valid.

    Reserved and unspecified addresses are always invalid.
    Private addresses are valid if the sender is a private host.
    Loopback addresses are valid if the sender is a loopback host.
    All other addresses are valid.
    """
    if addr.is_unspecified or addr.is_reserved:
        return False
    if addr.is_private and not sender.is_private:
        return False
    if addr.is_loopback and not sender.is_loopback:
        return False
    return True


def enc_port(port: int) -> bytes:
    return struct.pack(">H", port)


class Address(AddressAPI):
    def __init__(self, ip: str, udp_port: int, tcp_port: int) -> None:
        self.udp_port = udp_port
        self.tcp_port = tcp_port
        self._ip = cast(ipaddress.IPv4Address, ipaddress.ip_address(ip))

    @property
    def is_loopback(self) -> bool:
        return self._ip.is_loopback

    @property
    def is_unspecified(self) -> bool:
        return self._ip.is_unspecified

    @property
    def is_reserved(self) -> bool:
        return self._ip.is_reserved

    @property
    def is_private(self) -> bool:
        return self._ip.is_private

    @property
    def ip(self) -> str:
        return str(self._ip)

    @property
    def ip_packed(self) -> bytes:
        """The binary representation of this IP address."""
        return self._ip.packed

    def __eq__(self, other: Any) -> bool:
        return (self.ip, self.udp_port) == (other.ip, other.udp_port)

    def __repr__(self) -> str:
        return "Address(%s:udp:%s|tcp:%s)" % (self.ip, self.udp_port, self.tcp_port)

    def to_endpoint(self) -> List[bytes]:
        return [self._ip.packed, enc_port(self.udp_port), enc_port(self.tcp_port)]

    @classmethod
    def from_endpoint(
        cls: Type[TAddress], ip: str, udp_port: bytes, tcp_port: bytes = b"\x00\x00"
    ) -> TAddress:
        return cls(ip, big_endian_to_int(udp_port), big_endian_to_int(tcp_port))


def compute_distance(left_node_id: NodeID, right_node_id: NodeID) -> int:
    left_int = big_endian_to_int(left_node_id)
    right_int = big_endian_to_int(right_node_id)
    return left_int ^ right_int


def compute_log_distance(left_node_id: NodeID, right_node_id: NodeID) -> int:
    if left_node_id == right_node_id:
        raise ValueError("Cannot compute log distance between identical nodes")
    distance = compute_distance(left_node_id, right_node_id)
    return distance.bit_length()


class KademliaRoutingTable(RoutingTableAPI):
    def __init__(self, center_node_id: NodeID, bucket_size: int) -> None:
        self.logger = logging.getLogger("ddht.kademlia.KademliaRoutingTable")
        self.center_node_id = center_node_id
        self.bucket_size = bucket_size

        self.buckets: Tuple[Deque[NodeID], ...] = tuple(
            collections.deque(maxlen=bucket_size)
            for _ in range(NUM_ROUTING_TABLE_BUCKETS)
        )
        self.replacement_caches: Tuple[Deque[NodeID], ...] = tuple(
            collections.deque() for _ in range(NUM_ROUTING_TABLE_BUCKETS)
        )

        self.bucket_update_order: Deque[int] = collections.deque()

    @property
    def num_buckets(self) -> int:
        return len(self.buckets)

    def _contains(self, node_id: NodeID, include_replacement_cache: bool) -> bool:
        _, bucket, replacement_cache = self.get_index_bucket_and_replacement_cache(
            node_id
        )
        if include_replacement_cache:
            return node_id in bucket or node_id in replacement_cache
        else:
            return node_id in bucket

    def get_index_bucket_and_replacement_cache(
        self, node_id: NodeID
    ) -> Tuple[int, Deque[NodeID], Deque[NodeID]]:
        index = compute_log_distance(self.center_node_id, node_id) - 1
        bucket = self.buckets[index]
        replacement_cache = self.replacement_caches[index]
        return index, bucket, replacement_cache

    def update(self, node_id: NodeID) -> Optional[NodeID]:
        """
        Insert a node into the routing table or move it to the top if already present.

        If the bucket is already full, the node id will be added to the replacement cache and
        the oldest node is returned as an eviction candidate. Otherwise, the return value is
        `None`.
        """
        if node_id == self.center_node_id:
            raise ValueError("Cannot insert center node into routing table")

        (
            bucket_index,
            bucket,
            replacement_cache,
        ) = self.get_index_bucket_and_replacement_cache(node_id)

        is_bucket_full = len(bucket) >= self.bucket_size
        is_node_in_bucket = node_id in bucket

        if not is_node_in_bucket and not is_bucket_full:
            self.logger.debug(
                "Adding %s to bucket %d", encode_hex(node_id), bucket_index
            )
            self.update_bucket_unchecked(node_id)
            eviction_candidate = None
        elif is_node_in_bucket:
            self.logger.debug(
                "Updating %s in bucket %d", encode_hex(node_id), bucket_index
            )
            self.update_bucket_unchecked(node_id)
            eviction_candidate = None
        elif not is_node_in_bucket and is_bucket_full:
            if node_id not in replacement_cache:
                self.logger.debug(
                    "Adding %s to replacement cache of bucket %d",
                    encode_hex(node_id),
                    bucket_index,
                )
            else:
                self.logger.debug(
                    "Updating %s in replacement cache of bucket %d",
                    encode_hex(node_id),
                    bucket_index,
                )
                replacement_cache.remove(node_id)
            replacement_cache.appendleft(node_id)
            eviction_candidate = bucket[-1]
        else:
            raise Exception("unreachable")

        return eviction_candidate

    def update_bucket_unchecked(self, node_id: NodeID) -> None:
        """Add or update assuming the node is either present already or the bucket is not full."""
        (
            bucket_index,
            bucket,
            replacement_cache,
        ) = self.get_index_bucket_and_replacement_cache(node_id)

        for container in (bucket, replacement_cache):
            try:
                container.remove(node_id)
            except ValueError:
                pass
        bucket.appendleft(node_id)

        try:
            self.bucket_update_order.remove(bucket_index)
        except ValueError:
            pass
        self.bucket_update_order.appendleft(bucket_index)

    def remove(self, node_id: NodeID) -> None:
        """
        Remove a node from the routing table if it is present.

        If possible, the node will be replaced with the newest entry in the replacement cache.
        """
        (
            bucket_index,
            bucket,
            replacement_cache,
        ) = self.get_index_bucket_and_replacement_cache(node_id)

        in_bucket = node_id in bucket
        in_replacement_cache = node_id in replacement_cache

        if in_bucket:
            bucket.remove(node_id)
            if replacement_cache:
                replacement_node_id = replacement_cache.popleft()
                self.logger.debug(
                    "Replacing %s from bucket %d with %s from replacement cache",
                    encode_hex(node_id),
                    bucket_index,
                    encode_hex(replacement_node_id),
                )
                bucket.append(replacement_node_id)
            else:
                self.logger.debug(
                    "Removing %s from bucket %d without replacement",
                    encode_hex(node_id),
                    bucket_index,
                )

        if in_replacement_cache:
            self.logger.debug(
                "Removing %s from replacement cache of bucket %d",
                encode_hex(node_id),
                bucket_index,
            )
            replacement_cache.remove(node_id)

        if not in_bucket and not in_replacement_cache:
            self.logger.debug(
                "Not removing %s as it is neither present in the bucket nor the replacement cache",
                encode_hex(node_id),
                bucket_index,
            )

        # bucket_update_order should only contain non-empty buckets, so remove it if necessary
        if not bucket:
            try:
                self.bucket_update_order.remove(bucket_index)
            except ValueError:
                pass

    def get_nodes_at_log_distance(self, log_distance: int) -> Tuple[NodeID, ...]:
        """Get all nodes in the routing table at the given log distance to the center."""
        if log_distance <= 0:
            raise ValueError(f"Log distance must be positive, got {log_distance}")
        elif log_distance > len(self.buckets):
            raise ValueError(
                f"Log distance must not be greater than {len(self.buckets)}, got {log_distance}"
            )
        return tuple(self.buckets[log_distance - 1])

    @property
    def is_empty(self) -> bool:
        return all(len(bucket) == 0 for bucket in self.buckets)

    def get_least_recently_updated_log_distance(self) -> int:
        """
        Get the log distance whose corresponding bucket was updated least recently.

        Only non-empty buckets are considered. If all buckets are empty, a `ValueError` is raised.
        """
        try:
            bucket_index = self.bucket_update_order[-1]
        except IndexError:
            raise ValueError("Routing table is empty")
        else:
            return bucket_index + 1

    def iter_nodes_around(self, reference_node_id: NodeID) -> Iterator[NodeID]:
        """Iterate over all nodes in the routing table ordered by distance to a given reference."""
        all_node_ids = itertools.chain(*self.buckets)
        distance_to_reference = functools.partial(compute_distance, reference_node_id)
        sorted_node_ids = sorted(all_node_ids, key=distance_to_reference)
        for node_id in sorted_node_ids:
            yield node_id

    def iter_all_random(self) -> Iterator[NodeID]:
        """
        Iterate over all nodes in the table (including ones in the replacement cache) in a random
        order.
        """
        # Create a new list with all available nodes as buckets can mutate while we're iterating.
        # This shouldn't use a significant amount of memory as the new list will keep just
        # references to the existing NodeID instances.
        node_ids = list(itertools.chain(*self.buckets, *self.replacement_caches))
        random.shuffle(node_ids)
        for node_id in node_ids:
            yield node_id


def iter_closest_nodes(
    target: NodeID, routing_table: RoutingTableAPI, seen_nodes: Collection[NodeID]
) -> Iterator[NodeID]:
    """
    Iterate over the nodes in the routing table as well as additional nodes in order of
    distance to the target. Duplicates will only be yielded once.
    """
    yielded_nodes: Set[NodeID] = set()
    routing_iter = routing_table.iter_nodes_around(target)

    def dist(node: Optional[NodeID]) -> float:
        if node is not None:
            return compute_distance(target, node)
        else:
            return math.inf

    seen_iter = iter(sorted(seen_nodes, key=dist))
    closest_routing = next(routing_iter, None)
    closest_seen = next(seen_iter, None)

    while not (closest_routing is None and closest_seen is None):
        node_to_yield: NodeID

        if dist(closest_routing) < dist(closest_seen):
            node_to_yield = closest_routing  # type: ignore
            closest_routing = next(routing_iter, None)
        else:
            node_to_yield = closest_seen  # type: ignore
            closest_seen = next(seen_iter, None)

        if node_to_yield not in yielded_nodes:
            yielded_nodes.add(node_to_yield)
            yield node_to_yield
