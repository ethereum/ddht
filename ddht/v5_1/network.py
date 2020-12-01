import itertools
import logging
from typing import (
    AsyncContextManager,
    AsyncIterator,
    Collection,
    Dict,
    List,
    Optional,
    Set,
    Tuple,
)

from async_generator import asynccontextmanager
from async_service import Service
from eth_enr import ENRAPI, ENRManagerAPI, QueryableENRDatabaseAPI
from eth_enr.exceptions import OldSequenceNumber
from eth_typing import NodeID
from eth_utils import ValidationError
from eth_utils.toolz import take
from lru import LRU
import trio

from ddht._utils import adaptive_timeout, every, reduce_enrs
from ddht.base_message import InboundMessage
from ddht.constants import ROUTING_TABLE_BUCKET_SIZE
from ddht.endpoint import Endpoint
from ddht.exceptions import (
    DuplicateProtocol,
    EmptyFindNodesResponse,
    MissingEndpointFields,
)
from ddht.kademlia import (
    KademliaRoutingTable,
    compute_log_distance,
    iter_closest_nodes,
)
from ddht.v5_1.abc import (
    ClientAPI,
    DispatcherAPI,
    EventsAPI,
    NetworkAPI,
    NetworkProtocol,
    PoolAPI,
    TalkProtocolAPI,
)
from ddht.v5_1.exceptions import ProtocolNotSupported
from ddht.v5_1.messages import (
    FindNodeMessage,
    PingMessage,
    PongMessage,
    TalkRequestMessage,
)


@asynccontextmanager
async def common_recursive_find_nodes(
    network: NetworkProtocol,
    target: NodeID,
    *,
    concurrency: int = 3,
    unresponsive_cache: Dict[NodeID, float] = LRU(1024),
) -> AsyncIterator[trio.abc.ReceiveChannel[ENRAPI]]:
    """
    An optimized version of the recursive lookup algorithm for a kademlia
    network.

    Continually lookup nodes in the target part of the network, keeping track
    of all of the nodes we have seen.

    Exit once we have queried all of the `k` closest nodes to the target.

    The concurrency structure here is optimized to minimize the effect of
    unresponsive nodes on the total time it takes to perform the recursive
    lookup.  Some requests will hang for up to 10 seconds.  The
    `adaptive_timeout` combined with the multiple concurrent workers helps
    mitigate the overall slowdown caused by a few unresponsive nodes since the
    other queries can be issues concurrently.
    """
    network.logger.debug("Recursive find nodes: %s", target.hex())
    start_at = trio.current_time()

    # The set of NodeID values we have already queried.
    queried_node_ids: Set[NodeID] = set()

    # The set of NodeID that timed out
    #
    # The `local_node_id` is
    # included in this as a convenience mechanism so that we don't have to
    # continually fiter it out of the various filters
    unresponsive_node_ids: Set[NodeID] = {network.local_node_id}

    # We maintain a cache of nodes that were recently deemed unresponsive
    # within the last 10 minutes.
    unresponsive_node_ids.update(
        node_id
        for node_id, last_unresponsive_at in unresponsive_cache.items()
        if trio.current_time() - last_unresponsive_at < 300
    )

    # Accumulator of the node_ids we have seen
    received_node_ids: Set[NodeID] = set()

    # Tracker for node_ids that are actively being requested.
    in_flight: Set[NodeID] = set()

    condition = trio.Condition()

    def get_unqueried_node_ids() -> Tuple[NodeID, ...]:
        """
        Get the three nodes that are closest to the target such that the node
        is in the closest `k` nodes which haven't been deemed unresponsive.
        """
        # Construct an iterable of *all* the nodes we know about ordered by
        # closeness to the target.
        candidates = iter_closest_nodes(
            target, network.routing_table, received_node_ids
        )
        # Remove any unresponsive nodes from that iterable
        responsive_candidates = itertools.filterfalse(
            lambda node_id: node_id in unresponsive_node_ids, candidates
        )
        # Grab the closest K
        closest_k_candidates = take(
            network.routing_table.bucket_size, responsive_candidates,
        )
        # Filter out any from the closest K that we've already queried or that are in-flight
        closest_k_unqueried = itertools.filterfalse(
            lambda node_id: node_id in queried_node_ids or node_id in in_flight,
            closest_k_candidates,
        )

        return tuple(take(3, closest_k_unqueried))

    async def do_lookup(
        node_id: NodeID, send_channel: trio.abc.SendChannel[ENRAPI]
    ) -> None:
        """
        Perform an individual lookup on the target part of the network from the
        given `node_id`
        """
        if node_id == target:
            distance = 0
        else:
            distance = compute_log_distance(node_id, target)

        try:
            found_enrs = await network.find_nodes(node_id, distance)
        except (trio.TooSlowError, MissingEndpointFields, ValidationError):
            unresponsive_node_ids.add(node_id)
            unresponsive_cache[node_id] = trio.current_time()
            return
        except trio.Cancelled:
            # We don't add these to the unresponsive cache since they didn't
            # necessarily exceed the fulle 10s request/response timeout.
            unresponsive_node_ids.add(node_id)
            raise

        for enr in found_enrs:
            try:
                network.enr_db.set_enr(enr)
            except OldSequenceNumber:
                pass

        new_enrs = tuple(
            enr for enr in found_enrs if enr.node_id not in received_node_ids
        )
        received_node_ids.update(enr.node_id for enr in new_enrs)

        for enr in new_enrs:
            try:
                await send_channel.send(enr)
            except (trio.BrokenResourceError, trio.ClosedResourceError):
                # In the event that the consumer of `recursive_find_nodes`
                # exits early before the lookup has completed we can end up
                # operating on a closed channel.
                return

    async def worker(
        worker_id: NodeID, send_channel: trio.abc.SendChannel[ENRAPI]
    ) -> None:
        """
        Pulls unqueried nodes from the closest k nodes and performs a
        concurrent lookup on them.
        """
        for round in itertools.count():
            async with condition:
                node_ids = get_unqueried_node_ids()

                if not node_ids:
                    await condition.wait()
                    continue

                # Mark the node_ids as having been queried.
                queried_node_ids.update(node_ids)
                # Mark the node_ids as being in-flight.
                in_flight.update(node_ids)

            # Some of the node ids may have come from our routing table.
            # These won't be present in the `received_node_ids` so we
            # detect this here and send them over the channel.
            try:
                for node_id in node_ids:
                    if node_id not in received_node_ids:
                        enr = network.enr_db.get_enr(node_id)
                        received_node_ids.add(node_id)
                        await send_channel.send(enr)
            except (trio.BrokenResourceError, trio.ClosedResourceError):
                # In the event that the consumer of `recursive_find_nodes`
                # exits early before the lookup has completed we can end up
                # operating on a closed channel.
                return

            if len(node_ids) == 1:
                await do_lookup(node_ids[0], send_channel)
            else:
                tasks = tuple(
                    (do_lookup, (node_id, send_channel)) for node_id in node_ids
                )
                await adaptive_timeout(*tasks, threshold=1, variance=2.0)

            async with condition:
                # Remove the `node_ids` from the in_flight set.
                in_flight.difference_update(node_ids)

                condition.notify_all()

    async def _monitor_done(send_channel: trio.abc.SendChannel[ENRAPI]) -> None:
        async with send_channel:
            while True:
                # this `fail_after` is a failsafe to prevent deadlock situations
                # which are possible with `Condition` objects.
                with trio.fail_after(60):
                    async with condition:
                        node_ids = get_unqueried_node_ids()

                        if not node_ids and not in_flight:
                            break
                        else:
                            await condition.wait()

    send_channel, receive_channel = trio.open_memory_channel[ENRAPI](256)

    async with trio.open_nursery() as nursery:
        nursery.start_soon(_monitor_done, send_channel)

        for worker_id in range(concurrency):
            nursery.start_soon(worker, worker_id, send_channel)

        async with receive_channel:
            yield receive_channel

        nursery.cancel_scope.cancel()

    elapsed = trio.current_time() - start_at

    network.logger.debug(
        "Lookup for %s finished in %f seconds: seen=%d  queried=%d  unresponsive=%d",
        target.hex(),
        elapsed,
        len(received_node_ids),
        len(queried_node_ids),
        len(unresponsive_node_ids),
    )


class Network(Service, NetworkAPI):
    logger = logging.getLogger("ddht.Network")

    _bootnodes: Tuple[ENRAPI, ...]
    _talk_protocols: Dict[bytes, TalkProtocolAPI]

    def __init__(self, client: ClientAPI, bootnodes: Collection[ENRAPI],) -> None:
        self.client = client

        self._bootnodes = tuple(bootnodes)
        self.routing_table = KademliaRoutingTable(
            self.client.enr_manager.enr.node_id, ROUTING_TABLE_BUCKET_SIZE,
        )
        self._routing_table_ready = trio.Event()
        self._last_pong_at = LRU(2048)

        self._talk_protocols = {}

    #
    # Proxied ClientAPI properties
    #
    @property
    def local_node_id(self) -> NodeID:
        return self.client.local_node_id

    @property
    def events(self) -> EventsAPI:
        return self.client.events

    @property
    def dispatcher(self) -> DispatcherAPI:
        return self.client.dispatcher

    @property
    def enr_manager(self) -> ENRManagerAPI:
        return self.client.enr_manager

    @property
    def pool(self) -> PoolAPI:
        return self.client.pool

    @property
    def enr_db(self) -> QueryableENRDatabaseAPI:
        return self.client.enr_db

    #
    # TALK API
    #
    def add_talk_protocol(self, protocol: TalkProtocolAPI) -> None:
        if protocol.protocol_id in self._talk_protocols:
            raise DuplicateProtocol(
                f"A protocol is already registered for '{protocol.protocol_id!r}'"
            )
        self._talk_protocols[protocol.protocol_id] = protocol

    #
    # High Level API
    #
    async def bond(
        self, node_id: NodeID, *, endpoint: Optional[Endpoint] = None
    ) -> bool:
        self.logger.debug(
            "Bonding with %s", node_id.hex(),
        )

        try:
            pong = await self.ping(node_id, endpoint=endpoint)
        except trio.TooSlowError:
            self.logger.debug("Bonding with %s timed out during ping", node_id.hex())
            return False
        except MissingEndpointFields:
            self.logger.debug(
                "Bonding with %s failed due to missing endpoint information",
                node_id.hex(),
            )
            return False

        try:
            enr = await self.lookup_enr(
                node_id, enr_seq=pong.enr_seq, endpoint=endpoint
            )
        except trio.TooSlowError:
            self.logger.debug(
                "Bonding with %s timed out during ENR retrieval", node_id.hex(),
            )
            return False
        except EmptyFindNodesResponse:
            self.logger.debug(
                "Bonding with %s failed due to them not returing their ENR record",
                node_id.hex(),
            )
            return False

        self.routing_table.update(enr.node_id)

        self.logger.debug(
            "Bonded with %s successfully", node_id.hex(),
        )

        self._routing_table_ready.set()
        return True

    async def ping(
        self,
        node_id: NodeID,
        *,
        endpoint: Optional[Endpoint] = None,
        request_id: Optional[bytes] = None,
    ) -> PongMessage:
        if endpoint is None:
            endpoint = await self.endpoint_for_node_id(node_id)
        response = await self.client.ping(node_id, endpoint, request_id=request_id)
        return response.message

    async def find_nodes(
        self,
        node_id: NodeID,
        *distances: int,
        endpoint: Optional[Endpoint] = None,
        request_id: Optional[bytes] = None,
    ) -> Tuple[ENRAPI, ...]:
        if not distances:
            raise TypeError("Must provide at least one distance")

        if endpoint is None:
            endpoint = await self.endpoint_for_node_id(node_id)
        responses = await self.client.find_nodes(
            node_id, endpoint, distances=distances, request_id=request_id
        )

        # Validate that all responses are indeed at one of the
        # specified distances.
        for response in responses:
            for enr in response.message.enrs:
                if enr.node_id == node_id:
                    if 0 not in distances:
                        raise ValidationError(
                            f"Invalid response: distance=0  expected={distances}"
                        )
                else:
                    distance = compute_log_distance(enr.node_id, node_id)
                    if distance not in distances:
                        raise ValidationError(
                            f"Invalid response: distance={distance}  expected={distances}"
                        )

        return tuple(enr for response in responses for enr in response.message.enrs)

    async def talk(
        self,
        node_id: NodeID,
        *,
        protocol: bytes,
        payload: bytes,
        endpoint: Optional[Endpoint] = None,
        request_id: Optional[bytes] = None,
    ) -> bytes:
        if endpoint is None:
            endpoint = await self.endpoint_for_node_id(node_id)
        response = await self.client.talk(
            node_id, endpoint, protocol, payload, request_id=request_id
        )
        payload = response.message.payload
        if not payload:
            raise ProtocolNotSupported(protocol)
        return response.message.payload

    async def lookup_enr(
        self, node_id: NodeID, *, enr_seq: int = 0, endpoint: Optional[Endpoint] = None
    ) -> ENRAPI:
        if node_id == self.local_node_id:
            raise Exception(f"Cannot lookup local ENR: node_id={node_id.hex()}")

        try:
            enr = self.enr_db.get_enr(node_id)
        except KeyError:
            if endpoint is None:
                # Try to use a recursive network lookup to find the desired
                # node.
                async with self.recursive_find_nodes(node_id) as enr_aiter:
                    async for found_enr in enr_aiter:
                        if found_enr.node_id == node_id:
                            endpoint = Endpoint.from_enr(found_enr)
                            break
                    else:
                        # we weren't given an endpoint and we don't have an enr which would give
                        # us an endpoint, there's no way to reach this node.
                        raise KeyError(f"Could not find ENR: node_id={node_id.hex()}")
        else:
            if enr.sequence_number >= enr_seq:
                return enr

        enr = await self._fetch_enr(node_id, endpoint=endpoint)
        try:
            self.enr_db.set_enr(enr)
        except OldSequenceNumber:
            pass

        return enr

    async def _fetch_enr(
        self, node_id: NodeID, *, endpoint: Optional[Endpoint]
    ) -> ENRAPI:
        enrs = await self.find_nodes(node_id, 0, endpoint=endpoint)
        if not enrs:
            raise EmptyFindNodesResponse(f"{node_id.hex()} did not return its ENR")

        # Assuming we're given enrs for a single node, this reduce returns the enr for
        # that node with the highest sequence number
        return reduce_enrs(enrs)[0]

    def recursive_find_nodes(
        self, target: NodeID
    ) -> AsyncContextManager[trio.abc.ReceiveChannel[ENRAPI]]:
        return common_recursive_find_nodes(self, target)

    #
    # Long Running Processes
    #
    async def run(self) -> None:
        self.manager.run_daemon_child_service(self.client)
        await self.client.wait_listening()

        self.manager.run_daemon_task(self._serve_find_nodes)
        self.manager.run_daemon_task(self._handle_unhandled_talk_requests)

        await self.manager.wait_finished()

    async def _serve_find_nodes(self) -> None:
        async with self.dispatcher.subscribe(FindNodeMessage) as subscription:
            async for request in subscription:
                response_enrs: List[ENRAPI] = []
                distances = set(request.message.distances)
                if len(distances) != len(request.message.distances):
                    self.logger.debug(
                        "Ignoring invalid FindNodeMessage from %s@%s: duplicate distances",
                        request.sender_node_id.hex(),
                        request.sender_endpoint,
                    )
                    continue
                elif not distances:
                    self.logger.debug(
                        "Ignoring invalid FindNodeMessage from %s@%s: empty distances",
                        request.sender_node_id.hex(),
                        request.sender_endpoint,
                    )
                    continue
                elif any(
                    distance > self.routing_table.num_buckets for distance in distances
                ):
                    self.logger.debug(
                        "Ignoring invalid FindNodeMessage from %s@%s: distances: %s",
                        request.sender_node_id.hex(),
                        request.sender_endpoint,
                        distances,
                    )
                    continue

                for distance in distances:
                    if distance == 0:
                        response_enrs.append(self.enr_manager.enr)
                    elif distance <= self.routing_table.num_buckets:
                        node_ids_at_distance = self.routing_table.get_nodes_at_log_distance(
                            distance,
                        )
                        for node_id in node_ids_at_distance:
                            response_enrs.append(self.enr_db.get_enr(node_id))
                    else:
                        raise Exception("Should be unreachable")

                self.logger.info("SERVING: %s -> %d", distances, len(response_enrs))
                await self.client.send_found_nodes(
                    request.sender_node_id,
                    request.sender_endpoint,
                    enrs=response_enrs,
                    request_id=request.request_id,
                )

    async def _handle_unhandled_talk_requests(self) -> None:
        async with self.dispatcher.subscribe(TalkRequestMessage) as subscription:
            async for request in subscription:
                if request.message.protocol not in self._talk_protocols:
                    await self.client.send_talk_response(
                        request.sender_node_id,
                        request.sender_endpoint,
                        payload=b"",
                        request_id=request.message.request_id,
                    )

    #
    # Utility
    #
    async def endpoint_for_node_id(self, node_id: NodeID) -> Endpoint:
        try:
            enr = self.enr_db.get_enr(node_id)
        except KeyError:
            enr = await self.lookup_enr(node_id)

        return Endpoint.from_enr(enr)
