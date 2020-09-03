import itertools
import logging
import operator
import secrets
from typing import Collection, Iterable, List, Optional, Set, Tuple

from async_service import Service
from eth_enr import ENRAPI, ENRDatabaseAPI, ENRManagerAPI
from eth_enr.constants import IP_V4_ADDRESS_ENR_KEY, UDP_PORT_ENR_KEY
from eth_enr.exceptions import OldSequenceNumber
from eth_typing import NodeID
from eth_utils import to_tuple
from eth_utils.toolz import cons, first, groupby, take
from lru import LRU
import trio

from ddht._utils import every, humanize_node_id
from ddht.constants import ROUTING_TABLE_BUCKET_SIZE
from ddht.endpoint import Endpoint
from ddht.kademlia import (
    KademliaRoutingTable,
    compute_distance,
    compute_log_distance,
    iter_closest_nodes,
)
from ddht.v5_1.abc import ClientAPI, DispatcherAPI, EventsAPI, NetworkAPI, PoolAPI
from ddht.v5_1.constants import ROUTING_TABLE_KEEP_ALIVE
from ddht.v5_1.messages import FindNodeMessage, PingMessage, PongMessage


class Network(Service, NetworkAPI):
    logger = logging.getLogger("ddht.Network")

    _bootnodes: Tuple[ENRAPI, ...]

    def __init__(self, client: ClientAPI, bootnodes: Collection[ENRAPI],) -> None:
        self.client = client

        self._bootnodes = tuple(bootnodes)
        self.routing_table = KademliaRoutingTable(
            self.client.enr_manager.enr.node_id, ROUTING_TABLE_BUCKET_SIZE,
        )
        self._routing_table_ready = trio.Event()
        self._last_pong_at = LRU(2048)

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
    def enr_db(self) -> ENRDatabaseAPI:
        return self.client.enr_db

    #
    # High Level API
    #
    async def bond(
        self, node_id: NodeID, *, endpoint: Optional[Endpoint] = None
    ) -> bool:
        try:
            pong = await self.ping(node_id, endpoint=endpoint)
        except trio.TooSlowError:
            self.logger.debug(
                "Bonding with %s timed out during ping", humanize_node_id(node_id)
            )
            return False

        try:
            enr = self.enr_db.get_enr(node_id)
        except KeyError:
            try:
                enr = await self.get_enr(node_id, endpoint=endpoint)
            except trio.TooSlowError:
                self.logger.debug(
                    "Bonding with %s timed out during ENR retrieval",
                    humanize_node_id(node_id),
                )
                return False
        else:
            if pong.enr_seq > enr.sequence_number:
                try:
                    enr = await self.get_enr(node_id, endpoint=endpoint)
                except trio.TooSlowError:
                    self.logger.debug(
                        "Bonding with %s timed out during ENR retrieval",
                        humanize_node_id(node_id),
                    )
                else:
                    self.enr_db.set_enr(enr)

        self.routing_table.update(enr.node_id)

        self._routing_table_ready.set()
        return True

    async def _bond(self, node_id: NodeID, endpoint: Endpoint) -> None:
        await self.bond(node_id, endpoint=endpoint)

    async def ping(
        self, node_id: NodeID, *, endpoint: Optional[Endpoint] = None
    ) -> PongMessage:
        if endpoint is None:
            endpoint = self._endpoint_for_node_id(node_id)
        response = await self.client.ping(endpoint, node_id)
        return response.message

    async def find_nodes(
        self, node_id: NodeID, *distances: int, endpoint: Optional[Endpoint] = None,
    ) -> Tuple[ENRAPI, ...]:
        if not distances:
            raise TypeError("Must provide at least one distance")

        if endpoint is None:
            endpoint = self._endpoint_for_node_id(node_id)
        responses = await self.client.find_nodes(endpoint, node_id, distances=distances)
        return tuple(enr for response in responses for enr in response.message.enrs)

    async def get_enr(
        self, node_id: NodeID, *, endpoint: Optional[Endpoint] = None
    ) -> ENRAPI:
        enrs = await self.find_nodes(node_id, 0, endpoint=endpoint)
        if not enrs:
            raise Exception("Invalid response")
        # This reduce accounts for
        return _reduce_enrs(enrs)[0]

    async def recursive_find_nodes(self, target: NodeID) -> Tuple[ENRAPI, ...]:
        self.logger.debug("Recursive find nodes: %s", humanize_node_id(target))

        queried_node_ids = set()
        unresponsive_node_ids = set()
        received_enrs: List[ENRAPI] = []
        received_node_ids: Set[NodeID] = set()

        async def do_lookup(node_id: NodeID) -> None:
            queried_node_ids.add(node_id)

            distance = compute_log_distance(node_id, target)
            try:
                enrs = await self.find_nodes(node_id, distance)
            except trio.TooSlowError:
                unresponsive_node_ids.add(node_id)
                return

            for enr in enrs:
                received_node_ids.add(enr.node_id)
                try:
                    self.enr_db.set_enr(enr)
                except OldSequenceNumber:
                    received_enrs.append(self.enr_db.get_enr(enr.node_id))
                else:
                    received_enrs.append(enr)

        for lookup_round_counter in itertools.count():
            candidates = iter_closest_nodes(
                target, self.routing_table, received_node_ids
            )
            responsive_candidates = itertools.dropwhile(
                lambda node: node in unresponsive_node_ids, candidates
            )
            closest_k_candidates = take(
                self.routing_table.bucket_size, responsive_candidates
            )
            closest_k_unqueried_candidates = (
                candidate
                for candidate in closest_k_candidates
                if candidate not in queried_node_ids
            )
            nodes_to_query = tuple(take(3, closest_k_unqueried_candidates))

            if nodes_to_query:
                self.logger.debug(
                    "Starting lookup round %d for %s",
                    lookup_round_counter + 1,
                    humanize_node_id(target),
                )
                async with trio.open_nursery() as nursery:
                    for peer in nodes_to_query:
                        nursery.start_soon(do_lookup, peer)
            else:
                self.logger.debug(
                    "Lookup for %s finished in %d rounds",
                    humanize_node_id(target),
                    lookup_round_counter,
                )
                break

        # now sort and return the ENR records in order of closesness to the target.
        return tuple(
            sorted(
                _reduce_enrs(received_enrs),
                key=lambda enr: compute_distance(enr.node_id, target),
            )
        )

    #
    # Long Running Processes
    #
    async def run(self) -> None:
        self.manager.run_daemon_child_service(self.client)
        await self.client.wait_listening()

        self.manager.run_daemon_task(self._ping_oldest_routing_table_entry)
        self.manager.run_daemon_task(self._track_last_pong)
        self.manager.run_daemon_task(self._manage_routing_table)
        self.manager.run_daemon_task(self._pong_when_pinged)
        self.manager.run_daemon_task(self._serve_find_nodes)

        await self.manager.wait_finished()

    async def _periodically_report_routing_table(self) -> None:
        async for _ in every(30, initial_delay=30):
            non_empty_buckets = tuple(
                (idx, bucket)
                for idx, bucket in enumerate(reversed(self.routing_table.buckets))
                if bucket
            )
            total_size = sum(len(bucket) for idx, bucket in non_empty_buckets)
            bucket_info = "|".join(
                tuple(f"{idx}:{len(bucket)}" for idx, bucket in non_empty_buckets)
            )
            self.logger.debug(
                "routing-table-info: size=%d  buckets=%s", total_size, bucket_info,
            )

    async def _ping_oldest_routing_table_entry(self) -> None:
        await self._routing_table_ready.wait()

        while self.manager.is_running:
            # Here we preserve the lazy iteration while still checking that the
            # iterable is not empty before passing it into `min` below which
            # throws an ambiguous `ValueError` otherwise if the iterable is
            # empty.
            nodes_iter = self.routing_table.iter_all_random()
            try:
                first_node_id = first(nodes_iter)
            except StopIteration:
                await trio.sleep(ROUTING_TABLE_KEEP_ALIVE)
                continue
            else:
                least_recently_ponged_node_id = min(
                    cons(first_node_id, nodes_iter),
                    key=lambda node_id: self._last_pong_at.get(node_id, 0),
                )

            too_old_at = trio.current_time() - ROUTING_TABLE_KEEP_ALIVE
            try:
                last_pong_at = self._last_pong_at[least_recently_ponged_node_id]
            except KeyError:
                pass
            else:
                if last_pong_at > too_old_at:
                    await trio.sleep(last_pong_at - too_old_at)
                    continue

            did_bond = await self.bond(least_recently_ponged_node_id)
            if not did_bond:
                self.routing_table.remove(least_recently_ponged_node_id)

    async def _track_last_pong(self) -> None:
        async with self.dispatcher.subscribe(PongMessage) as subscription:
            async for message in subscription:
                self._last_pong_at[message.sender_node_id] = trio.current_time()

    async def _manage_routing_table(self) -> None:
        # First load all the bootnode ENRs into our database
        for enr in self._bootnodes:
            try:
                self.enr_db.set_enr(enr)
            except OldSequenceNumber:
                pass

        # Now repeatedly try to bond with each bootnode until one succeeds.
        async with trio.open_nursery() as nursery:
            while self.manager.is_running:
                for enr in self._bootnodes:
                    if enr.node_id == self.local_node_id:
                        continue
                    endpoint = self._endpoint_for_enr(enr)
                    nursery.start_soon(self._bond, enr.node_id, endpoint)

                with trio.move_on_after(10):
                    await self._routing_table_ready.wait()
                    break

        # TODO: Need better logic here for more quickly populating the
        # routing table.  Should start off aggressively filling in the
        # table, only backing off once the table contains some minimum
        # number of records **or** searching for new records fails to find
        # new nodes.  Maybe use a TokenBucket
        async for _ in every(30):
            async with trio.open_nursery() as nursery:
                target_node_id = NodeID(secrets.token_bytes(32))
                found_enrs = await self.recursive_find_nodes(target_node_id)
                for enr in found_enrs:
                    endpoint = self._endpoint_for_enr(enr)
                    nursery.start_soon(self._bond, enr.node_id, endpoint)

    async def _pong_when_pinged(self) -> None:
        async with self.dispatcher.subscribe(PingMessage) as subscription:
            async for request in subscription:
                await self.dispatcher.send_message(
                    request.to_response(
                        PongMessage(
                            request.message.request_id,
                            self.enr_manager.enr.sequence_number,
                            request.sender_endpoint.ip_address,
                            request.sender_endpoint.port,
                        )
                    )
                )

    async def _serve_find_nodes(self) -> None:
        async with self.dispatcher.subscribe(FindNodeMessage) as subscription:
            async for request in subscription:
                response_enrs: List[ENRAPI] = []
                distances = set(request.message.distances)
                if len(distances) != len(request.message.distances):
                    self.logger.debug(
                        "Ignoring invalid FindNodeMessage from %s@%s: duplicate distances",
                        humanize_node_id(request.sender_node_id),
                        request.sender_endpoint,
                    )
                    continue
                elif not distances:
                    self.logger.debug(
                        "Ignoring invalid FindNodeMessage from %s@%s: empty distances",
                        humanize_node_id(request.sender_node_id),
                        request.sender_endpoint,
                    )
                    continue
                elif any(
                    distance > self.routing_table.num_buckets for distance in distances
                ):
                    self.logger.debug(
                        "Ignoring invalid FindNodeMessage from %s@%s: distances: %s",
                        humanize_node_id(request.sender_node_id),
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

                await self.client.send_found_nodes(
                    request.sender_endpoint,
                    request.sender_node_id,
                    enrs=response_enrs,
                    request_id=request.message.request_id,
                )

    #
    # Utility
    #
    def _endpoint_for_enr(self, enr: ENRAPI) -> Endpoint:
        try:
            ip_address = enr[IP_V4_ADDRESS_ENR_KEY]
            port = enr[UDP_PORT_ENR_KEY]
        except KeyError:
            raise Exception("Missing endpoint address information: ")

        return Endpoint(ip_address, port)

    def _endpoint_for_node_id(self, node_id: NodeID) -> Endpoint:
        enr = self.enr_db.get_enr(node_id)
        return self._endpoint_for_enr(enr)


@to_tuple
def _reduce_enrs(enrs: Collection[ENRAPI]) -> Iterable[ENRAPI]:
    enrs_by_node_id = groupby(operator.attrgetter("node_id"), enrs)
    for _, enr_group in enrs_by_node_id.items():
        if len(enr_group) == 1:
            yield enr_group[0]
        else:
            yield max(enr_group, key=operator.attrgetter("sequence_number"))
