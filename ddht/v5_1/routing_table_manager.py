from typing import Collection, Optional, Type, Any

from async_service import Service
from eth_enr import ENRAPI, OldSequenceNumber, QueryableENRDatabaseAPI
from eth_typing import NodeID
from eth_utils.toolz import first, cons
import trio

from ddht._utils import weighted_choice
from ddht.abc import RoutingTableAPI
from ddht.endpoint import Endpoint
from ddht.kademlia import at_log_distance
from ddht.token_bucket import TokenBucket
from ddht.v5_1.abc import RoutingTableManagerAPI, NetworkProtocol
from ddht.v5_1.constants import ROUTING_TABLE_KEEP_ALIVE


class RoutingTableManager(Service, RoutingTableManagerAPI):
    def __init__(self,
                 network: NetworkProtocol,
                 bootnodes: Collection[ENRAPI],
                 pong_message_class: Type[Any],
                 ) -> None:
        self._network = network
        self._bootnodes = tuple(bootnodes)
        self._routing_table_ready = trio.Condition()

    @property
    def routing_table(self) -> RoutingTableAPI:
        return self._network.routing_table

    @property
    def enr_db(self) -> QueryableENRDatabaseAPI:
        return self._network.enr_db

    async def _bond(self, node_id: NodeID, endpoint: Optional[Endpoint] = None) -> None:
        await self._network.bond(node_id, endpoint=endpoint)

    async def ready(self) -> None:
        while self.routing_table.is_empty:
            async with self._routing_table_ready:
                await self._routing_table_ready.wait()

    async def run(self) -> None:
        for enr in self._bootnodes:
            try:
                self.enr_db.set_enr(enr)
            except OldSequenceNumber:
                pass

        self.manager.run_daemon_task(self._monitor_for_empty_routing_table)
        self.manager.run_daemon_task(self._explore_network)
        self.manager.run_daemon_task(self._ping_oldest_routing_table_entry)
        self.manager.run_daemon_task(self._track_last_pong)

        await self.manager.wait_finished()

    async def _monitor_for_empty_routing_table(self) -> None:
        while self.manager.is_running:
            if self.routing_table.is_empty:

                # Now repeatedly try to bond with each bootnode until one succeeds.
                while self.manager.is_running:
                    with trio.move_on_after(20):
                        async with trio.open_nursery() as nursery:
                            for enr in self._bootnodes:
                                if enr.node_id == self.local_node_id:
                                    continue
                                endpoint = Endpoint.from_enr(enr)
                                nursery.start_soon(self._bond, enr.node_id, endpoint)

                            await self._routing_table_ready.wait()
                            break

    async def _track_last_pong(self) -> None:
        async with self._network.dispatcher.subscribe(self.pong_message_class) as subscription:
            async for message in subscription:
                self._last_pong_at[message.sender_node_id] = trio.current_time()

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

    async def _explore_network(self) -> None:
        # Now we enter into an infinite loop that continually probes the
        # network to beep the routing table fresh.  We both perform completely
        # random lookups, as well as targeted lookups on the outermost routing
        # table buckets which are not full.
        #
        # The `TokenBucket` allows us to burst at the beginning, making quick
        # successive probes, then slowing down once the
        #
        # TokenBucket starts with 10 tokens, refilling at 1 token every 30
        # seconds.
        token_bucket = TokenBucket(1 / 30, 10)

        async with trio.open_nursery() as nursery:
            while self.manager.is_running:
                # Always wait for the routing table to be ready
                await self.ready()

                # Rate limiter
                await token_bucket.take()

                # Get the logarithmic distance to the "largest" buckets
                # that are not full.
                non_full_bucket_distances = tuple(
                    idx + 1
                    for idx, bucket in enumerate(self.routing_table.buckets)
                    if len(bucket) < self.routing_table.bucket_size  # noqa: E501
                )[-16:]

                # Probe one of the not-full-buckets with a weighted preference
                # towards the largest buckets.
                distance_to_probe = weighted_choice(non_full_bucket_distances)
                target_node_id = at_log_distance(self.local_node_id, distance_to_probe)

                async with self.recursive_find_nodes(target_node_id) as enr_aiter:
                    async for enr in enr_aiter:
                        if enr.node_id == self.local_node_id:
                            continue

                        try:
                            self.enr_db.set_enr(enr)
                        except OldSequenceNumber:
                            pass

                        nursery.start_soon(self._bond, enr.node_id)
