import contextlib
import itertools
import logging
from typing import AsyncIterator, Iterator, Tuple

from async_generator import asynccontextmanager
from async_service import Service
from eth_enr import ENRAPI, OldSequenceNumber
from eth_typing import NodeID
from eth_utils import ValidationError
from eth_utils.toolz import cons, first, partition_all, sliding_window
import trio

from ddht._utils import caboose, every, reduce_enrs
from ddht.exceptions import MissingEndpointFields
from ddht.kademlia import compute_log_distance, iter_closest
from ddht.v5_1.abc import ExplorerAPI, ExploreStats, NetworkProtocol


class Explorer(Service, ExplorerAPI):
    """
    TODO: needs updating

    Return an async iterator (trio.abc.ReceiveChannel) which will find *all*
    nodes in the network, prioritizing the search towards nodes closest to
    `target`.

    The algorithm builds on `recursive_find_nodes` to quickly find the node in
    the network that is closest to the target.

    Then we work through the known nodes in order of proximity to the `target`.
    For each `node_id` we check the distance between it and its closest
    neighbors to determine the maximum bucket index we should query.  Since
    each knows the most about the neighborhood of the network it resides in, we
    only want to query the buckets up to and including the ones that include
    the closest neighbors.  We issue FIND_NODES queries until we encounter
    empty buckets.

    This function strikes a balance between focusing exploration on the
    `target` part of the network, and quickly returning results.  The initial
    results returned by this function might not be close to the target, but it
    should very quickly narrow in towards the target, after which it will
    slowly work away from the target.
    """

    logger = logging.getLogger("ddht.Explorer")

    def __init__(
        self, network: NetworkProtocol, target: NodeID, concurrency: int = 3
    ) -> None:
        self._network = network
        self.target = target
        self._concurrency = concurrency

        self._condition = trio.Condition()

        self.in_flight = set()
        self.seen = set()
        self.queried = {self._network.local_node_id}
        self.unresponsive = set()
        self.unreachable = set()
        self.invalid = set()

        # Using a relatively small buffer size here ensures that we are applying
        # back-pressure against the workers.  If the consumer is only consuming a
        # few nodes, we don't need to continue issuing requests.
        self._send_channel, self._receive_channel = trio.open_memory_channel[ENRAPI](16)

        # signal that the initial set of nodes for exploration has been seeded.
        self._exploration_seeded = trio.Event()

        # signal that the service is up and running and ready for nodes to be streamed.
        self._ready = trio.Event()

    def get_stats(self) -> ExploreStats:
        return ExploreStats(
            in_flight=len(self.in_flight),
            seen=len(self.seen),
            queried=len(self.queried),
            unresponsive=len(self.unresponsive),
            unreachable=len(self.unreachable),
            invalid=len(self.invalid),
            elapsed=trio.current_time() - self._start_at,
        )

    @contextlib.contextmanager
    def _mark_in_flight(self, node_id: NodeID) -> Iterator[None]:
        self.in_flight.add(node_id)
        try:
            yield
        finally:
            self.in_flight.remove(node_id)

    async def ready(self) -> None:
        await self._ready.wait()

    @asynccontextmanager
    async def stream(self) -> AsyncIterator[trio.abc.ReceiveChannel[ENRAPI]]:
        async with self._receive_channel:
            yield self._receive_channel
            self.manager.cancel()

    def _get_ordered_candidates(self) -> Iterator[NodeID]:
        return iter_closest(self.target, self.seen)

    def _get_nodes_for_exploration(self) -> Iterator[Tuple[NodeID, int]]:
        candidates = self._get_ordered_candidates()
        candidate_triplets = sliding_window(3, caboose(cons(None, candidates), None))

        for left_id, node_id, right_id in candidate_triplets:
            # Filter out nodes that have already been queried
            if node_id in self.queried:
                continue
            elif node_id in self.in_flight:
                continue

            # By looking at the two closest *sibling* nodes we can determine
            # how much of their routing table we need to query.  We consider
            # the maximum logarithmic distance to either neighbor which
            # guarantees that we look up the region of the network that this
            # node knows the most about, but avoid querying buckets for which
            # other nodes are going to have a more complete view.
            if left_id is None:
                left_distance = 256
            else:
                left_distance = compute_log_distance(node_id, left_id)

            if right_id is None:
                right_distance = 256
            else:
                right_distance = compute_log_distance(node_id, right_id)

            # We use the maximum distance to ensure that we cover every part of
            # the address space.
            yield node_id, max(left_distance, right_distance)

    async def run(self) -> None:
        self._start_at = trio.current_time()

        self.manager.run_task(self._source_initial_nodes)

        for worker_id in range(self._concurrency):
            self.manager.run_daemon_task(self._worker, worker_id)

        async with self._send_channel:
            self._ready.set()

            # First wait for the RFN to be complete.
            await self._exploration_seeded.wait()

            while self.manager.is_running:
                # TODO: stop-gap to ensure we don't deadlock
                with trio.fail_after(60):
                    async with self._condition:

                        try:
                            first(self._get_nodes_for_exploration())
                        except StopIteration:
                            if not self.in_flight:
                                break

                        await self._condition.wait()

        self.logger.debug("%s[final]: %s", self, self.get_stats())
        self.manager.cancel()

    async def _periodically_log_stats(self) -> None:
        async for _ in every(5):
            self._network.logger.debug("%s[stats]: %s", self, self.get_stats())

    async def _bond_then_send(self, enr: ENRAPI,) -> None:
        """
        Ensure that we only yield nodes that have passed a liveliness check.
        """
        if enr.node_id == self._network.local_node_id:
            did_bond = True
        else:
            did_bond = await self._network.bond(enr.node_id)

        if did_bond:
            try:
                await self._send_channel.send(enr)
            except (trio.BrokenResourceError, trio.ClosedResourceError):
                # In the event that the consumer of `recursive_find_nodes`
                # exits early before the lookup has completed we can end up
                # operating on a closed channel.
                pass

    async def _explore(self, node_id: NodeID, max_distance: int,) -> None:
        """
        Explore the neighborhood around the given `node_id` out to the
        specified `max_distance`
        """
        async with trio.open_nursery() as nursery:
            for distances in partition_all(2, range(max_distance, 0, -1)):
                try:
                    found_enrs = await self._network.find_nodes(node_id, *distances)
                except trio.TooSlowError:
                    self.unresponsive.add(node_id)
                    return
                except MissingEndpointFields:
                    self.unreachable.add(node_id)
                    return
                except ValidationError:
                    self.invalid.add(node_id)
                    return
                else:
                    # once we encounter a pair of buckets that elicits an empty
                    # response we assume that all subsequent buckets will also
                    # be empty.
                    if not found_enrs:
                        self.logger.debug(
                            "explore-finish: node_id=%s  covered=%d-%d",
                            node_id.hex(),
                            max_distance,
                            distances[0],
                        )
                        break

                for enr in found_enrs:
                    try:
                        self._network.enr_db.set_enr(enr)
                    except OldSequenceNumber:
                        pass

                # check if we have found any new records.  If so, queue them and
                # wake up the new workers.  This is guarded by the `condition`
                # object to ensure we maintain a consistent view of the `seen`
                # nodes.
                async with self._condition:
                    new_enrs = tuple(
                        enr
                        for enr in reduce_enrs(found_enrs)
                        if enr.node_id not in self.seen
                    )

                    if new_enrs:
                        self.seen.update(enr.node_id for enr in new_enrs)
                        self._condition.notify_all()

                # use the `NetworkProtocol.bond` to perform a liveliness check
                for enr in new_enrs:
                    nursery.start_soon(self._bond_then_send, enr)

    async def _worker(self, worker_id: int) -> None:
        """
        Work through the unqueried nodes to explore each of their neighborhoods
        in the network.
        """
        for round in itertools.count():
            async with self._condition:
                try:
                    node_id, radius = first(self._get_nodes_for_exploration())
                except StopIteration:
                    await self._condition.wait()
                    continue

            with self._mark_in_flight(node_id):
                self.queried.add(node_id)

                # Some of the node ids may have come from our routing table.
                # These won't be present in the `received_node_ids` so we
                # detect this here and send them over the channel.
                if node_id not in self.seen:
                    enr = self._network.enr_db.get_enr(node_id)
                    self.seen.add(node_id)

                    try:
                        await self._send_channel.send(enr)
                    except (trio.BrokenResourceError, trio.ClosedResourceError):
                        # In the event that the exploration exits early before
                        # the lookup has completed we can end up operating on a
                        # closed channel.
                        return

                await self._explore(node_id, radius)

            # we need to trigger the condition here so that our "done" check
            # will wake up and once we query our last node and see that there
            # are no more nodes in flight or left to query.
            async with self._condition:
                self._condition.notify_all()

    async def _source_initial_nodes(self) -> None:
        """
        We use RFN to quickly find the nodes closest to the target.
        """
        async with self._network.recursive_find_nodes(self.target) as enr_aiter:
            async for enr in enr_aiter:
                async with self._condition:
                    if enr.node_id not in self.seen:
                        self.seen.add(enr.node_id)
                        self._condition.notify_all()
                        await self._send_channel.send(enr)

        self.logger.info("%s: finished seeding nodes for exploration", self)
        self._exploration_seeded.set()
