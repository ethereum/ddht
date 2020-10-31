import io
import itertools
import logging
import secrets
from typing import Collection, List, Optional, Set, Tuple

from async_service import Service
from eth_enr import ENRAPI, ENRDatabaseAPI, ENRManagerAPI
from eth_enr.exceptions import OldSequenceNumber
from eth_typing import Hash32, NodeID
from eth_utils import ValidationError
from eth_utils.toolz import cons, first, take
from lru import LRU
from ssz.constants import CHUNK_SIZE
import trio

from ddht._utils import every, reduce_enrs
from ddht.constants import ROUTING_TABLE_BUCKET_SIZE
from ddht.endpoint import Endpoint
from ddht.kademlia import (
    KademliaRoutingTable,
    compute_distance,
    compute_log_distance,
    iter_closest_nodes,
)
from ddht.v5_1.abc import NetworkAPI
from ddht.v5_1.alexandria.abc import AlexandriaNetworkAPI
from ddht.v5_1.alexandria.client import AlexandriaClient
from ddht.v5_1.alexandria.content import content_key_to_content_id
from ddht.v5_1.alexandria.messages import FindNodesMessage, PingMessage, PongMessage
from ddht.v5_1.alexandria.partials._utils import get_chunk_count_for_data_length
from ddht.v5_1.alexandria.partials.chunking import slice_segments_to_max_chunk_count
from ddht.v5_1.alexandria.partials.proof import Proof, compute_proof, validate_proof
from ddht.v5_1.alexandria.payloads import PongPayload
from ddht.v5_1.alexandria.resource_queue import ResourceQueue
from ddht.v5_1.alexandria.sedes import content_sedes
from ddht.v5_1.constants import ROUTING_TABLE_KEEP_ALIVE


class AlexandriaNetwork(Service, AlexandriaNetworkAPI):
    logger = logging.getLogger("ddht.Alexandria")

    # Delegate to the AlexandriaClient for determining `protocol_id`
    protocol_id = AlexandriaClient.protocol_id

    def __init__(self, network: NetworkAPI, bootnodes: Collection[ENRAPI]) -> None:
        self._bootnodes = tuple(bootnodes)

        self.client = AlexandriaClient(network)

        self.routing_table = KademliaRoutingTable(
            self.enr_manager.enr.node_id, ROUTING_TABLE_BUCKET_SIZE,
        )

        self._last_pong_at = LRU(2048)
        self._routing_table_ready = trio.Event()

    @property
    def network(self) -> NetworkAPI:
        return self.client.network

    @property
    def local_node_id(self) -> NodeID:
        return self.network.local_node_id

    @property
    def enr_manager(self) -> ENRManagerAPI:
        return self.network.enr_manager

    @property
    def enr_db(self) -> ENRDatabaseAPI:
        return self.network.enr_db

    async def run(self) -> None:
        self.manager.run_daemon_child_service(self.client)

        # Long running processes
        self.manager.run_daemon_task(self._periodically_report_routing_table)
        self.manager.run_daemon_task(self._ping_oldest_routing_table_entry)
        self.manager.run_daemon_task(self._track_last_pong)
        self.manager.run_daemon_task(self._manage_routing_table)
        self.manager.run_daemon_task(self._pong_when_pinged)
        self.manager.run_daemon_task(self._serve_find_nodes)

        await self.manager.wait_finished()

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

        try:
            enr = await self.lookup_enr(
                node_id, enr_seq=pong.enr_seq, endpoint=endpoint
            )
        except trio.TooSlowError:
            self.logger.debug(
                "Bonding with %s timed out during ENR retrieval", node_id.hex(),
            )
            return False

        self.routing_table.update(enr.node_id)

        self.logger.debug(
            "Bonded with %s successfully", node_id.hex(),
        )

        self._routing_table_ready.set()
        return True

    async def _bond(self, node_id: NodeID, endpoint: Endpoint) -> None:
        await self.bond(node_id, endpoint=endpoint)

    async def ping(
        self, node_id: NodeID, *, endpoint: Optional[Endpoint] = None,
    ) -> PongPayload:
        if endpoint is None:
            endpoint = self._endpoint_for_node_id(node_id)
        response = await self.client.ping(node_id, endpoint=endpoint)
        return response.payload

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
            endpoint = self._endpoint_for_node_id(node_id)
        responses = await self.client.find_nodes(
            node_id, endpoint, distances=distances, request_id=request_id
        )
        return tuple(
            enr for response in responses for enr in response.message.payload.enrs
        )

    async def lookup_enr(
        self, node_id: NodeID, *, enr_seq: int = 0, endpoint: Optional[Endpoint] = None
    ) -> ENRAPI:
        try:
            enr = self.enr_db.get_enr(node_id)

            if enr.sequence_number >= enr_seq:
                return enr
        except KeyError:
            if endpoint is None:
                # we weren't given an endpoint and we don't have an enr which would give
                # us an endpoint, there's no way to reach this node.
                raise

        enr = await self._fetch_enr(node_id, endpoint=endpoint)
        self.enr_db.set_enr(enr)

        return enr

    async def _fetch_enr(
        self, node_id: NodeID, *, endpoint: Optional[Endpoint]
    ) -> ENRAPI:
        enrs = await self.find_nodes(node_id, 0, endpoint=endpoint)
        if not enrs:
            raise Exception("Invalid response")
        # This reduce accounts for
        return reduce_enrs(enrs)[0]

    async def recursive_find_nodes(self, target: NodeID) -> Tuple[ENRAPI, ...]:
        self.logger.debug("Recursive find nodes: %s", target.hex())

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
                if candidate not in queried_node_ids and candidate != self.local_node_id
            )
            nodes_to_query = tuple(take(3, closest_k_unqueried_candidates))

            if nodes_to_query:
                self.logger.debug(
                    "Starting lookup round %d for %s",
                    lookup_round_counter + 1,
                    target.hex(),
                )
                async with trio.open_nursery() as nursery:
                    for peer in nodes_to_query:
                        nursery.start_soon(do_lookup, peer)
            else:
                self.logger.debug(
                    "Lookup for %s finished in %d rounds",
                    target.hex(),
                    lookup_round_counter,
                )
                break

        # now sort and return the ENR records in order of closesness to the target.
        return tuple(
            sorted(
                reduce_enrs(received_enrs),
                key=lambda enr: compute_distance(enr.node_id, target),
            )
        )

    async def get_content_proof(
        self,
        node_id: NodeID,
        *,
        hash_tree_root: Hash32,
        key: bytes,
        start_chunk_index: int,
        max_chunks: int,
        endpoint: Optional[Endpoint] = None,
        request_id: Optional[bytes] = None,
    ) -> Proof:
        if endpoint is None:
            endpoint = self._endpoint_for_node_id(node_id)

        content_id = content_key_to_content_id(key)
        response = await self.client.get_content(
            node_id,
            endpoint,
            content_id=content_id,
            start_chunk_index=start_chunk_index,
            max_chunks=max_chunks,
            request_id=request_id,
        )
        if response.payload.is_proof:
            proof = Proof.deserialize(
                stream=io.BytesIO(response.payload.payload),
                hash_tree_root=hash_tree_root,
                sedes=content_sedes,
            )
        else:
            proof = compute_proof(
                content=response.payload.payload, sedes=content_sedes,
            )

        validate_proof(proof)
        return proof

    async def get_content_from_nodes(
        self,
        nodes: Collection[Tuple[NodeID, Optional[Endpoint]]],
        *,
        hash_tree_root: Hash32,
        key: bytes,
        concurrency: int = 4,
    ) -> Proof:
        """
        Rough sketch of retrieval algorithm.

        1. Fetch the proof for the first chunk:

            - Maybe early exit if the content is small and we get the full data
              in one request.

        2. Extract the content length from the first chunk and create a queue
           of the chunks we still need.
        3. Create a queue for the nodes that we will fetch data from.
        4. Start up to `concurrency` *workers* processes:
            - worker grabs available node from queue
            - worker grabs chunk range from queue
            - worker requests proof, validates proof contains data starting at
              the requested `start_chunk_index`.
            - *IF* proof is valid
                - push proof into the proof queue
                - push node back onto available nodes queue
                - push any needed chunks that aren't in the proof onto the proof queue.
            - *OTHERWISE*
                - penalize node???
                - place needed chunk back in the queue
        5. Outer process collects proofs from proof queue, merging them
           together.  Once merged proof contains full data, stop workers and
           return the merged data.

        """
        if not nodes:
            raise Exception("Must provide at least one node")

        # Fill out all of the `Endpoint` objects that were not expicitely
        # provided.
        node_ids_and_endpoints = tuple(
            (
                node_id,
                (
                    endpoint
                    if endpoint is not None
                    else self._endpoint_for_node_id(node_id)
                ),
            )
            for (node_id, endpoint) in nodes
        )

        proof_send_channel, proof_receive_channel = trio.open_memory_channel[Proof](16)

        for node_id, endpoint in node_ids_and_endpoints:
            # TODO: timeout handling
            try:
                base_proof = await self.get_content_proof(
                    node_id,
                    hash_tree_root=hash_tree_root,
                    key=key,
                    start_chunk_index=0,
                    max_chunks=10,
                    endpoint=endpoint,
                )
            except trio.TooSlowError:
                continue
            else:
                tree = base_proof.get_tree()
                content_length = tree.get_data_length()
                break
        else:
            raise trio.TooSlowError(
                "Unable to retrieve initial proof from any of the provide nodes"
            )

        missing_segments = base_proof.get_missing_segments(content_length)
        bite_size_missing_segments = slice_segments_to_max_chunk_count(
            missing_segments, max_chunk_count=16,
        )

        # The size of the `segment_queue` mitigates against an attack scenario
        # where a malicious node gives us back proofs that contain the first
        # chunk which is required, but then contains as many gaps as possible
        # in the requested data.  At a `max_chunk_count` of 16, a valid proof
        # can push four new sub-segments onto the queue.  If *all* of our
        # connected peers are giving us this type of malicious response, our
        # queue will quickly grow to the maximum size at which point the
        # deadlock protection will be triggered within the worker process.
        #
        # This situation is mitigated by both setting our queue size to a
        # sufficiently large size that we expect it to not be able to be
        # filled, as well as allowing our worker process to timeout while
        # trying to add sub-segments back into the queue.
        segment_queue = ResourceQueue(bite_size_missing_segments * 4)
        node_queue = ResourceQueue(node_ids_and_endpoints)

        async def _worker(worker_id: int) -> None:
            worker_name = f"Worker[{key.hex()}:{worker_id}]"
            while True:
                async with node_queue.reserve() as node_id_and_endpoint:
                    node_id, endpoint = node_id_and_endpoint
                    self.logger.debug(
                        "%s: reserved node: %s", worker_name, node_id.hex()
                    )
                    async with segment_queue.reserve() as segment:
                        start_data_index, data_length = segment

                        start_chunk_index = start_data_index // CHUNK_SIZE
                        max_chunks = get_chunk_count_for_data_length(data_length)

                        self.logger.debug(
                            "%s: reserved chunk: start_index=%d  max_chunks=%d",
                            worker_name,
                            start_chunk_index,
                            max_chunks,
                        )
                        proof = await self.get_content_proof(
                            node_id,
                            hash_tree_root=hash_tree_root,
                            key=key,
                            start_chunk_index=start_chunk_index,
                            max_chunks=max_chunks,
                            endpoint=endpoint,
                        )

                        try:
                            validate_proof(proof)
                        except ValidationError:
                            # If a peer gives us an invalid proof, remove them
                            # from rotation.
                            node_queue.remove(node_id_and_endpoint)
                            continue

                        # check that the proof contains at minimum the first chunk we requested.
                        if not proof.has_chunk(start_chunk_index):
                            # If the peer didn't include the start chunk,
                            # remove them from rotation.
                            node_queue.remove(node_id_and_endpoint)
                            continue

                        await proof_send_channel.send(proof)

                        # Determine if there are any subsections to this
                        # segment that are still missing.
                        remaining_segments = segment.intersection(
                            tuple(proof.get_missing_segments(content_length))
                        )

                        # This *timeout* ensures that the workers will not deadlock on
                        # a full `segment_queue`.  In the case where we hit this
                        # timeout we may end up re-requesting a proof that we already
                        # have but that is *ok* and doesn't cause anything to break.
                        with trio.move_on_after(2):
                            # Remove the segment and push any still missing
                            # sub-segments onto the queue
                            for sub_segment in remaining_segments:
                                await segment_queue.add(sub_segment)

                            # It is important that the removal happen *after*
                            # we push the sub-segments on, otherwise, if we
                            # timeout after the main segment has been removed
                            # but before the sub-segments have been added,
                            # we'll lose track of the still missing
                            # sub-segments.
                            segment_queue.remove(segment)

        async with trio.open_nursery() as nursery:
            for worker_id in range(concurrency):
                nursery.start_soon(_worker, worker_id)

            self.logger.info(
                "STILL MISSING: %s",
                tuple(base_proof.get_missing_segments(content_length)),
            )
            if base_proof.is_complete:
                proven_data = base_proof.get_proven_data()
                assert len(proven_data[:content_length]) == content_length
            else:
                async with proof_receive_channel:
                    async for partial_proof in proof_receive_channel:
                        self.logger.debug("Re-assembling proof: %s", base_proof)
                        base_proof = base_proof.merge(partial_proof)
                        self.logger.info(
                            "STILL MISSING: %s",
                            tuple(base_proof.get_missing_segments(content_length)),
                        )
                        if base_proof.is_complete:
                            break

            # n
            nursery.cancel_scope.cancel()

        return base_proof

    #
    # Long Running Processes
    #
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

    async def _pong_when_pinged(self) -> None:
        async with self.client.subscribe(PingMessage) as subscription:
            async for request in subscription:
                await self.client.send_pong(
                    request.sender_node_id,
                    request.sender_endpoint,
                    enr_seq=self.enr_manager.enr.sequence_number,
                    request_id=request.request_id,
                )

    async def _serve_find_nodes(self) -> None:
        async with self.client.subscribe(FindNodesMessage) as subscription:
            async for request in subscription:
                response_enrs: List[ENRAPI] = []
                distances = set(request.message.payload.distances)
                if len(distances) != len(request.message.payload.distances):
                    self.logger.debug(
                        "Ignoring invalid FindNodesMessage from %s@%s: duplicate distances",
                        request.sender_node_id.hex(),
                        request.sender_endpoint,
                    )
                    continue
                elif not distances:
                    self.logger.debug(
                        "Ignoring invalid FindNodesMessage from %s@%s: empty distances",
                        request.sender_node_id.hex(),
                        request.sender_endpoint,
                    )
                    continue
                elif any(
                    distance > self.routing_table.num_buckets for distance in distances
                ):
                    self.logger.debug(
                        "Ignoring invalid FindNodesMessage from %s@%s: distances: %s",
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

                await self.client.send_found_nodes(
                    request.sender_node_id,
                    request.sender_endpoint,
                    enrs=response_enrs,
                    request_id=request.request_id,
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
        async with self.client.subscribe(PongMessage) as subscription:
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
                    endpoint = Endpoint.from_enr(enr)
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
                    if enr.node_id == self.local_node_id:
                        continue
                    endpoint = Endpoint.from_enr(enr)
                    nursery.start_soon(self._bond, enr.node_id, endpoint)

    #
    # Utility
    #
    def _endpoint_for_node_id(self, node_id: NodeID) -> Endpoint:
        enr = self.enr_db.get_enr(node_id)
        return Endpoint.from_enr(enr)
