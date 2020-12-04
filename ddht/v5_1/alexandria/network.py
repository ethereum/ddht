import logging
from typing import AsyncContextManager, Collection, List, Optional, Tuple

from async_service import Service
from eth_enr import ENRAPI, ENRManagerAPI, QueryableENRDatabaseAPI
from eth_enr.exceptions import OldSequenceNumber
from eth_typing import Hash32, NodeID
from eth_utils import ValidationError
from eth_utils.toolz import cons, first
from lru import LRU
from ssz.constants import CHUNK_SIZE
import trio

from ddht._utils import every, weighted_choice
from ddht.constants import ROUTING_TABLE_BUCKET_SIZE
from ddht.endpoint import Endpoint
from ddht.kademlia import KademliaRoutingTable, at_log_distance
from ddht.token_bucket import TokenBucket
from ddht.v5_1.abc import NetworkAPI
from ddht.v5_1.alexandria.abc import (
    AdvertisementDatabaseAPI,
    AlexandriaNetworkAPI,
    ContentStorageAPI,
)
from ddht.v5_1.alexandria.advertisements import Advertisement
from ddht.v5_1.alexandria.client import AlexandriaClient
from ddht.v5_1.alexandria.content import compute_content_distance
from ddht.v5_1.alexandria.content_provider import ContentProvider
from ddht.v5_1.alexandria.messages import FindNodesMessage, PingMessage, PongMessage
from ddht.v5_1.alexandria.partials._utils import get_chunk_count_for_data_length
from ddht.v5_1.alexandria.partials.chunking import slice_segments_to_max_chunk_count
from ddht.v5_1.alexandria.partials.proof import Proof, compute_proof, validate_proof
from ddht.v5_1.alexandria.payloads import AckPayload, PongPayload
from ddht.v5_1.alexandria.radius_tracker import RadiusTracker
from ddht.v5_1.alexandria.resource_queue import ResourceQueue
from ddht.v5_1.alexandria.sedes import content_sedes
from ddht.v5_1.alexandria.typing import ContentKey
from ddht.v5_1.constants import ROUTING_TABLE_KEEP_ALIVE
from ddht.v5_1.network import common_recursive_find_nodes

NEIGHBORHOOD_DISTANCES = (
    # First bucket is combined (128 + 64 + 32) since these will rarely be
    # occupied.
    tuple(range(1, 224)),
    # Next few buckets drop in size by about half each time.
    tuple(range(224, 240)),
    tuple(range(240, 248)),
    (248, 249, 250, 251),
    (252, 253, 254),
    # This last one is 3/4 of the network
    (255, 256),
)


class AlexandriaNetwork(Service, AlexandriaNetworkAPI):
    logger = logging.getLogger("ddht.Alexandria")

    # Delegate to the AlexandriaClient for determining `protocol_id`
    protocol_id = AlexandriaClient.protocol_id

    def __init__(
        self,
        network: NetworkAPI,
        bootnodes: Collection[ENRAPI],
        content_storage: ContentStorageAPI,
        advertisement_db: AdvertisementDatabaseAPI,
        max_advertisement_count: int = 65536,
    ) -> None:
        self._bootnodes = tuple(bootnodes)

        self.max_advertisement_count = max_advertisement_count

        self.client = AlexandriaClient(network)

        self.radius_tracker = RadiusTracker(self)

        self.routing_table = KademliaRoutingTable(
            self.enr_manager.enr.node_id, ROUTING_TABLE_BUCKET_SIZE,
        )

        self.content_storage = content_storage
        self.content_provider = ContentProvider(
            client=self.client, content_storage=content_storage,
        )

        self.advertisement_db = advertisement_db

        self._last_pong_at = LRU(2048)
        self._routing_table_ready = trio.Event()

        self._ping_handler_ready = trio.Event()
        self._find_nodes_handler_ready = trio.Event()

    async def ready(self) -> None:
        await self._ping_handler_ready.wait()
        await self._find_nodes_handler_ready.wait()

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
    def enr_db(self) -> QueryableENRDatabaseAPI:
        return self.network.enr_db

    async def run(self) -> None:
        self.manager.run_daemon_child_service(self.client)
        self.manager.run_daemon_child_service(self.content_provider)
        self.manager.run_daemon_child_service(self.radius_tracker)

        # Long running processes
        self.manager.run_daemon_task(self._periodically_report_routing_table)
        self.manager.run_daemon_task(self._ping_oldest_routing_table_entry)
        self.manager.run_daemon_task(self._track_last_pong)
        self.manager.run_daemon_task(self._manage_routing_table)
        self.manager.run_daemon_task(self._pong_when_pinged)
        self.manager.run_daemon_task(self._serve_find_nodes)

        await self.manager.wait_finished()

    #
    # Local properties
    #
    @property
    def local_advertisement_radius(self) -> int:
        advertisement_count = self.advertisement_db.count()

        if advertisement_count < self.max_advertisement_count:
            return 2 ** 256 - 1

        furthest_advertisement = first(
            self.advertisement_db.furthest(self.local_node_id)
        )
        return compute_content_distance(
            self.local_node_id, furthest_advertisement.content_id,
        )

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
            enr = await self.network.lookup_enr(
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

    async def _bond(self, node_id: NodeID, endpoint: Optional[Endpoint] = None) -> None:
        await self.bond(node_id, endpoint=endpoint)

    async def ping(
        self,
        node_id: NodeID,
        *,
        enr_seq: Optional[int] = None,
        advertisement_radius: Optional[int] = None,
        endpoint: Optional[Endpoint] = None,
        request_id: Optional[bytes] = None,
    ) -> PongPayload:
        if endpoint is None:
            endpoint = await self.network.endpoint_for_node_id(node_id)
        if enr_seq is None:
            enr_seq = self.network.enr_manager.enr.sequence_number
        if advertisement_radius is None:
            advertisement_radius = self.local_advertisement_radius

        response = await self.client.ping(
            node_id,
            enr_seq=enr_seq,
            advertisement_radius=advertisement_radius,
            endpoint=endpoint,
            request_id=request_id,
        )
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
            endpoint = await self.network.endpoint_for_node_id(node_id)
        responses = await self.client.find_nodes(
            node_id, endpoint, distances=distances, request_id=request_id
        )
        return tuple(
            enr for response in responses for enr in response.message.payload.enrs
        )

    def recursive_find_nodes(
        self, target: NodeID
    ) -> AsyncContextManager[trio.abc.ReceiveChannel[ENRAPI]]:
        return common_recursive_find_nodes(self, target)

    async def get_content_proof(
        self,
        node_id: NodeID,
        *,
        hash_tree_root: Hash32,
        content_key: ContentKey,
        start_chunk_index: int,
        max_chunks: int,
        endpoint: Optional[Endpoint] = None,
        request_id: Optional[bytes] = None,
    ) -> Proof:
        if endpoint is None:
            endpoint = await self.network.endpoint_for_node_id(node_id)

        response = await self.client.get_content(
            node_id,
            endpoint,
            content_key=content_key,
            start_chunk_index=start_chunk_index,
            max_chunks=max_chunks,
            request_id=request_id,
        )
        if response.payload.is_proof:
            proof = Proof.deserialize(
                data=response.payload.payload, sedes=content_sedes,
            )
        else:
            proof = compute_proof(
                content=response.payload.payload, sedes=content_sedes,
            )

        if not proof.get_hash_tree_root() == hash_tree_root:
            raise ValidationError(
                f"Received proof has incorrect `hash_tree_root`: "
                f"{proof.get_hash_tree_root().hex()}"
            )

        validate_proof(proof)
        return proof

    async def get_content_from_nodes(
        self,
        nodes: Collection[Tuple[NodeID, Optional[Endpoint]]],
        *,
        hash_tree_root: Hash32,
        content_key: ContentKey,
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
            [
                (
                    node_id,
                    (
                        endpoint
                        if endpoint is not None
                        else await self.network.endpoint_for_node_id(node_id)
                    ),
                )
                for (node_id, endpoint) in nodes
            ]
        )

        proof_send_channel, proof_receive_channel = trio.open_memory_channel[Proof](16)

        for node_id, endpoint in node_ids_and_endpoints:
            # TODO: timeout handling
            try:
                base_proof = await self.get_content_proof(
                    node_id,
                    hash_tree_root=hash_tree_root,
                    content_key=content_key,
                    start_chunk_index=0,
                    max_chunks=10,
                    endpoint=endpoint,
                )
            except trio.TooSlowError:
                continue
            else:
                content_length = base_proof.get_content_length()
                break
        else:
            raise trio.TooSlowError(
                "Unable to retrieve initial proof from any of the provide nodes"
            )

        missing_segments = base_proof.get_missing_segments()
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
            worker_name = f"Worker[{content_key.hex()}:{worker_id}]"
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
                            content_key=content_key,
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
                            tuple(proof.get_missing_segments())
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
                "STILL MISSING: %s", tuple(base_proof.get_missing_segments()),
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
                            tuple(base_proof.get_missing_segments()),
                        )
                        if base_proof.is_complete:
                            break

            # n
            nursery.cancel_scope.cancel()

        return base_proof

    async def advertise(
        self,
        node_id: NodeID,
        *,
        advertisements: Collection[Advertisement],
        endpoint: Optional[Endpoint] = None,
    ) -> Tuple[AckPayload, ...]:
        if endpoint is None:
            endpoint = await self.network.endpoint_for_node_id(node_id)
        responses = await self.client.advertise(
            node_id, advertisements=advertisements, endpoint=endpoint,
        )
        return tuple(response.payload for response in responses)

    async def locate(
        self,
        node_id: NodeID,
        *,
        content_key: ContentKey,
        endpoint: Optional[Endpoint] = None,
        request_id: Optional[bytes] = None,
    ) -> Tuple[Advertisement, ...]:
        if endpoint is None:
            endpoint = await self.network.endpoint_for_node_id(node_id)
        responses = await self.client.locate(
            node_id, content_key=content_key, endpoint=endpoint, request_id=request_id,
        )
        return tuple(
            advertisement
            for response in responses
            for advertisement in response.message.payload.locations
        )

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
            self._ping_handler_ready.set()

            async for request in subscription:
                await self.client.send_pong(
                    request.sender_node_id,
                    request.sender_endpoint,
                    enr_seq=self.enr_manager.enr.sequence_number,
                    advertisement_radius=self.local_advertisement_radius,
                    request_id=request.request_id,
                )
                enr = await self.network.lookup_enr(
                    request.sender_node_id,
                    enr_seq=request.message.payload.enr_seq,
                    endpoint=request.sender_endpoint,
                )
                self.routing_table.update(enr.node_id)
                self._routing_table_ready.set()

    async def _serve_find_nodes(self) -> None:
        async with self.client.subscribe(FindNodesMessage) as subscription:
            self._find_nodes_handler_ready.set()

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
