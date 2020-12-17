from typing import (
    AsyncContextManager,
    AsyncIterator,
    Collection,
    List,
    Optional,
    Set,
    Tuple,
    Union,
)

from async_generator import asynccontextmanager
from async_service import Service, background_trio_service
from eth_enr import ENRAPI, ENRManagerAPI, QueryableENRDatabaseAPI
from eth_enr.exceptions import OldSequenceNumber
from eth_typing import Hash32, NodeID
from eth_utils import ValidationError, get_extended_debug_logger
from eth_utils.toolz import cons, first, take
from lru import LRU
import trio

from ddht._utils import every, humanize_bytes, weighted_choice
from ddht.constants import ROUTING_TABLE_BUCKET_SIZE
from ddht.endpoint import Endpoint
from ddht.exceptions import MissingEndpointFields
from ddht.kademlia import KademliaRoutingTable, at_log_distance
from ddht.token_bucket import TokenBucket
from ddht.v5_1.abc import NetworkAPI
from ddht.v5_1.alexandria._utils import humanize_advertisement_radius
from ddht.v5_1.alexandria.abc import (
    AdvertisementDatabaseAPI,
    AlexandriaNetworkAPI,
    ContentRetrievalAPI,
    ContentStorageAPI,
)
from ddht.v5_1.alexandria.advertisement_collector import AdvertisementCollector
from ddht.v5_1.alexandria.advertisement_manager import AdvertisementManager
from ddht.v5_1.alexandria.advertisement_provider import AdvertisementProvider
from ddht.v5_1.alexandria.advertisements import Advertisement, partition_advertisements
from ddht.v5_1.alexandria.broadcast_log import BroadcastLog
from ddht.v5_1.alexandria.client import AlexandriaClient
from ddht.v5_1.alexandria.constants import (
    DEFAULT_COMMONS_STORAGE_SIZE,
    DEFAULT_MAX_ADVERTISEMENTS,
    MAX_PAYLOAD_SIZE,
)
from ddht.v5_1.alexandria.content import (
    compute_content_distance,
    content_key_to_content_id,
)
from ddht.v5_1.alexandria.content_collector import ContentCollector
from ddht.v5_1.alexandria.content_manager import ContentManager
from ddht.v5_1.alexandria.content_provider import ContentProvider
from ddht.v5_1.alexandria.content_retrieval import ContentRetrieval
from ddht.v5_1.alexandria.content_validator import ContentValidator
from ddht.v5_1.alexandria.messages import FindNodesMessage, PingMessage, PongMessage
from ddht.v5_1.alexandria.partials.proof import Proof, compute_proof, validate_proof
from ddht.v5_1.alexandria.payloads import AckPayload, PongPayload
from ddht.v5_1.alexandria.radius_tracker import RadiusTracker
from ddht.v5_1.alexandria.sedes import content_sedes
from ddht.v5_1.alexandria.typing import ContentID, ContentKey
from ddht.v5_1.constants import ROUTING_TABLE_KEEP_ALIVE
from ddht.v5_1.explorer import Explorer
from ddht.v5_1.network import common_recursive_find_nodes


class AlexandriaNetwork(Service, AlexandriaNetworkAPI):
    # Delegate to the AlexandriaClient for determining `protocol_id`
    protocol_id = AlexandriaClient.protocol_id

    def __init__(
        self,
        network: NetworkAPI,
        bootnodes: Collection[ENRAPI],
        commons_content_storage: ContentStorageAPI,
        pinned_content_storage: ContentStorageAPI,
        local_advertisement_db: AdvertisementDatabaseAPI,
        remote_advertisement_db: AdvertisementDatabaseAPI,
        max_advertisement_count: int = DEFAULT_MAX_ADVERTISEMENTS,
        commons_content_storage_max_size: int = DEFAULT_COMMONS_STORAGE_SIZE,
    ) -> None:
        self.logger = get_extended_debug_logger("ddht.Alexandria")

        self._bootnodes = tuple(bootnodes)

        self.max_advertisement_count = max_advertisement_count

        self.client = AlexandriaClient(network)

        self.radius_tracker = RadiusTracker(self)
        self.broadcast_log = BroadcastLog()

        self.routing_table = KademliaRoutingTable(
            self.enr_manager.enr.node_id, ROUTING_TABLE_BUCKET_SIZE,
        )

        self.commons_content_storage = commons_content_storage
        self.commons_content_storage_max_size = commons_content_storage_max_size
        self.commons_content_manager = ContentManager(
            self,
            commons_content_storage,
            max_size=self.commons_content_storage_max_size,
        )
        self.commons_content_collector = ContentCollector(
            self, self.commons_content_manager
        )

        self.pinned_content_storage = pinned_content_storage
        self.pinned_content_manager = ContentManager(self, pinned_content_storage)

        self.content_provider = ContentProvider(
            client=self.client,
            content_storages=(commons_content_storage, pinned_content_storage),
        )
        self.content_validator = ContentValidator(self)

        self.local_advertisement_db = local_advertisement_db
        self.local_advertisement_manager = AdvertisementManager(
            network=self,
            advertisement_db=local_advertisement_db,
            max_advertisement_count=None,
        )

        self.remote_advertisement_db = remote_advertisement_db
        self.remote_advertisement_manager = AdvertisementManager(
            network=self,
            advertisement_db=remote_advertisement_db,
            max_advertisement_count=max_advertisement_count,
        )

        self.advertisement_collector = AdvertisementCollector(
            network=self,
            local_advertisement_manager=self.local_advertisement_manager,
            remote_advertisement_manager=self.remote_advertisement_manager,
        )
        self.advertisement_provider = AdvertisementProvider(
            client=self.client,
            advertisement_dbs=(
                self.local_advertisement_db,
                self.remote_advertisement_db,
            ),
        )

        self.radius_tracker = RadiusTracker(self)

        self._last_pong_at = LRU(2048)
        self._routing_table_ready = trio.Event()

        self._ping_handler_ready = trio.Event()
        self._find_nodes_handler_ready = trio.Event()

    async def routing_table_ready(self) -> None:
        await self._routing_table_ready.wait()

    async def ready(self) -> None:
        await self.local_advertisement_manager.ready()
        await self.remote_advertisement_manager.ready()
        await self.advertisement_provider.ready()
        await self.advertisement_collector.ready()
        await self.content_provider.ready()
        await self.commons_content_collector.ready()
        await self.radius_tracker.ready()

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
        # Long running processes
        self.manager.run_daemon_task(self._periodically_report_status)
        self.manager.run_daemon_task(self._periodically_report_routing_table)
        self.manager.run_daemon_task(self._ping_oldest_routing_table_entry)
        self.manager.run_daemon_task(self._track_last_pong)
        self.manager.run_daemon_task(self._manage_routing_table)
        self.manager.run_daemon_task(self._pong_when_pinged)
        self.manager.run_daemon_task(self._serve_find_nodes)

        # Child services
        self.manager.run_daemon_child_service(self.client)
        self.manager.run_daemon_child_service(self.local_advertisement_manager)
        self.manager.run_daemon_child_service(self.remote_advertisement_manager)
        self.manager.run_daemon_child_service(self.advertisement_collector)
        self.manager.run_daemon_child_service(self.advertisement_provider)
        self.manager.run_daemon_child_service(self.content_provider)
        self.manager.run_daemon_child_service(self.commons_content_manager)
        self.manager.run_daemon_child_service(self.commons_content_collector)
        self.manager.run_daemon_child_service(self.pinned_content_manager)
        self.manager.run_daemon_child_service(self.radius_tracker)

        await self.manager.wait_finished()

    #
    # Local properties
    #
    @property
    def local_advertisement_radius(self) -> int:
        return self.remote_advertisement_manager.advertisement_radius

    #
    # High Level API
    #
    async def bond(
        self, node_id: NodeID, *, endpoint: Optional[Endpoint] = None
    ) -> bool:
        self.logger.debug2(
            "Bonding with %s", node_id.hex(),
        )

        try:
            pong = await self.ping(node_id, endpoint=endpoint)
        except trio.TooSlowError:
            self.logger.debug("Bonding with %s timed out during ping", node_id.hex())
            return False
        except KeyError:
            self.logger.debug(
                "Unable to lookup endpoint information for node: %s", node_id.hex()
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

        self.routing_table.update(enr.node_id)

        self.logger.debug(
            "Bonded with %s successfully", node_id.hex(),
        )

        self._routing_table_ready.set()
        return True

    async def _bond(self, node_id: NodeID, endpoint: Optional[Endpoint] = None) -> None:
        await self.bond(node_id, endpoint=endpoint)

    async def lookup_enr(
        self, node_id: NodeID, *, enr_seq: int = 0, endpoint: Optional[Endpoint] = None
    ) -> ENRAPI:
        return await self.network.lookup_enr(
            node_id, enr_seq=enr_seq, endpoint=endpoint
        )

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
        self, target: Union[NodeID, ContentID],
    ) -> AsyncContextManager[trio.abc.ReceiveChannel[ENRAPI]]:
        return common_recursive_find_nodes(self, NodeID(target))

    @asynccontextmanager
    async def explore(
        self, target: NodeID, concurrency: int = 3,
    ) -> AsyncIterator[trio.abc.ReceiveChannel[ENRAPI]]:
        explorer = Explorer(self, target, concurrency)
        with trio.fail_after(300):
            async with background_trio_service(explorer):
                await explorer.ready()

                async with explorer.stream() as receive_channel:
                    yield receive_channel

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
            # TODO: computationally expensive
            proof = compute_proof(
                content=response.payload.payload, sedes=content_sedes,
            )

        # TODO: computationally expensive
        if not proof.get_hash_tree_root() == hash_tree_root:
            raise ValidationError(
                f"Received proof has incorrect `hash_tree_root`: "
                f"{proof.get_hash_tree_root().hex()}"
            )

        # TODO: computationally expensive
        validate_proof(proof)
        return proof

    @asynccontextmanager
    async def retrieve_content(
        self, content_key: ContentKey, hash_tree_root: Hash32, concurrency: int = 3
    ) -> AsyncIterator[ContentRetrievalAPI]:
        content_retrieval = ContentRetrieval(
            self, content_key, hash_tree_root, concurrency=concurrency,
        )
        with trio.fail_after(300):
            async with background_trio_service(content_retrieval):
                yield content_retrieval

    async def get_content(
        self, content_key: ContentKey, hash_tree_root: Hash32, *, concurrency: int = 3,
    ) -> Proof:
        async def _feed_content_retrieval(
            content_retrieval: ContentRetrievalAPI,
        ) -> None:
            stream_locations_ctx = self.stream_locations(
                content_key, hash_tree_root=hash_tree_root, concurrency=concurrency,
            )
            async with stream_locations_ctx as advertisements_aiter:
                async for advertisement in advertisements_aiter:
                    if advertisement.node_id == self.local_node_id:
                        continue
                    await content_retrieval.node_queue.add(advertisement.node_id)

        content_retrieval_ctx = self.retrieve_content(
            content_key, hash_tree_root, concurrency=concurrency,
        )
        async with content_retrieval_ctx as content_retrieval:
            async with trio.open_nursery() as nursery:
                nursery.start_soon(_feed_content_retrieval, content_retrieval)

                proof = await content_retrieval.wait_content_proof()

                nursery.cancel_scope.cancel()
                return proof

    async def advertise(
        self,
        node_id: NodeID,
        *,
        advertisements: Collection[Advertisement],
        endpoint: Optional[Endpoint] = None,
    ) -> AckPayload:
        if not all(ad.is_valid for ad in advertisements):
            raise Exception("Cannot send invalid advertisements")
        if endpoint is None:
            endpoint = await self.network.endpoint_for_node_id(node_id)

        advertisement_batches = partition_advertisements(
            advertisements, max_payload_size=MAX_PAYLOAD_SIZE,
        )
        responses = tuple(
            [
                await self.client.advertise(node_id, endpoint, advertisements=batch)
                for batch in advertisement_batches
            ]
        )

        for batch, response in zip(advertisement_batches, responses):
            if len(batch) != len(response.payload.acked):
                raise ValidationError(
                    f"Invalid response: acked={len(response.payload.acked)}  "
                    f"expected={len(batch)}"
                )

        advertisement_radius = min(
            response.payload.advertisement_radius for response in responses
        )
        acked = tuple(
            was_acked for response in responses for was_acked in response.payload.acked
        )
        return AckPayload(advertisement_radius, acked)

    async def locate(
        self,
        node_id: NodeID,
        *,
        content_key: ContentKey,
        endpoint: Optional[Endpoint] = None,
        request_id: Optional[bytes] = None,
    ) -> Tuple[Advertisement, ...]:
        stream_locate_ctx = self.stream_locate(
            node_id, content_key=content_key, endpoint=endpoint, request_id=request_id,
        )
        async with stream_locate_ctx as advertisement_aiter:
            return tuple([advertisement async for advertisement in advertisement_aiter])

    @asynccontextmanager
    async def stream_locate(
        self,
        node_id: NodeID,
        *,
        content_key: ContentKey,
        endpoint: Optional[Endpoint] = None,
        request_id: Optional[bytes] = None,
    ) -> AsyncIterator[trio.abc.ReceiveChannel[Advertisement]]:
        async def _feed_advertisements(
            send_channel: trio.abc.SendChannel[Advertisement],
        ) -> None:
            nonlocal endpoint

            if endpoint is None:
                endpoint = await self.network.endpoint_for_node_id(node_id)

            stream_locate_ctx = self.client.stream_locate(
                node_id,
                content_key=content_key,
                endpoint=endpoint,
                request_id=request_id,
            )
            async with send_channel:
                async with stream_locate_ctx as response_aiter:
                    seen_totals = set()

                    async for response in response_aiter:
                        seen_totals.add(response.message.payload.total)

                        if response.message.payload.total == 0:
                            raise ValidationError("Invalid message total: total=0")
                        elif len(seen_totals) != 1:
                            raise ValidationError(
                                f"Inconsisten message totals: {sorted(tuple(seen_totals))}"
                            )

                        advertisements = response.message.payload.locations

                        if not all(
                            advertisement.is_valid for advertisement in advertisements
                        ):
                            raise ValidationError(
                                f"Response contains invalid advertisements: "
                                f"advertisements={advertisements}"
                            )

                        unexpected_content_keys = tuple(
                            sorted(
                                set(
                                    advertisement.content_key
                                    for advertisement in advertisements
                                    if advertisement.content_key != content_key
                                )
                            )
                        )
                        if unexpected_content_keys:
                            raise ValidationError(
                                f"Response contains unerquested content keys: "
                                f"content_keys={unexpected_content_keys}"
                            )

                        for advertisement in response.message.payload.locations:
                            await send_channel.send(advertisement)

        send_channel, receive_channel = trio.open_memory_channel[Advertisement](32)

        with trio.fail_after(300):
            async with trio.open_nursery() as nursery:
                nursery.start_soon(_feed_advertisements, send_channel)

                async with receive_channel:
                    yield receive_channel

                nursery.cancel_scope.cancel()

    @asynccontextmanager
    async def stream_locations(
        self,
        content_key: ContentKey,
        *,
        hash_tree_root: Optional[Hash32] = None,
        concurrency: int = 3,
    ) -> AsyncIterator[trio.abc.ReceiveChannel[Advertisement]]:
        content_id = content_key_to_content_id(content_key)

        async def _feed_advertisements_from_db(
            advertisement_db: AdvertisementDatabaseAPI,
            ad_send_channel: trio.abc.SendChannel[Advertisement],
        ) -> None:
            advertisements = tuple(
                take(
                    32,
                    advertisement_db.query(
                        content_key=content_key, hash_tree_root=hash_tree_root,
                    ),
                )
            )
            async with ad_send_channel:
                for advertisement in advertisements:
                    await ad_send_channel.send(advertisement)

        async def _feed_candidate_nodes(
            work_send_channel: trio.abc.SendChannel[NodeID],
        ) -> None:
            async with work_send_channel:
                async with self.explore(content_id) as enr_aiter:
                    async for enr in enr_aiter:
                        if enr.node_id == self.local_node_id:
                            continue
                        await work_send_channel.send(enr.node_id)

        async def _worker(
            worker_id: int,
            work_receive_channel: trio.abc.ReceiveChannel[NodeID],
            ad_send_channel: trio.abc.SendChannel[Advertisement],
        ) -> None:
            async with ad_send_channel:
                async for node_id in work_receive_channel:
                    distance_to_content = compute_content_distance(node_id, content_id)
                    try:
                        advertisement_radius = await self.radius_tracker.get_advertisement_radius(
                            node_id
                        )
                    except trio.TooSlowError:
                        continue

                    if distance_to_content > advertisement_radius:
                        continue

                    stream_locate_ctx = self.stream_locate(
                        node_id, content_key=content_key,
                    )
                    try:
                        async with stream_locate_ctx as advertisement_aiter:
                            async for advertisement in advertisement_aiter:
                                if hash_tree_root is not None:
                                    if advertisement.hash_tree_root != hash_tree_root:
                                        continue
                                await ad_send_channel.send(advertisement)
                    except (trio.TooSlowError, MissingEndpointFields):
                        continue

        work_send_channel, work_receive_channel = trio.open_memory_channel[NodeID](
            concurrency
        )
        ad_send_channel, ad_receive_channel = trio.open_memory_channel[Advertisement](
            32
        )

        with trio.fail_after(300):
            async with trio.open_nursery() as nursery:
                nursery.start_soon(
                    _feed_advertisements_from_db,
                    self.remote_advertisement_db,
                    ad_send_channel.clone(),
                )
                nursery.start_soon(
                    _feed_advertisements_from_db,
                    self.local_advertisement_db,
                    ad_send_channel.clone(),
                )
                nursery.start_soon(_feed_candidate_nodes, work_send_channel)

                for worker_id in range(concurrency):
                    nursery.start_soon(
                        _worker,
                        worker_id,
                        work_receive_channel,
                        ad_send_channel.clone(),
                    )
                await ad_send_channel.aclose()

                async with ad_receive_channel:
                    yield ad_receive_channel

                nursery.cancel_scope.cancel()

    async def broadcast(
        self, advertisement: Advertisement, redundancy_factor: int = 3
    ) -> Tuple[NodeID, ...]:
        self.logger.debug("Broadcasting: advertisement=%s", advertisement)
        start_at = trio.current_time()
        acked_nodes: Set[NodeID] = set()

        # Use the redundancy_factor also as the concurrency limit
        lock = trio.Semaphore(redundancy_factor)
        condition = trio.Condition()

        num_tried = 0

        async def _do_advertise(node_id: NodeID) -> None:
            nonlocal acked_nodes
            nonlocal num_tried

            async with lock:
                if len(acked_nodes) >= redundancy_factor:
                    return

                # verify we haven't recently sent this node the same advertisement
                if self.broadcast_log.was_logged(node_id, advertisement):
                    return

                # verify the node should be interested in the advertisement based
                # on their advertisement radius.
                try:
                    advertisement_radius = await self.radius_tracker.get_advertisement_radius(
                        node_id,
                    )
                except trio.TooSlowError:
                    return

                distance_to_content = compute_content_distance(
                    node_id, advertisement.content_id
                )

                if distance_to_content > advertisement_radius:
                    return

                num_tried += 1

                # attempt to send the advertisement to the node.
                try:
                    ack_payload = await self.advertise(
                        node_id, advertisements=(advertisement,)
                    )
                except trio.TooSlowError:
                    self.logger.debug(
                        "Broadcast timeout: node_id=%s  advertisement=%s",
                        node_id.hex(),
                        advertisement,
                    )
                    return
                except MissingEndpointFields:
                    self.logger.debug(
                        "Unreachable node: node_id=%s  advertisement=%s",
                        node_id.hex(),
                        advertisement,
                    )
                    return
                else:
                    if all(ack_payload.acked):
                        self.logger.debug(
                            "Broadcast successful: node_id=%s  advertisement=%s",
                            node_id.hex(),
                            advertisement,
                        )
                        # log the broadcast
                        acked_nodes.add(node_id)
                    self.broadcast_log.log(node_id, advertisement)
                finally:
                    async with condition:
                        condition.notify_all()

        async with trio.open_nursery() as nursery:

            async def _source_nodes_for_broadcast() -> None:
                async with self.explore(advertisement.content_id) as enr_aiter:
                    async for enr in enr_aiter:
                        if enr.node_id == self.local_node_id:
                            continue

                        if len(acked_nodes) >= redundancy_factor:
                            break

                        nursery.start_soon(_do_advertise, enr.node_id)

            nursery.start_soon(_source_nodes_for_broadcast)

            # exit as soon as there are either no more child tasks or we have
            # successfully broadcast to enough nodes.
            while nursery.child_tasks and len(acked_nodes) < redundancy_factor:
                with trio.move_on_after(1):
                    async with condition:
                        await condition.wait()

        elapsed = trio.current_time() - start_at
        self.logger.debug(
            "Broadcast: acked=%d  tried=%d  elapsed=%0.2f",
            len(acked_nodes),
            num_tried,
            elapsed,
        )

        return tuple(acked_nodes)

    #
    # Long Running Processes
    #
    async def _periodically_report_status(self) -> None:
        if self.remote_advertisement_manager.max_advertisement_count is None:
            # this fixes a type hinting error below since this value is Optional[int]
            raise Exception("Invalid")
        if self.commons_content_manager.max_size is None:
            # this fixes a type hinting error below since this value is Optional[int]
            raise Exception("Invalid")

        async for _ in every(5, initial_delay=5):
            num_remote_ads = self.remote_advertisement_db.count()
            max_remote_ads = self.remote_advertisement_manager.max_advertisement_count
            remote_ad_capacity = 100 * num_remote_ads / max_remote_ads

            commons_size = self.commons_content_storage.total_size()
            commons_max_size = self.commons_content_manager.max_size
            commons_capacity = 100 * commons_size / commons_max_size

            radius_display = humanize_advertisement_radius(
                self.remote_advertisement_manager.advertisement_radius
            )
            self.logger.info(
                "status: commons=%d (%s/%s %.1f%%)  pinned=%d (%s)  local-ads=%d  "
                "remote-ads=%d/%d (%.1f%%)  radius=%.2f",
                len(self.commons_content_storage),
                humanize_bytes(commons_size),
                humanize_bytes(commons_max_size),
                commons_capacity,
                len(self.pinned_content_storage),
                humanize_bytes(self.pinned_content_storage.total_size()),
                self.local_advertisement_db.count(),
                num_remote_ads,
                max_remote_ads,
                remote_ad_capacity,
                radius_display,
            )

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
                enr = await self.lookup_enr(
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
        while self.manager.is_running:
            with trio.move_on_after(10):
                async with trio.open_nursery() as nursery:
                    for enr in self._bootnodes:
                        if enr.node_id == self.local_node_id:
                            continue
                        endpoint = Endpoint.from_enr(enr)
                        nursery.start_soon(self._bond, enr.node_id, endpoint)

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
