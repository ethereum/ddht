from typing import (
    AsyncContextManager,
    AsyncIterator,
    Collection,
    List,
    Optional,
    Tuple,
    Union,
)

from async_generator import asynccontextmanager
from async_service import Service, background_trio_service
from eth_enr import ENRAPI, ENRManagerAPI, QueryableENRDatabaseAPI
from eth_enr.exceptions import OldSequenceNumber
from eth_typing import NodeID
from eth_utils import ValidationError, get_extended_debug_logger
from eth_utils.toolz import cons, first
from lru import LRU
import trio

from ddht._utils import every, weighted_choice
from ddht.constants import ROUTING_TABLE_BUCKET_SIZE
from ddht.endpoint import Endpoint
from ddht.kademlia import KademliaRoutingTable, at_log_distance
from ddht.token_bucket import TokenBucket
from ddht.v5_1.abc import NetworkAPI
from ddht.v5_1.alexandria.abc import AlexandriaNetworkAPI, ContentStorageAPI
from ddht.v5_1.alexandria.client import AlexandriaClient
from ddht.v5_1.alexandria.constants import MAX_RADIUS
from ddht.v5_1.alexandria.content import (
    compute_content_distance,
    content_key_to_content_id,
)
from ddht.v5_1.alexandria.messages import (
    FindContentMessage,
    FindNodesMessage,
    PingMessage,
    PongMessage,
)
from ddht.v5_1.alexandria.payloads import FoundContentPayload, PongPayload
from ddht.v5_1.alexandria.seeker import Seeker
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
        storage: ContentStorageAPI,
    ) -> None:
        self.logger = get_extended_debug_logger("ddht.Alexandria")

        self._bootnodes = tuple(bootnodes)

        self.client = AlexandriaClient(network)

        self.routing_table = KademliaRoutingTable(
            self.enr_manager.enr.node_id, ROUTING_TABLE_BUCKET_SIZE,
        )

        self._last_pong_at = LRU(2048)
        self._routing_table_ready = trio.Event()

        self._ping_handler_ready = trio.Event()
        self._find_nodes_handler_ready = trio.Event()
        self._find_content_handler_ready = trio.Event()

        self.storage = storage

    async def routing_table_ready(self) -> None:
        await self._routing_table_ready.wait()

    async def ready(self) -> None:
        await self._ping_handler_ready.wait()
        await self._find_nodes_handler_ready.wait()
        await self._find_content_handler_ready.wait()

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
        self.manager.run_daemon_task(self._periodically_report_routing_table)
        self.manager.run_daemon_task(self._ping_oldest_routing_table_entry)
        self.manager.run_daemon_task(self._track_last_pong)
        self.manager.run_daemon_task(self._manage_routing_table)
        self.manager.run_daemon_task(self._pong_when_pinged)
        self.manager.run_daemon_task(self._serve_find_nodes)
        self.manager.run_daemon_task(self._serve_find_content)

        # Child services
        self.manager.run_daemon_child_service(self.client)
        # self.manager.run_daemon_child_service(self.radius_tracker)

        await self.manager.wait_finished()

    #
    # Local properties
    #
    @property
    def local_advertisement_radius(self) -> int:
        return MAX_RADIUS

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
        with trio.move_on_after(300) as scope:
            async with background_trio_service(explorer):
                await explorer.ready()

                async with explorer.stream() as receive_channel:
                    yield receive_channel

        if scope.cancelled_caught:
            self.logger.error("Timeout from explore")

    async def find_content(
        self,
        node_id: NodeID,
        *,
        content_key: ContentKey,
        endpoint: Optional[Endpoint] = None,
        request_id: Optional[bytes] = None,
    ) -> FoundContentPayload:
        if endpoint is None:
            endpoint = await self.network.endpoint_for_node_id(node_id)

        response = await self.client.find_content(
            node_id, endpoint, content_key=content_key, request_id=request_id,
        )
        if response.payload.is_content and response.payload.encoded_enrs:
            raise ValidationError("Content response with non-empty ENR payload")
        elif response.payload.is_content and not response.payload.content:
            raise ValidationError("Content response with empty content")

        return response.payload

    @asynccontextmanager
    async def recursive_find_content(
        self, content_key: ContentKey,
    ) -> AsyncIterator[trio.abc.ReceiveChannel[bytes]]:
        seeker = Seeker(self, content_key)

        async with background_trio_service(seeker):
            yield seeker.content_receive

    async def retrieve_content(self, content_key: ContentKey) -> bytes:
        async with self.recursive_find_content(content_key) as content_aiter:
            return await content_aiter.receive()

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
                enr = await self.lookup_enr(
                    request.sender_node_id,
                    enr_seq=request.message.payload.enr_seq,
                    endpoint=request.sender_endpoint,
                )
                self.routing_table.update(enr.node_id)
                self._routing_table_ready.set()

    def _source_nodes(self, distances: Tuple[int, ...]) -> Tuple[ENRAPI, ...]:
        response_enrs: List[ENRAPI] = []
        unique_distances = set(distances)
        if len(unique_distances) != len(distances):
            raise ValidationError("duplicate distances")
        elif not distances:
            raise ValidationError("empty distances")
        elif any(distance > self.routing_table.num_buckets for distance in distances):
            raise ValidationError("invalid distances")

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

        return tuple(response_enrs)

    async def _serve_find_nodes(self) -> None:
        async with self.client.subscribe(FindNodesMessage) as subscription:
            self._find_nodes_handler_ready.set()

            async for request in subscription:
                try:
                    response_enrs = self._source_nodes(
                        request.message.payload.distances
                    )
                except ValidationError as err:
                    self.logger.debug(
                        "Ignoring invalid FindNodesMessage from %s@%s: %s",
                        request.sender_node_id.hex(),
                        request.sender_endpoint,
                        err,
                    )
                else:
                    await self.client.send_found_nodes(
                        request.sender_node_id,
                        request.sender_endpoint,
                        enrs=response_enrs,
                        request_id=request.request_id,
                    )

    async def _serve_find_content(self) -> None:
        async with self.client.subscribe(FindContentMessage) as subscription:
            self._find_content_handler_ready.set()

            async for request in subscription:
                # if content in storage, serve it....
                # else serve ENR records that we know of which are *closest*
                content_key = request.message.payload.content_key

                if self.storage.has_content(content_key):
                    content = self.storage.get_content(content_key)

                    await self.client.send_found_content(
                        request.sender_node_id,
                        request.sender_endpoint,
                        enrs=None,
                        content=content,
                        request_id=request.request_id,
                    )
                else:
                    content_id = content_key_to_content_id(content_key)
                    distance = compute_content_distance(self.local_node_id, content_id)
                    try:
                        response_enrs = self._source_nodes((distance,))
                    except ValidationError as err:
                        self.logger.debug(
                            "Ignoring invalid FindNodesMessage from %s@%s: %s",
                            request.sender_node_id.hex(),
                            request.sender_endpoint,
                            err,
                        )
                    else:
                        await self.client.send_found_content(
                            request.sender_node_id,
                            request.sender_endpoint,
                            enrs=response_enrs,
                            content=None,
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
