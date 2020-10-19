import itertools
import logging
from typing import Collection, List, Optional, Set, Tuple

from async_service import Service
from eth_enr import ENRAPI, ENRDatabaseAPI, ENRManagerAPI
from eth_enr.exceptions import OldSequenceNumber
from eth_typing import NodeID
from eth_utils.toolz import take
import trio

from ddht._utils import humanize_node_id, reduce_enrs
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
from ddht.v5_1.alexandria.messages import FindNodesMessage, PingMessage
from ddht.v5_1.alexandria.payloads import PongPayload
from ddht.v5_1.find_nodes_handler import FindNodesHandler
from ddht.v5_1.ping_handler import PingHandler


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

        # child-services
        self._ping_handler = PingHandler(
            client=self.client,
            enr_manager=self.enr_manager,
            routing_table=self.routing_table,
            ping_message_type=PingMessage,
        )

        self._find_nodes_handler = FindNodesHandler(
            client=self.client,
            routing_table=self.routing_table,
            enr_manager=self.enr_manager,
            enr_db=self.enr_db,
            find_nodes_message_type=FindNodesMessage,
        )

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
        self.manager.run_daemon_child_service(self._ping_handler)
        self.manager.run_daemon_child_service(self._find_nodes_handler)

        await self.manager.wait_finished()

    #
    # High Level API
    #
    async def ping(
        self, node_id: NodeID, *, endpoint: Optional[Endpoint] = None,
    ) -> PongPayload:
        if endpoint is None:
            endpoint = self._endpoint_for_node_id(node_id)
        response = await self.client.ping(node_id, endpoint=endpoint)
        return response.message.payload

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

    async def get_enr(
        self, node_id: NodeID, *, enr_seq: int = 0, endpoint: Optional[Endpoint] = None
    ) -> ENRAPI:
        try:
            enr = self.enr_db.get_enr(node_id)
        except KeyError:
            enr = await self._fetch_enr(node_id, endpoint=endpoint)
            self.enr_db.set_enr(enr)
        else:
            if enr_seq > enr.sequence_number:
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
            except trio.EndOfChannel:
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
                reduce_enrs(received_enrs),
                key=lambda enr: compute_distance(enr.node_id, target),
            )
        )

    #
    # Utility
    #
    def _endpoint_for_node_id(self, node_id: NodeID) -> Endpoint:
        enr = self.enr_db.get_enr(node_id)
        return Endpoint.from_enr(enr)
