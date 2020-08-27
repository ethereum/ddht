import logging
from typing import List

from async_service import Service

from ddht._utils import humanize_node_id
from ddht.abc import ENRManagerAPI, NodeDBAPI
from ddht.constants import NUM_ROUTING_TABLE_BUCKETS
from ddht.enr import ENR
from ddht.kademlia import KademliaRoutingTable
from ddht.v5_1.abc import ClientAPI, DispatcherAPI, EventsAPI, NetworkAPI, PoolAPI
from ddht.v5_1.messages import FindNodeMessage, PingMessage, PongMessage


class Network(Service, NetworkAPI):
    logger = logging.getLogger("ddht.Network")

    def __init__(self, client: ClientAPI) -> None:
        self.client = client
        self.routing_table = KademliaRoutingTable(
            self.client.enr_manager.enr.node_id, NUM_ROUTING_TABLE_BUCKETS,
        )

    #
    # Proxied ClientAPI properties
    #
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
    def node_db(self) -> NodeDBAPI:
        return self.client.node_db

    async def run(self) -> None:
        self.manager.run_daemon_child_service(self.client)
        await self.client.wait_listening()

        self.manager.run_daemon_task(self._pong_when_pinged)
        self.manager.run_daemon_task(self._serve_find_nodes)

        await self.manager.wait_finished()

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
                response_enrs: List[ENR] = []
                distances = set(request.message.distances)
                if len(distances) != len(request.message.distances):
                    self.logger.debug(
                        "Ignoring invalid FindNodeMessage from %s@%s: duplicate distances",
                        humanize_node_id(request.sender_node_id),
                        request.sender_endpoint,
                    )
                    return
                elif not distances:
                    self.logger.debug(
                        "Ignoring invalid FindNodeMessage from %s@%s: empty distances",
                        humanize_node_id(request.sender_node_id),
                        request.sender_endpoint,
                    )
                    return

                for distance in distances:
                    if distance == 0:
                        response_enrs.append(self.enr_manager.enr)
                    elif distance < self.routing_table.num_buckets:
                        node_ids_at_distance = self.routing_table.get_nodes_at_log_distance(
                            distance,
                        )
                        for node_id in node_ids_at_distance:
                            response_enrs.append(self.node_db.get_enr(node_id))
                    else:
                        self.logger.debug(
                            "Ignoring invalid FindNodeMessage from %s@%s: invalid distance: %d",
                            humanize_node_id(request.sender_node_id),
                            request.sender_endpoint,
                            distance,
                        )
                        return

                await self.client.send_found_nodes(
                    request.sender_endpoint,
                    request.sender_node_id,
                    enrs=response_enrs,
                    request_id=request.message.request_id,
                )
