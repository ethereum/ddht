import logging
from typing import Any, List, Type

from async_service import Service
from eth_enr import ENRAPI, ENRDatabaseAPI, ENRManagerAPI

from ddht._utils import humanize_node_id
from ddht.abc import RoutingTableAPI
from ddht.v5_1.abc import FindNodesClientAPI


class FindNodesHandler(Service):
    logger = logging.getLogger("ddht.FindNodesHandler")

    def __init__(
        self,
        client: FindNodesClientAPI[Any],
        routing_table: RoutingTableAPI,
        enr_manager: ENRManagerAPI,
        enr_db: ENRDatabaseAPI,
        find_nodes_message_type: Type[Any],
    ) -> None:
        self.client = client
        self.routing_table = routing_table
        self.enr_manager = enr_manager
        self.enr_db = enr_db
        self.find_nodes_message_type = find_nodes_message_type

    async def run(self) -> None:
        async with self.client.subscribe(self.find_nodes_message_type) as subscription:
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
                    request.sender_node_id,
                    request.sender_endpoint,
                    enrs=response_enrs,
                    request_id=request.request_id,
                )
