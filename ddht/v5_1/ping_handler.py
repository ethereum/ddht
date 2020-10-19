from typing import Any, Type

from async_service import Service
from eth_enr import ENRManagerAPI

from ddht.abc import RoutingTableAPI
from ddht.v5_1.abc import PingPongClientAPI


class PingHandler(Service):
    def __init__(
        self,
        client: PingPongClientAPI[Any],
        enr_manager: ENRManagerAPI,
        routing_table: RoutingTableAPI,
        ping_message_type: Type[Any],
    ) -> None:
        self.client = client
        self.enr_manager = enr_manager
        self.routing_table = routing_table
        self.ping_message_type = ping_message_type

    async def run(self) -> None:
        async with self.client.subscribe(self.ping_message_type) as subscription:
            async for request in subscription:
                await self.client.send_pong(
                    node_id=request.sender_node_id,
                    endpoint=request.sender_endpoint,
                    enr_seq=self.enr_manager.enr.sequence_number,
                    request_id=request.request_id,
                )
