from async_service import Service

from eth_enr import OldSequenceNumber
from eth_typing import NodeID
from lru import LRU
import trio

from ddht.endpoint import Endpoint
from ddht.exceptions import EmptyFindNodesResponse
from ddht.v5_1.abc import PingHandlerAPI, ClientAPI


class PingHandler(Service, PingHandlerAPI):
    def __init__(self, client: ClientAPI) -> None:
        self._client = client
        self._last_pong_at = LRU(4096)

    async def _maybe_add_to_routing_table(
        self,
        node_id: NodeID,
        endpoint: Endpoint,
        enr_seq: int,
    ) -> None:
        try:
            enr = await self._network.lookup_enr(
                node_id,
                enr_seq=enr_seq,
                endpoint=endpoint,
            )
        except (trio.TooSlowError, EmptyFindNodesResponse):
            return

        try:
            self._network.enr_db.set_enr(enr)
        except OldSequenceNumber:
            pass

        self.routing_table.update(enr.node_id)

    async def _pong_when_pinged(self) -> None:
        async with trio.open_nursery() as nursery:
            async with self.dispatcher.subscribe(PingMessage) as subscription:
                async for request in subscription:
                    await self._network.client.send_pong(
                        request.sender_node_id,
                        request.sender_endpoint,
                        self._network.enr_manager.enr.sequence_number,
                        request.sender_endpoint.ip_address,
                        request.sender_endpoint.port,
                    )
                    nursery.start_soon(
                        self._maybe_add_to_routing_table,
                        request.sender_node_id,
                        request.sender_endpoint,
                        request.message.enr_seq,
                    )
