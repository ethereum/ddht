from async_service import Service

from eth_enr import OldSequenceNumber
from eth_utils import ValidationError
from lru import LRU
import trio

from ddht.exceptions import EmptyFindNodesResponse
from ddht.v5_1.abc import PingHandlerAPI, CommonPingPayload, NetworkProtocol


class BasePingHandler(Service, PingHandlerAPI):
    def __init__(self,
                 network: NetworkProtocol) -> None:
        self._network = network
        self._last_pong_at = LRU(4096)

    async def _maybe_add_to_routing_table(self, payload: CommonPingPayload) -> None:
        try:
            enr = await self._network.lookup_enr(
                payload.sender_node_id,
                enr_seq=payload.enr_seq,
                endpoint=payload.sender_endpoint,
            )
        except (trio.TooSlowError, EmptyFindNodesResponse, ValidationError):
            return

        try:
            self._network.enr_db.set_enr(enr)
        except OldSequenceNumber:
            pass

        self._network.routing_table.update(enr.node_id)

    async def _pong_when_pinged(self) -> None:
        async with trio.open_nursery() as nursery:
            async with self.subscribe_ping() as subscription:
                async for payload in subscription:
                    await self.send_pong(payload)
                    nursery.start_soon(self._maybe_add_to_routing_table, payload)
