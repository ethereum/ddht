import logging
from typing import Dict

from async_service import Service
from eth_typing import NodeID
from lru import LRU
import trio

from ddht.v5_1.alexandria.abc import AlexandriaNetworkAPI, RadiusTrackerAPI
from ddht.v5_1.alexandria.messages import AckMessage, PingMessage, PongMessage


class RadiusTracker(Service, RadiusTrackerAPI):
    logger = logging.getLogger("ddht.RadiusTracker")

    def __init__(self, network: AlexandriaNetworkAPI) -> None:
        self._network = network
        self._node_ad_radius: Dict[NodeID, int] = LRU(8129)  # ~ 0.5 mb

        self._ack_ready = trio.Event()
        self._ping_ready = trio.Event()
        self._pong_ready = trio.Event()

    async def run(self) -> None:
        self.manager.run_daemon_task(self._track_radius_from_ping)
        self.manager.run_daemon_task(self._track_radius_from_pong)
        self.manager.run_daemon_task(self._track_radius_from_ack)

        await self.manager.wait_finished()

    async def ready(self) -> None:
        await self._ack_ready.wait()
        await self._ping_ready.wait()
        await self._pong_ready.wait()

    async def get_advertisement_radius(self, node_id: NodeID) -> int:
        if node_id == self._network.local_node_id:
            raise Exception("Cannot query local node id")
        elif node_id in self._node_ad_radius:
            return self._node_ad_radius[node_id]
        else:
            pong = await self._network.ping(node_id)
            return pong.advertisement_radius

    async def _track_radius_from_ping(self) -> None:
        async with self._network.client.subscribe(PingMessage) as subscription:
            self._ping_ready.set()

            async for request in subscription:
                radius = request.message.payload.advertisement_radius
                self._node_ad_radius[request.sender_node_id] = radius

    async def _track_radius_from_pong(self) -> None:
        async with self._network.client.subscribe(PongMessage) as subscription:
            self._pong_ready.set()

            async for response in subscription:
                radius = response.message.payload.advertisement_radius
                self._node_ad_radius[response.sender_node_id] = radius

    async def _track_radius_from_ack(self) -> None:
        async with self._network.client.subscribe(AckMessage) as subscription:
            self._ack_ready.set()

            async for response in subscription:
                radius = response.message.payload.advertisement_radius
                self._node_ad_radius[response.sender_node_id] = radius
