import logging
from typing import Optional

from async_service import Service
from eth_enr import ENRDatabaseAPI, ENRManagerAPI
from eth_typing import NodeID

from ddht.endpoint import Endpoint
from ddht.v5_1.abc import NetworkAPI
from ddht.v5_1.alexandria.abc import AlexandriaNetworkAPI
from ddht.v5_1.alexandria.client import AlexandriaClient
from ddht.v5_1.alexandria.messages import PingMessage
from ddht.v5_1.alexandria.payloads import PongPayload


class AlexandriaNetwork(Service, AlexandriaNetworkAPI):
    logger = logging.getLogger("ddht.Alexandria")

    # Delegate to the AlexandriaClient for determining `protocol_id`
    protocol_id = AlexandriaClient.protocol_id

    def __init__(self, network: NetworkAPI) -> None:
        self.client = AlexandriaClient(network)

    @property
    def network(self) -> NetworkAPI:
        return self.client.network

    @property
    def enr_manager(self) -> ENRManagerAPI:
        return self.client.network.enr_manager

    @property
    def enr_db(self) -> ENRDatabaseAPI:
        return self.client.network.enr_db

    async def run(self) -> None:
        self.manager.run_daemon_child_service(self.client)
        self.manager.run_daemon_task(self._pong_when_pinged)

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
        return response.payload

    #
    # Long Running Processes
    #
    async def _pong_when_pinged(self) -> None:
        async with self.client.subscribe(PingMessage) as subscription:
            async for request in subscription:
                await self.client.send_pong(
                    request.sender_node_id,
                    request.sender_endpoint,
                    enr_seq=self.enr_manager.enr.sequence_number,
                    request_id=request.request_id,
                )

    #
    # Utility
    #
    def _endpoint_for_node_id(self, node_id: NodeID) -> Endpoint:
        enr = self.enr_db.get_enr(node_id)
        return Endpoint.from_enr(enr)
