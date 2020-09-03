from typing import AsyncIterator, Collection, Optional

from async_generator import asynccontextmanager
from async_service import background_trio_service
from eth_enr import ENRAPI, ENRDatabaseAPI
from eth_enr.constants import IP_V4_ADDRESS_ENR_KEY, UDP_PORT_ENR_KEY
from eth_enr.tools.factories import ENRFactory
from eth_keys import keys
from eth_typing import NodeID
from eth_utils import humanize_hash

from ddht.endpoint import Endpoint
from ddht.tools.driver.abc import NodeAPI
from ddht.v5_1.abc import ClientAPI, EventsAPI, NetworkAPI
from ddht.v5_1.client import Client
from ddht.v5_1.events import Events
from ddht.v5_1.network import Network


class Node(NodeAPI):
    def __init__(
        self,
        private_key: keys.PrivateKey,
        endpoint: Endpoint,
        enr_db: ENRDatabaseAPI,
        events: Optional[EventsAPI] = None,
    ) -> None:
        self.private_key = private_key
        self.enr_db = enr_db
        self.enr = ENRFactory(
            private_key=private_key.to_bytes(),
            address__ip=endpoint.ip_address,
            address__udp_port=endpoint.port,
        )
        self.enr_db.set_enr(self.enr)
        if events is None:
            events = Events()
        self.events = events

    def __str__(self) -> str:
        return f"{humanize_hash(self.node_id)}@{self.endpoint}"  # type: ignore

    @property
    def endpoint(self) -> Endpoint:
        return Endpoint(self.enr[IP_V4_ADDRESS_ENR_KEY], self.enr[UDP_PORT_ENR_KEY],)

    @property
    def node_id(self) -> NodeID:
        return self.enr.node_id

    @asynccontextmanager
    async def client(self) -> AsyncIterator[ClientAPI]:
        client = Client(
            local_private_key=self.private_key,
            listen_on=self.endpoint,
            enr_db=self.enr_db,
            events=self.events,
        )
        async with background_trio_service(client):
            await client.wait_listening()
            yield client

    @asynccontextmanager
    async def network(
        self, bootnodes: Collection[ENRAPI] = ()
    ) -> AsyncIterator[NetworkAPI]:
        client = Client(
            local_private_key=self.private_key,
            listen_on=self.endpoint,
            enr_db=self.enr_db,
            events=self.events,
        )
        network = Network(client, bootnodes)
        async with background_trio_service(network):
            await client.wait_listening()
            yield network
