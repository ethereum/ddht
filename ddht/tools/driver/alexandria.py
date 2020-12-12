from contextlib import AsyncExitStack
import sqlite3
from typing import AsyncContextManager, AsyncIterator, Collection, Optional, Tuple

from async_generator import asynccontextmanager
from async_service import background_trio_service
from eth_enr import ENRAPI

from ddht._utils import asyncnullcontext
from ddht.tools.driver._utils import NamedLock
from ddht.tools.driver.abc import (
    AlexandriaNodeAPI,
    AlexandriaTesterAPI,
    NodeAPI,
    TesterAPI,
)
from ddht.v5_1.abc import NetworkAPI
from ddht.v5_1.alexandria.abc import AlexandriaClientAPI, AlexandriaNetworkAPI
from ddht.v5_1.alexandria.advertisement_db import AdvertisementDatabase
from ddht.v5_1.alexandria.client import AlexandriaClient
from ddht.v5_1.alexandria.content_storage import MemoryContentStorage
from ddht.v5_1.alexandria.network import AlexandriaNetwork


class AlexandriaNode(AlexandriaNodeAPI):
    _lock: NamedLock

    def __init__(self, node: NodeAPI) -> None:
        self.node = node
        self.commons_content_storage = MemoryContentStorage()
        self.pinned_content_storage = MemoryContentStorage()
        self.advertisement_db = AdvertisementDatabase(sqlite3.connect(":memory:"),)
        self._lock = NamedLock()

    @property
    def enr(self) -> ENRAPI:
        return self.node.enr

    @asynccontextmanager
    async def client(
        self, network: Optional[NetworkAPI] = None,
    ) -> AsyncIterator[AlexandriaClientAPI]:
        network_context: AsyncContextManager[NetworkAPI]

        if network is None:
            network_context = self.node.network()
        else:
            # unclear why the typing isn't work for `asyncnullcontext`
            network_context = asyncnullcontext(network)  # type: ignore

        async with self._lock.acquire("AlexandriaNode.client(...)"):
            async with network_context as network:
                alexandria_client = AlexandriaClient(network)
                async with background_trio_service(alexandria_client):
                    yield alexandria_client

    @asynccontextmanager
    async def network(
        self,
        network: Optional[NetworkAPI] = None,
        bootnodes: Collection[ENRAPI] = (),
        max_advertisement_count: int = 32,
    ) -> AsyncIterator[AlexandriaNetworkAPI]:
        network_context: AsyncContextManager[NetworkAPI]

        if network is None:
            network_context = self.node.network()
        else:
            # unclear why the typing isn't work for `asyncnullcontext`
            network_context = asyncnullcontext(network)  # type: ignore

        async with self._lock.acquire("AlexandriaNode.network(...)"):
            async with network_context as network:
                alexandria_network = AlexandriaNetwork(
                    network=network,
                    bootnodes=bootnodes,
                    commons_content_storage=self.commons_content_storage,
                    pinned_content_storage=self.pinned_content_storage,
                    advertisement_db=self.advertisement_db,
                    max_advertisement_count=max_advertisement_count,
                )
                async with background_trio_service(alexandria_network):
                    await alexandria_network.ready()
                    yield alexandria_network


class AlexandriaTester(AlexandriaTesterAPI):
    def __init__(self, tester: TesterAPI) -> None:
        self._tester = tester

    def node(self) -> AlexandriaNodeAPI:
        return self._tester.node().alexandria

    @asynccontextmanager
    async def network_group(
        self, num_networks: int, bootnodes: Collection[ENRAPI] = (),
    ) -> AsyncIterator[Tuple[AlexandriaNetworkAPI, ...]]:
        all_bootnodes = list(bootnodes)
        networks = []
        async with AsyncExitStack() as stack:
            for _ in range(num_networks):
                node = self.node()
                network = await stack.enter_async_context(
                    node.network(bootnodes=all_bootnodes)
                )
                all_bootnodes.append(node.enr)
                networks.append(network)

            yield tuple(networks)
