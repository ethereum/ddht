from typing import AsyncContextManager, AsyncIterator, Optional

from async_generator import asynccontextmanager
from async_service import background_trio_service

from ddht._utils import asyncnullcontext
from ddht.tools.driver._utils import NamedLock
from ddht.tools.driver.abc import AlexandriaNodeAPI, NodeAPI
from ddht.v5_1.abc import NetworkAPI
from ddht.v5_1.alexandria.abc import AlexandriaClientAPI, AlexandriaNetworkAPI
from ddht.v5_1.alexandria.client import AlexandriaClient
from ddht.v5_1.alexandria.network import AlexandriaNetwork


class AlexandriaNode(AlexandriaNodeAPI):
    _lock: NamedLock

    def __init__(self, node: NodeAPI) -> None:
        self.node = node
        self._lock = NamedLock()

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
                network.add_talk_protocol(alexandria_client)
                async with background_trio_service(alexandria_client):
                    yield alexandria_client

    @asynccontextmanager
    async def network(
        self, network: Optional[NetworkAPI] = None,
    ) -> AsyncIterator[AlexandriaNetworkAPI]:
        network_context: AsyncContextManager[NetworkAPI]

        if network is None:
            network_context = self.node.network()
        else:
            # unclear why the typing isn't work for `asyncnullcontext`
            network_context = asyncnullcontext(network)  # type: ignore

        async with self._lock.acquire("AlexandriaNode.network(...)"):
            async with network_context as network:
                alexandria_network = AlexandriaNetwork(network)
                network.add_talk_protocol(alexandria_network)
                async with background_trio_service(alexandria_network):
                    yield alexandria_network
