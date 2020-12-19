import logging

from async_service import Service
from eth_utils import ValidationError
import trio

from ddht.v5_1.alexandria.abc import (
    AdvertisementCollectorAPI,
    AlexandriaNetworkAPI,
    ContentCollectorAPI,
    ContentManagerAPI,
    ContentStorageAPI,
)
from ddht.v5_1.alexandria.advertisements import Advertisement
from ddht.v5_1.alexandria.content import compute_content_distance


class ContentCollector(Service, ContentCollectorAPI):
    logger = logging.getLogger("ddht.ContentCollector")

    def __init__(
        self,
        network: AlexandriaNetworkAPI,
        content_manager: ContentManagerAPI,
        concurrency: int = 3,
    ) -> None:
        self._network = network
        self.content_manager = content_manager

        self._concurrency = concurrency

        self._ready = trio.Event()

    @property
    def content_storage(self) -> ContentStorageAPI:
        return self.content_manager.content_storage

    @property
    def _advertisement_collector(self) -> AdvertisementCollectorAPI:
        return self._network.advertisement_collector

    async def ready(self) -> None:
        await self._ready.wait()

    async def run(self) -> None:
        self.manager.run_daemon_task(self._monitor_new_advertisements)

        await self.manager.wait_finished()

    async def _worker(
        self, worker_id: int, receive_channel: trio.abc.ReceiveChannel[Advertisement]
    ) -> None:
        async for advertisement in receive_channel:
            if self.content_storage.has_content(advertisement.content_key):
                continue

            distance = compute_content_distance(
                self._network.local_node_id, advertisement.content_id,
            )
            if distance > self.content_manager.content_radius:
                continue

            with trio.move_on_after(30) as scope:
                await self._gather_advertisement_content(advertisement)

            if scope.cancelled_caught:
                self.logger.debug(
                    "Unable to retrieve advertised content: node_id=%s  content_key=%s",
                    advertisement.node_id.hex(),
                    advertisement.content_key.hex(),
                )

    async def _monitor_new_advertisements(self) -> None:
        send_channel, receive_channel = trio.open_memory_channel[Advertisement](
            self._concurrency
        )
        async with self._advertisement_collector.new_advertisement.subscribe() as subscription:
            self._ready.set()
            async with trio.open_nursery() as nursery:
                for worker_id in range(self._concurrency):
                    nursery.start_soon(self._worker, worker_id, receive_channel)

                async for advertisement in subscription:
                    await send_channel.send(advertisement)

    async def _gather_advertisement_content(self, advertisement: Advertisement) -> None:
        proof = await self._network.get_content(
            content_key=advertisement.content_key,
            hash_tree_root=advertisement.hash_tree_root,
        )

        content = proof.get_content()

        try:
            await self._network.content_validator.validate_content(
                content_key=advertisement.content_key, content=content,
            )
        except ValidationError as err:
            self.logger.debug(
                "Content validation failed: content_key=%s  content=%s  error=%s",
                advertisement.content_key.hex(),
                content.hex(),
                err,
            )
        else:
            await self.content_manager.process_content(
                advertisement.content_key, content
            )
            self.logger.debug(
                "Collected content: content_key=%s", advertisement.content_key.hex(),
            )
