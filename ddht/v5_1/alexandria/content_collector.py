import logging

from async_service import Service
from eth_utils import ValidationError
import trio

from ddht.v5_1.alexandria.abc import (
    AdvertisementManagerAPI,
    AlexandriaNetworkAPI,
    ContentCollectorAPI,
    ContentManagerAPI,
    ContentStorageAPI,
)
from ddht.v5_1.alexandria.advertisements import Advertisement


class ContentCollector(Service, ContentCollectorAPI):
    logger = logging.getLogger("ddht.ContentCollector")

    def __init__(
        self, network: AlexandriaNetworkAPI, content_manager: ContentManagerAPI
    ) -> None:
        self._network = network
        self.content_manager = content_manager

        self._ready = trio.Event()

    @property
    def content_storage(self) -> ContentStorageAPI:
        return self.content_manager.content_storage

    @property
    def _advertisement_manager(self) -> AdvertisementManagerAPI:
        return self._network.advertisement_manager

    async def ready(self) -> None:
        await self._ready.wait()

    async def run(self) -> None:
        self.manager.run_daemon_task(self._monitor_new_advertisements)

        await self.manager.wait_finished()

    async def _monitor_new_advertisements(self) -> None:
        async with self._advertisement_manager.new_advertisement.subscribe() as subscription:
            self._ready.set()
            async with trio.open_nursery() as nursery:
                async for advertisement in subscription:
                    if self.content_storage.has_content(advertisement.content_key):
                        continue

                    nursery.start_soon(
                        self._gather_advertisement_content, advertisement
                    )

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
        except ValidationError:
            self.logger.info(
                f"Content validation failed: "
                f"content_key={advertisement.content_key.hex()}  "
                f"content={content.hex()}"
            )
        else:
            await self.content_manager.process_content(
                advertisement.content_key, content
            )
