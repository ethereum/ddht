import logging
from typing import Optional

from async_service import Service
from async_service import external_trio_api as external_api
from eth_typing import Hash32
from eth_utils import humanize_seconds
from eth_utils.toolz import first, take
import ssz
import trio

from ddht._utils import every
from ddht.v5_1.alexandria.abc import (
    AdvertisementDatabaseAPI,
    AdvertisementManagerAPI,
    AlexandriaNetworkAPI,
    ContentManagerAPI,
    ContentStorageAPI,
)
from ddht.v5_1.alexandria.advertisements import Advertisement
from ddht.v5_1.alexandria.content import content_key_to_content_id
from ddht.v5_1.alexandria.sedes import content_sedes
from ddht.v5_1.alexandria.typing import ContentKey


class ContentManager(Service, ContentManagerAPI):
    logger = logging.getLogger("ddht.ContentManager")

    def __init__(
        self, network: AlexandriaNetworkAPI, content_storage: ContentStorageAPI
    ) -> None:
        self._network = network
        self.content_storage = content_storage

    @property
    def _advertisement_manager(self) -> AdvertisementManagerAPI:
        return self._network.advertisement_manager

    @property
    def _advertisement_db(self) -> AdvertisementDatabaseAPI:
        return self._network.advertisement_db

    async def run(self) -> None:
        self.manager.run_daemon_task(self._periodically_advertise_content)

        await self.manager.wait_finished()

    async def _periodically_advertise_content(self) -> None:
        await self._network.routing_table_ready()

        async for _ in every(30 * 60):
            start_at = trio.current_time()

            total_keys = len(self.content_storage)
            if not total_keys:
                continue

            processed_keys = 0

            last_key: Optional[ContentKey] = None

            while self.manager.is_running:
                elapsed = trio.current_time() - start_at
                content_keys = tuple(
                    take(4, self.content_storage.enumerate_keys(start_key=last_key))
                )

                # TODO: We need to adjust the
                # `ContentStorageAPI.enumerate_keys` to allow a
                # non-inclusive left bound so we can query all the keys
                # **after** the last key we processed.
                if content_keys and content_keys[0] == last_key:
                    content_keys = content_keys[1:]

                if not content_keys:
                    break

                for content_key in content_keys:
                    content = self.content_storage.get_content(content_key)

                    # TODO: computationally expensive
                    hash_tree_root = ssz.get_hash_tree_root(
                        content, sedes=content_sedes
                    )
                    advertisement = self._get_or_create_advertisement(
                        content_key=content_key, hash_tree_root=hash_tree_root,
                    )
                    await self._network.broadcast(advertisement)

                last_key = content_keys[-1]
                processed_keys += len(content_keys)
                progress = processed_keys / total_keys

                self.logger.debug(
                    "processing-local-content: progress=%0.1f  processed=%d  "
                    "total=%d  at=%s  elapsed=%s",
                    progress,
                    processed_keys,
                    total_keys,
                    "None" if last_key is None else last_key.hex(),
                    humanize_seconds(int(elapsed)),
                )

            self.logger.info(
                "processing-local-content-final: processed=%d/%d  elapsed=%s",
                processed_keys,
                total_keys,
                humanize_seconds(int(elapsed)),
            )

    def _get_or_create_advertisement(
        self, content_key: ContentKey, hash_tree_root: Hash32
    ) -> Advertisement:
        try:
            advertisement = first(
                self._advertisement_db.query(
                    content_key=content_key,
                    node_id=self._network.local_node_id,
                    hash_tree_root=hash_tree_root,
                )
            )
        except StopIteration:
            advertisement = Advertisement.create(
                content_key=content_key,
                hash_tree_root=hash_tree_root,
                private_key=self._network.client.local_private_key,
            )
            self._advertisement_db.add(advertisement)

        return advertisement  # type: ignore

    @external_api
    async def process_content(self, content_key: ContentKey, content: bytes) -> None:
        if self.content_storage.has_content(content_key):
            local_content = self.content_storage.get_content(content_key)
            if local_content == content:
                self.logger.debug(
                    "Ignoring content we already have: content_key=%s",
                    content_key.hex(),
                )
                return

        self.logger.debug(
            "Processing content: content_key=%s  content=%s",
            content_key.hex(),
            content.hex(),
        )
        content_id = content_key_to_content_id(content_key)

        # TODO: computationally expensive
        hash_tree_root = ssz.get_hash_tree_root(content, sedes=content_sedes)

        known_hash_tree_roots = self._advertisement_db.get_hash_tree_roots_for_content_id(
            content_id,
        )

        # We should avoid "polution" of our content database with mismatching
        # roots.  This is a stop gap right now because we will need a mechanism
        # for inserting our own "correct" content into the system even in the
        # case where the existing "network" content doesn't agree on the hash
        # tree root.
        if known_hash_tree_roots and hash_tree_root not in known_hash_tree_roots:
            known_roots_display = "|".join(
                (root.hex() for root in known_hash_tree_roots)
            )
            raise NotImplementedError(
                f"Content hash tree root mismatch: root={hash_tree_root.hex()}  "
                f"known={known_roots_display}"
            )

        self.content_storage.set_content(content_key, content, exists_ok=True)

        advertisement = self._get_or_create_advertisement(content_key, hash_tree_root)
        await self._network.broadcast(advertisement)

        self.logger.info(
            "Processed content: content_key=%s  content=%s",
            content_key.hex(),
            content.hex(),
        )
