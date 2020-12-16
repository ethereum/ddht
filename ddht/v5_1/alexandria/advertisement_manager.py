import itertools
import logging
from typing import Optional

from async_service import Service
from eth_utils.toolz import first, take
import trio

from ddht.v5_1.alexandria.abc import (
    AdvertisementDatabaseAPI,
    AdvertisementManagerAPI,
    AlexandriaNetworkAPI,
)
from ddht.v5_1.alexandria.constants import MAX_RADIUS
from ddht.v5_1.alexandria.content import compute_content_distance


class AdvertisementManager(Service, AdvertisementManagerAPI):
    logger = logging.getLogger("ddht.AdvertisementManager")

    def __init__(
        self,
        network: AlexandriaNetworkAPI,
        advertisement_db: AdvertisementDatabaseAPI,
        max_advertisement_count: Optional[int] = None,
    ) -> None:
        self._network = network
        self.advertisement_db = advertisement_db
        self._ready = trio.Event()

        self.max_advertisement_count = max_advertisement_count

    @property
    def advertisement_radius(self) -> int:
        if self.max_advertisement_count is None:
            return MAX_RADIUS

        advertisement_count = self.advertisement_db.count()

        if advertisement_count < self.max_advertisement_count:
            return MAX_RADIUS

        try:
            furthest_advertisement = first(
                self.advertisement_db.furthest(self._network.local_node_id)
            )
        except StopIteration:
            return MAX_RADIUS
        else:
            return compute_content_distance(
                self._network.local_node_id, furthest_advertisement.content_id,
            )

    async def ready(self) -> None:
        await self._ready.wait()

    async def purge_expired_ads(self) -> None:
        while self.manager.is_running:
            await trio.lowlevel.checkpoint()

            # Purge in chunks of 64 to avoid blocking too long
            to_purge = tuple(take(64, self.advertisement_db.expired()))
            if not to_purge:
                break

            self.logger.debug("Purging expired ads: count=%d", len(to_purge))

            for advertisement in to_purge:
                self.advertisement_db.remove(advertisement)

    async def purge_distant_ads(self) -> None:
        if self.max_advertisement_count is None:
            raise Exception("Invalid")

        while self.manager.is_running:
            # Ensure that the inner loop here doesn't block the event loop
            await trio.lowlevel.checkpoint()

            # Check if we are over the limit
            total_advertisements = self.advertisement_db.count()
            if total_advertisements <= self.max_advertisement_count:
                break

            num_to_purge = total_advertisements - self.max_advertisement_count

            # Purge in chunks of 64 to avoid blocking too long
            to_purge = tuple(
                take(
                    min(64, num_to_purge),
                    itertools.filterfalse(
                        lambda ad: ad.node_id == self._network.local_node_id,
                        self.advertisement_db.furthest(self._network.local_node_id),
                    ),
                )
            )

            self.logger.debug("Purging ads outside radius: count=%d", len(to_purge))

            for advertisement in to_purge:
                self.advertisement_db.remove(advertisement)

    async def run(self) -> None:
        if self.max_advertisement_count is not None:
            self.manager.run_daemon_task(self._enforce_max_advertisement_limit)
        self.manager.run_daemon_task(self._periodically_purge_expired)

        self._ready.set()

        await self.manager.wait_finished()

    async def _enforce_max_advertisement_limit(self) -> None:
        if self.max_advertisement_count is None:
            raise Exception("Invalid")

        while self.manager.is_running:
            await self.purge_distant_ads()

            await trio.sleep(30)

    async def _periodically_purge_expired(self) -> None:
        while self.manager.is_running:
            await self.purge_expired_ads()

            await trio.sleep(30)
