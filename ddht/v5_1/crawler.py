import logging
from typing import Iterator

from async_service import Service
from eth_typing import NodeID

from ddht._utils import every
from ddht.kademlia import at_log_distance
from ddht.v5_1.abc import NetworkProtocol
from ddht.v5_1.explorer import Explorer


class CrawlerExplorer(Explorer):
    def _get_ordered_candidates(self) -> Iterator[NodeID]:
        return iter(self.seen)


class Crawler(Service):
    logger = logging.getLogger("ddht.Crawler")

    def __init__(self, network: NetworkProtocol, concurrency: int = 32) -> None:
        target = at_log_distance(network.local_node_id, 256)
        self._explorer = CrawlerExplorer(network, target, concurrency)

    async def run(self) -> None:
        self.logger.info("crawl-starting")

        self.manager.run_child_service(self._explorer)
        await self._explorer.ready()

        self.manager.run_daemon_task(self._periodically_report_crawl_stats)

        async with self._explorer.stream() as enrs_aiter:
            async for enr in enrs_aiter:
                pass

        stats = self._explorer.get_stats()
        citizen_count = (
            stats.seen
            - stats.unresponsive
            - stats.invalid
            - stats.unreachable
            - stats.pending
        )
        self.logger.info("crawl-finished:  citizens=%d  %s", citizen_count, stats)
        self.manager.cancel()

    async def _periodically_report_crawl_stats(self) -> None:
        async for _ in every(5, 5):
            stats = self._explorer.get_stats()
            citizen_count = (
                stats.seen
                - stats.unresponsive
                - stats.invalid
                - stats.unreachable
                - stats.pending
            )
            self.logger.info("crawl-stats:  citizens=%d  %s", citizen_count, stats)
