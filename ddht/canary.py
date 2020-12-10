import logging

from async_service import Service
import trio

WAKEUP_INTERVAL = 2
DELAY_DEBUG = 0.1
DELAY_WARNING = 1


class Canary(Service):
    logger = logging.getLogger("ddht.Canary")

    async def run(self) -> None:
        while True:
            start_at = trio.current_time()
            await trio.sleep(WAKEUP_INTERVAL)
            elapsed = trio.current_time() - start_at
            delay = elapsed - WAKEUP_INTERVAL
            self.logger.debug("Loop monitoring task called; delay=%.3fs", delay)

            if delay < DELAY_DEBUG:
                continue

            if delay >= DELAY_WARNING:
                log_fn = self.logger.warning
            else:
                log_fn = self.logger.debug

            stats = trio.lowlevel.current_statistics()
            log_fn(
                "Event loop blocked or overloaded: delay=%.3fs, tasks=%d, stats=%s",
                delay,
                stats.tasks_living,
                stats.io_statistics,
            )
