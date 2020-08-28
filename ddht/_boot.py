from typing import TYPE_CHECKING, AsyncIterator

from async_service import Service, TrioManager
import trio

if TYPE_CHECKING:
    import signal  # noqa: F401


async def _main() -> None:
    from ddht.main import main

    await main()


class BootService(Service):
    async def run(self) -> None:
        import signal  # noqa: F811

        with trio.open_signal_receiver(signal.SIGTERM, signal.SIGINT) as signal_aiter:
            ready = trio.Event()
            self.manager.run_daemon_task(self._monitor_signals, ready, signal_aiter)
            # this is needed to give the async iterable time to be entered.
            await ready.wait()

            # imports are triggered at this stage.
            await _main()

            import logging

            logger = logging.getLogger("ddht")
            logger.info("Stopping: Application Exited")
            self.manager.cancel()

    async def _monitor_signals(
        self, ready: trio.Event, signal_aiter: AsyncIterator["signal.Signals"]
    ) -> None:
        import logging
        import signal  # noqa: F811

        ready.set()
        async for sig in signal_aiter:
            logger = logging.getLogger()

            if sig == signal.SIGTERM:
                logger.info("Stopping: SIGTERM")
            elif sig == signal.SIGINT:
                logger.info("Stopping: CTRL+C")
            else:
                logger.error("Stopping: unexpected signal: %s:%s", sig.value, sig.name)

            self.manager.cancel()


def _boot() -> None:
    try:
        manager = TrioManager(BootService())

        trio.run(manager.run)
    except KeyboardInterrupt:
        import logging
        import sys

        logger = logging.getLogger()
        logger.info("Stopping: Fast CTRL+C")
        sys.exit(2)
    else:
        import sys

        sys.exit(0)


if __name__ == "__main__":
    _boot()
