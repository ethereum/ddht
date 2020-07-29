import logging
import os

from async_service import background_trio_service

from ddht.app import Application
from ddht.boot_info import BootInfo

logger = logging.getLogger("ddht")


async def do_main(boot_info: BootInfo) -> None:
    app = Application(boot_info)

    logger.info("Started main process (pid=%d)", os.getpid())
    async with background_trio_service(app) as manager:
        await manager.wait_finished()
