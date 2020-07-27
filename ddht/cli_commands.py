import argparse
import logging
import os

from async_service import background_trio_service

from ddht.app import Application

logger = logging.getLogger("ddht")


async def do_main(args: argparse.Namespace) -> None:
    app = Application()

    logger.info("Started main process (pid=%d)", os.getpid())
    async with background_trio_service(app) as manager:
        await manager.wait_finished()
