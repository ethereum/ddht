import logging
import os

from async_service import ServiceAPI, background_trio_service

from ddht.boot_info import BootInfo
from ddht.constants import ProtocolVersion
from ddht.v5.app import Application as ApplicationV5
from ddht.v5_1.app import Application as ApplicationV5_1

logger = logging.getLogger("ddht")


async def do_main(boot_info: BootInfo) -> None:
    app: ServiceAPI
    if boot_info.protocol_version is ProtocolVersion.v5:
        app = ApplicationV5(boot_info)
    elif boot_info.protocol_version is ProtocolVersion.v5_1:
        app = ApplicationV5_1(boot_info)
    else:
        raise Exception(f"Unsupported protocol version: {boot_info.protocol_version}")

    logger.info("Started main process (pid=%d)", os.getpid())
    async with background_trio_service(app) as manager:
        await manager.wait_finished()
