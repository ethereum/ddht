import argparse
import logging

from async_service import background_trio_service, run_trio_service
import trio

from ddht.abc import ApplicationAPI
from ddht.boot_info import BootInfo
from ddht.constants import ProtocolVersion
from ddht.v5.app import Application as ApplicationV5
from ddht.v5.crawl import Crawler as CrawlerV5
from ddht.v5_1.alexandria.app import AlexandriaApplication
from ddht.v5_1.app import Application as ApplicationV5_1
from ddht.v5_1.crawler import Crawler as CrawlerV51

logger = logging.getLogger("ddht")


async def do_main(args: argparse.Namespace, boot_info: BootInfo) -> None:
    app: ApplicationAPI

    if boot_info.protocol_version is ProtocolVersion.v5:
        app = ApplicationV5(args, boot_info)
    elif boot_info.protocol_version is ProtocolVersion.v5_1:
        app = ApplicationV5_1(args, boot_info)
    else:
        raise Exception(f"Unsupported protocol version: {boot_info.protocol_version}")

    async with background_trio_service(app) as manager:
        await manager.wait_finished()


async def do_crawl(args: argparse.Namespace, boot_info: BootInfo) -> None:
    if boot_info.protocol_version is ProtocolVersion.v5:
        crawler_v5 = CrawlerV5(args, boot_info)

        await run_trio_service(crawler_v5)
    elif boot_info.protocol_version is ProtocolVersion.v5_1:
        app = ApplicationV5_1(args, boot_info)
        async with background_trio_service(app):
            await app.wait_ready()
            await app.network.events.session_handshake_complete.wait()
            await trio.sleep(1)
            crawler_v51 = CrawlerV51(
                network=app.network, concurrency=args.crawl_concurrency,  # type: ignore
            )
            await run_trio_service(crawler_v51)
    else:
        raise ValueError(f"Unsupported protocol version: {boot_info.protocol_version}")


async def do_alexandria(args: argparse.Namespace, boot_info: BootInfo) -> None:
    app = AlexandriaApplication(args, boot_info)

    async with background_trio_service(app) as manager:
        await manager.wait_finished()
