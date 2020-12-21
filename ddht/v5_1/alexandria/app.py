import argparse
import pathlib
import sqlite3

from ddht._utils import humanize_bytes
from ddht.app import BaseApplication
from ddht.boot_info import BootInfo
from ddht.v5_1.alexandria.abc import AdvertisementDatabaseAPI, ContentStorageAPI
from ddht.v5_1.alexandria.advertisement_db import AdvertisementDatabase
from ddht.v5_1.alexandria.boot_info import AlexandriaBootInfo
from ddht.v5_1.alexandria.broadcast_log import BroadcastLog
from ddht.v5_1.alexandria.content_storage import (
    FileSystemContentStorage,
    MemoryContentStorage,
)
from ddht.v5_1.alexandria.network import AlexandriaNetwork
from ddht.v5_1.alexandria.rpc_handlers import get_alexandria_rpc_handlers
from ddht.v5_1.alexandria.xdg import get_xdg_alexandria_root
from ddht.v5_1.app import Application


class AlexandriaApplication(BaseApplication):
    base_protocol_app: Application

    def __init__(self, args: argparse.Namespace, boot_info: BootInfo) -> None:
        super().__init__(args, boot_info)
        self._alexandria_boot_info = AlexandriaBootInfo.from_namespace(self._args)
        self.base_protocol_app = Application(self._args, self._boot_info)

    async def run(self) -> None:
        self.manager.run_daemon_child_service(self.base_protocol_app)

        await self.base_protocol_app.wait_ready()

        xdg_alexandria_root = get_xdg_alexandria_root()
        xdg_alexandria_root.mkdir(parents=True, exist_ok=True)

        max_advertisement_count = self._alexandria_boot_info.max_advertisement_count

        commons_content_storage_max_size = (
            self._alexandria_boot_info.commons_storage_size
        )

        commons_content_storage: ContentStorageAPI
        commons_storage_display: str

        if self._alexandria_boot_info.commons_storage == ":memory:":
            commons_content_storage = MemoryContentStorage()

            commons_storage_display = "<memory>"
        elif self._alexandria_boot_info.commons_storage is None:
            commons_content_storage_path = xdg_alexandria_root / "content" / "commons"
            commons_content_storage_path.mkdir(parents=True, exist_ok=True)
            commons_content_storage = FileSystemContentStorage(
                commons_content_storage_path
            )

            commons_storage_display = str(commons_content_storage_path)
        elif isinstance(self._alexandria_boot_info.commons_storage, pathlib.Path):
            commons_content_storage = FileSystemContentStorage(
                self._alexandria_boot_info.commons_storage,
            )

            commons_storage_display = str(self._alexandria_boot_info.commons_storage)
        else:
            raise Exception(
                f"Unsupported value: "
                f"commons_storage={self._alexandria_boot_info.commons_storage}"
            )

        pinned_content_storage: ContentStorageAPI
        pinned_storage_display: str

        if self._alexandria_boot_info.pinned_storage == ":memory:":
            pinned_content_storage = MemoryContentStorage()

            pinned_storage_display = "<memory>"
        elif self._alexandria_boot_info.pinned_storage is None:
            pinned_content_storage_path = xdg_alexandria_root / "content" / "pinned"
            pinned_content_storage_path.mkdir(parents=True, exist_ok=True)
            pinned_content_storage = FileSystemContentStorage(
                pinned_content_storage_path
            )

            pinned_storage_display = str(pinned_content_storage_path)
        elif isinstance(self._alexandria_boot_info.pinned_storage, pathlib.Path):
            pinned_content_storage = FileSystemContentStorage(
                self._alexandria_boot_info.pinned_storage,
            )

            pinned_storage_display = str(self._alexandria_boot_info.pinned_storage)
        else:
            raise Exception(
                f"Unsupported value: "
                f"pinned_storage={self._alexandria_boot_info.pinned_storage}"
            )

        local_advertisement_db_path = (
            xdg_alexandria_root / "advertisements.local.sqlite3"
        )
        local_advertisement_db: AdvertisementDatabaseAPI = AdvertisementDatabase(
            sqlite3.connect(str(local_advertisement_db_path)),
        )

        remote_advertisement_db_path = (
            xdg_alexandria_root / "advertisements.remote.sqlite3"
        )
        remote_advertisement_db: AdvertisementDatabaseAPI = AdvertisementDatabase(
            sqlite3.connect(str(remote_advertisement_db_path)),
        )

        broadcast_log_db_path = xdg_alexandria_root / "broadcast_log.sqlite3"
        broadcast_log = BroadcastLog(sqlite3.connect(str(broadcast_log_db_path)))

        alexandria_network = AlexandriaNetwork(
            network=self.base_protocol_app.network,
            bootnodes=self._alexandria_boot_info.bootnodes,
            commons_content_storage=commons_content_storage,
            pinned_content_storage=pinned_content_storage,
            local_advertisement_db=local_advertisement_db,
            remote_advertisement_db=remote_advertisement_db,
            broadcast_log=broadcast_log,
            commons_content_storage_max_size=commons_content_storage_max_size,
            max_advertisement_count=max_advertisement_count,
        )

        self.manager.run_daemon_child_service(alexandria_network)

        self.logger.info("Starting Alexandria...")
        self.logger.info("Root Directory         : %s", xdg_alexandria_root)
        self.logger.info(
            "ContentStorage[Commons]: storage=%s  items=%d  size=%s  max_size=%s",
            commons_storage_display,
            len(commons_content_storage),
            humanize_bytes(commons_content_storage.total_size()),
            humanize_bytes(commons_content_storage_max_size),
        )
        self.logger.info(
            "ContentStorage[Pinned] : storage=%s  items=%d  size=%s",
            pinned_storage_display,
            len(pinned_content_storage),
            humanize_bytes(pinned_content_storage.total_size()),
        )
        self.logger.info(
            "AdvertisementDB[local] : storage=%s  total=%d",
            local_advertisement_db_path,
            local_advertisement_db.count(),
        )
        self.logger.info(
            "AdvertisementDB[remote]: storage=%s  total=%d  max=%d",
            remote_advertisement_db_path,
            remote_advertisement_db.count(),
            max_advertisement_count,
        )
        self.logger.info(
            "BroadcastLog: storage=%s  total=%d  max=%d",
            broadcast_log_db_path,
            broadcast_log.count,
            broadcast_log.cache_size,
        )

        await alexandria_network.ready()

        if self._boot_info.is_rpc_enabled:
            self.base_protocol_app.rpc_server.add_handers(
                get_alexandria_rpc_handlers(alexandria_network)
            )

        await self.manager.wait_finished()
