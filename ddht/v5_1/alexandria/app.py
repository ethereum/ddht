import argparse

from ddht.app import BaseApplication
from ddht.boot_info import BootInfo
from ddht.v5_1.alexandria.abc import ContentStorageAPI
from ddht.v5_1.alexandria.boot_info import AlexandriaBootInfo
from ddht.v5_1.alexandria.content_storage import MemoryContentStorage
from ddht.v5_1.alexandria.network import AlexandriaNetwork
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

        content_storage: ContentStorageAPI = MemoryContentStorage()

        alexandria_network = AlexandriaNetwork(
            network=self.base_protocol_app.network,
            bootnodes=self._alexandria_boot_info.bootnodes,
            content_storage=content_storage,
        )

        self.manager.run_daemon_child_service(alexandria_network)

        self.logger.info("Starting Alexandria...")

        await self.manager.wait_finished()
