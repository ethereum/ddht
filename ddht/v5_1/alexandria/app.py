import argparse

from ddht.app import BaseApplication
from ddht.boot_info import BootInfo
from ddht.v5_1.alexandria.boot_info import AlexandriaBootInfo
from ddht.v5_1.alexandria.network import AlexandriaNetwork
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

        alexandria_network = AlexandriaNetwork(
            network=self.base_protocol_app.network,
            bootnodes=self._alexandria_boot_info.bootnodes,
        )

        self.manager.run_daemon_child_service(alexandria_network)

        self.logger.info("Starting Alexandria...")
        self.logger.info("Root Directory         : %s", xdg_alexandria_root)
        await alexandria_network.ready()

        await self.manager.wait_finished()
