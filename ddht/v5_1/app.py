import logging

from async_service import Service, run_trio_service
from eth.db.backends.level import LevelDB
from eth_enr import ENRDB, ENRManager, default_identity_scheme_registry
from eth_keys import keys
from eth_utils import encode_hex
import trio

from ddht._utils import generate_node_key_file, read_node_key_file
from ddht.boot_info import BootInfo
from ddht.constants import DEFAULT_LISTEN, IP_V4_ADDRESS_ENR_KEY
from ddht.endpoint import Endpoint
from ddht.typing import AnyIPAddress
from ddht.upnp import UPnPService
from ddht.v5_1.client import Client
from ddht.v5_1.events import Events
from ddht.v5_1.messages import v51_registry
from ddht.v5_1.network import Network

logger = logging.getLogger("ddht.DDHT")


ENR_DATABASE_DIR_NAME = "enr-db"


def get_local_private_key(boot_info: BootInfo) -> keys.PrivateKey:
    if boot_info.private_key is None:
        # load from disk or generate
        node_key_file_path = boot_info.base_dir / "nodekey"
        if not node_key_file_path.exists():
            generate_node_key_file(node_key_file_path)
        return read_node_key_file(node_key_file_path)
    else:
        return boot_info.private_key


class Application(Service):
    logger = logger
    _boot_info: BootInfo

    def __init__(self, boot_info: BootInfo) -> None:
        self._boot_info = boot_info

    async def _update_enr_ip_from_upnp(
        self, enr_manager: ENRManager, upnp_service: UPnPService
    ) -> None:
        await upnp_service.get_manager().wait_started()

        with trio.move_on_after(10):
            _, external_ip = await upnp_service.get_ip_addresses()
            enr_manager.update((IP_V4_ADDRESS_ENR_KEY, external_ip.packed))

        while self.manager.is_running:
            _, external_ip = await upnp_service.wait_ip_changed()
            enr_manager.update((IP_V4_ADDRESS_ENR_KEY, external_ip.packed))

    async def run(self) -> None:
        identity_scheme_registry = default_identity_scheme_registry

        message_type_registry = v51_registry

        enr_database_dir = self._boot_info.base_dir / ENR_DATABASE_DIR_NAME
        enr_database_dir.mkdir(exist_ok=True)
        enr_db = ENRDB(LevelDB(enr_database_dir), identity_scheme_registry)

        local_private_key = get_local_private_key(self._boot_info)

        enr_manager = ENRManager(enr_db=enr_db, private_key=local_private_key,)

        port = self._boot_info.port
        events = Events()

        if b"udp" not in enr_manager.enr:
            enr_manager.update((b"udp", port))

        listen_on_ip_address: AnyIPAddress
        if self._boot_info.listen_on is None:
            listen_on_ip_address = DEFAULT_LISTEN
        else:
            listen_on_ip_address = self._boot_info.listen_on
            # Update the ENR if an explicit listening address was provided
            enr_manager.update((IP_V4_ADDRESS_ENR_KEY, listen_on_ip_address.packed))

        listen_on = Endpoint(listen_on_ip_address.packed, self._boot_info.port)

        if self._boot_info.is_upnp_enabled:
            upnp_service = UPnPService(port)
            self.manager.run_daemon_child_service(upnp_service)
            self.manager.run_daemon_task(
                self._update_enr_ip_from_upnp, enr_manager, upnp_service
            )

        bootnodes = self._boot_info.bootnodes

        client = Client(
            local_private_key=local_private_key,
            listen_on=listen_on,
            enr_db=enr_db,
            events=events,
            message_type_registry=message_type_registry,
        )
        network = Network(client=client, bootnodes=bootnodes,)

        logger.info("Protocol-Version: %s", self._boot_info.protocol_version.value)
        logger.info("DDHT base dir: %s", self._boot_info.base_dir)
        logger.info("Starting discovery service...")
        logger.info("Listening on %s:%d", listen_on, port)
        logger.info("Local Node ID: %s", encode_hex(enr_manager.enr.node_id))
        logger.info(
            "Local ENR: seq=%d enr=%s", enr_manager.enr.sequence_number, enr_manager.enr
        )

        await run_trio_service(network)
