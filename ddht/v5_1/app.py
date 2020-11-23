import argparse
import sqlite3

from eth_enr import (
    ENRManager,
    QueryableENRDatabaseAPI,
    QueryableENRDB,
    default_identity_scheme_registry,
)
from eth_keys import keys
from eth_utils import encode_hex
from eth_utils.toolz import merge
import trio

from ddht._utils import generate_node_key_file, read_node_key_file
from ddht.app import BaseApplication
from ddht.boot_info import BootInfo
from ddht.constants import DEFAULT_LISTEN, IP_V4_ADDRESS_ENR_KEY
from ddht.endpoint import Endpoint
from ddht.rpc import RPCServer
from ddht.rpc_handlers import get_core_rpc_handlers
from ddht.typing import AnyIPAddress
from ddht.upnp import UPnPService
from ddht.v5_1.abc import ClientAPI, NetworkAPI
from ddht.v5_1.client import Client
from ddht.v5_1.events import Events
from ddht.v5_1.messages import v51_registry
from ddht.v5_1.network import Network
from ddht.v5_1.rpc_handlers import get_v51_rpc_handlers

ENR_DATABASE_FILENAME = "enrdb.sqlite3"


def get_local_private_key(boot_info: BootInfo) -> keys.PrivateKey:
    if boot_info.private_key is None:
        # load from disk or generate
        node_key_file_path = boot_info.base_dir / "nodekey"
        if not node_key_file_path.exists():
            generate_node_key_file(node_key_file_path)
        return read_node_key_file(node_key_file_path)
    else:
        return boot_info.private_key


class Application(BaseApplication):
    client: ClientAPI
    network: NetworkAPI

    def __init__(self, args: argparse.Namespace, boot_info: BootInfo) -> None:
        super().__init__(args, boot_info)
        self._ready = trio.Event()

    async def wait_ready(self) -> None:
        await self._ready.wait()

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

        enr_database_file = self._boot_info.base_dir / ENR_DATABASE_FILENAME
        enr_db: QueryableENRDatabaseAPI = QueryableENRDB(
            sqlite3.connect(enr_database_file), identity_scheme_registry
        )

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

        self.client = Client(
            local_private_key=local_private_key,
            listen_on=listen_on,
            enr_db=enr_db,
            events=events,
            message_type_registry=message_type_registry,
        )
        self.network = Network(client=self.client, bootnodes=bootnodes,)

        if self._boot_info.is_rpc_enabled:
            handlers = merge(
                get_core_rpc_handlers(enr_manager, self.network.routing_table),
                get_v51_rpc_handlers(self.network),
            )
            rpc_server = RPCServer(self._boot_info.ipc_path, handlers)
            self.manager.run_daemon_child_service(rpc_server)

        self.logger.info("Protocol-Version: %s", self._boot_info.protocol_version.value)
        self.logger.info("DDHT base dir: %s", self._boot_info.base_dir)
        self.logger.info("Starting discovery service...")
        self.logger.info("Listening on %s", listen_on)
        self.logger.info("Local Node ID: %s", encode_hex(enr_manager.enr.node_id))
        self.logger.info(
            "Local ENR: seq=%d enr=%s", enr_manager.enr.sequence_number, enr_manager.enr
        )

        self.manager.run_daemon_child_service(self.network)

        self._ready.set()

        await self.manager.wait_finished()
