import sqlite3

from eth_enr import ENRManager, QueryableENRDB, default_identity_scheme_registry
from eth_enr.exceptions import OldSequenceNumber
from eth_keys import keys
from eth_utils import encode_hex
import trio

from ddht._utils import generate_node_key_file, read_node_key_file
from ddht.app import BaseApplication
from ddht.boot_info import BootInfo
from ddht.constants import (
    DEFAULT_LISTEN,
    IP_V4_ADDRESS_ENR_KEY,
    ROUTING_TABLE_BUCKET_SIZE,
)
from ddht.kademlia import KademliaRoutingTable
from ddht.typing import AnyIPAddress
from ddht.upnp import UPnPService
from ddht.v5.client import Client
from ddht.v5.endpoint_tracker import EndpointTracker, EndpointVote
from ddht.v5.routing_table_manager import RoutingTableManager

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
    async def run(self) -> None:
        identity_scheme_registry = default_identity_scheme_registry

        enr_database_file = self._boot_info.base_dir / ENR_DATABASE_FILENAME
        enr_db = QueryableENRDB(
            sqlite3.connect(enr_database_file), identity_scheme_registry
        )
        self.enr_db = enr_db

        local_private_key = get_local_private_key(self._boot_info)

        enr_manager = ENRManager(private_key=local_private_key, enr_db=enr_db,)

        port = self._boot_info.port

        if b"udp" not in enr_manager.enr:
            enr_manager.update((b"udp", port))

        listen_on: AnyIPAddress
        if self._boot_info.listen_on is None:
            listen_on = DEFAULT_LISTEN
        else:
            listen_on = self._boot_info.listen_on
            # Update the ENR if an explicit listening address was provided
            enr_manager.update((IP_V4_ADDRESS_ENR_KEY, listen_on.packed))

        if self._boot_info.is_upnp_enabled:
            upnp_service = UPnPService(port)
            self.manager.run_daemon_child_service(upnp_service)

        routing_table = KademliaRoutingTable(
            enr_manager.enr.node_id, ROUTING_TABLE_BUCKET_SIZE
        )
        self.routing_table = routing_table

        for enr in self._boot_info.bootnodes:
            try:
                enr_db.set_enr(enr)
            except OldSequenceNumber:
                pass
            routing_table.update(enr.node_id)

        sock = trio.socket.socket(
            family=trio.socket.AF_INET, type=trio.socket.SOCK_DGRAM
        )

        client = Client(local_private_key, enr_db, enr_manager.enr.node_id, sock)

        endpoint_vote_channels = trio.open_memory_channel[EndpointVote](0)

        endpoint_tracker = EndpointTracker(
            local_private_key=local_private_key.to_bytes(),
            local_node_id=enr_manager.enr.node_id,
            enr_db=enr_db,
            identity_scheme_registry=identity_scheme_registry,
            vote_receive_channel=endpoint_vote_channels[1],
        )

        routing_table_manager = RoutingTableManager(
            local_node_id=enr_manager.enr.node_id,
            routing_table=routing_table,
            message_dispatcher=client.message_dispatcher,
            enr_db=enr_db,
            outbound_message_send_channel=client.outbound_message_send_channel,
            endpoint_vote_send_channel=endpoint_vote_channels[0],
        )

        self.logger.info(f"DDHT base dir: {self._boot_info.base_dir}")
        self.logger.info("Starting discovery service...")
        self.logger.info(f"Listening on {listen_on}:{port}")
        self.logger.info(f"Local Node ID: {encode_hex(enr_manager.enr.node_id)}")
        self.logger.info(f"Local ENR: {enr_manager.enr}")

        services = (
            client,
            endpoint_tracker,
            routing_table_manager,
        )
        await sock.bind((str(listen_on), port))
        with sock:
            for service in services:
                self.manager.run_daemon_child_service(service)
            await self.manager.wait_finished()
