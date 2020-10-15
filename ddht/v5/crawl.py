import contextlib
import logging
from typing import Iterator, Set

from eth_enr import ENRAPI, ENRDB, ENRManager, default_identity_scheme_registry
from eth_enr.exceptions import OldSequenceNumber
from eth_utils import encode_hex
import trio

from ddht.app import BaseApplication
from ddht.boot_info import BootInfo
from ddht.constants import DEFAULT_LISTEN, IP_V4_ADDRESS_ENR_KEY
from ddht.exceptions import UnexpectedMessage
from ddht.v5.app import get_local_private_key
from ddht.v5.client import Client
from ddht.v5.messages import FindNodeMessage

logger = logging.getLogger("ddht.crawler")


class Crawler(BaseApplication):
    def __init__(self, concurrency: int, boot_info: BootInfo) -> None:
        super().__init__(boot_info)
        self.concurrency = concurrency

        self.enr_db = ENRDB(dict(), default_identity_scheme_registry)

        self.sock = trio.socket.socket(
            family=trio.socket.AF_INET, type=trio.socket.SOCK_DGRAM
        )

        self.private_key = get_local_private_key(boot_info)

        self.enr_manager = ENRManager(private_key=self.private_key, enr_db=self.enr_db)
        self.enr_manager.update((b"udp", boot_info.port))

        my_node_id = self.enr_manager.enr.node_id
        self.client = Client(self.private_key, self.enr_db, my_node_id, self.sock)

        self.active_tasks = ActiveTaskCounter()

        self.seen_nodeids: Set[bytes] = set()
        self.bad_enr_count = 0

        self.enrqueue_send, self.enrqueue_recv = trio.open_memory_channel[ENRAPI](2048)

    async def visit_enr(self, remote_enr: ENRAPI) -> None:
        logger.debug(f"sending FindNode(256). nodeid={encode_hex(remote_enr.node_id)}")
        try:
            with trio.fail_after(2):
                find_node = FindNodeMessage(request_id=0, distance=256)

                responses = await self.client.message_dispatcher.request_nodes(
                    remote_enr.node_id, find_node
                )

                assert len(responses) > 0

                total_enrs = responses[0].message.total
                logger.info(
                    f"successful handshake and response from peer. "
                    f"enrs={total_enrs} nodeid={encode_hex(remote_enr.node_id)} "
                    f"enr={remote_enr}"
                )

                for resp in responses:
                    enr_count = resp.message.total
                    received_enrs = resp.message.enrs
                    logger.debug(
                        f"Received response. nodeid={encode_hex(remote_enr.node_id)} "
                        f"enr_count={enr_count}"
                    )

                    for enr in received_enrs:
                        await self.schedule_enr_to_be_visited(enr)

        except trio.TooSlowError:
            logger.debug(
                f"no response from peer. nodeid={encode_hex(remote_enr.node_id)} enr={remote_enr}"
            )
            self.bad_enr_count += 1
        except UnexpectedMessage:
            # TODO: Nodes sometimes send us Nodes messages with an unexpectedly large
            # number of peers. 12 or 15 of them. We should probably accept these messages!
            logger.exception(
                "Received a bad message from the peer. "
                f"nodeid={encode_hex(remote_enr.node_id)} enr={remote_enr}"
            )
            self.bad_enr_count += 1
        finally:
            # we only send one packet per peer. do some cleanup now or we'll leak memory.
            self.client.discard_peer(remote_enr.node_id)

    async def read_from_queue_until_done(self) -> None:
        while True:

            # CAUTION: Do not insert any code here. There must not be any checkpoints
            #          between leaving active_tasks (in the previous loop iteration) and
            #          trying to read from the queue.

            try:
                enr = self.enrqueue_recv.receive_nowait()
            except trio.WouldBlock:
                if len(self.active_tasks) > 0:
                    # some tasks are still active so it's too soon to quit. Instead, block
                    # until new work comes in. If we've truly run out of work then this
                    # will block forever but that's okay, another task will notice and
                    # cause our cancellation.
                    enr = await self.enrqueue_recv.receive()
                else:
                    # nobody else is performing any work and there's also no work left to
                    # be done. It's time to quit! Calling this causes all other tasks to
                    # be canceled.
                    await self.manager.stop()
                    return

            # CAUTION: Do not insert any code here. There must not be any checkpoints
            #          between popping from the queue and entering active_tasks.

            with self.active_tasks.enter():
                await self.visit_enr(enr)

    async def schedule_enr_to_be_visited(self, enr: ENRAPI) -> None:
        if enr.node_id in self.seen_nodeids:
            # In order to be nicer on the network only send a single packet to each
            # remote node.
            return

        if IP_V4_ADDRESS_ENR_KEY not in enr:
            logger.info(f"Dropping ENR without IP address enr={enr} kv={enr.items()}")
            return

        self.seen_nodeids.add(enr.node_id)

        try:
            self.enr_db.set_enr(enr)
        except OldSequenceNumber:
            logger.info(f"Dropping old ENR. enr={enr} kv={enr.items()}")
            return

        logger.info(
            f"Found ENR. count={len(self.seen_nodeids)} enr={enr} node_id={enr.node_id.hex()}"
            f" sequence_number={enr.sequence_number} kv={enr.items()}"
        )

        await self.enrqueue_send.send(enr)

    async def run(self) -> None:
        logger.info("Crawling!")

        boot_info = self._boot_info

        if boot_info.is_upnp_enabled:
            logger.info(
                "UPNP will not be used; crawling does not require listening for "
                "incoming connections."
            )

        for bootnode in boot_info.bootnodes:
            await self.schedule_enr_to_be_visited(bootnode)

        listen_on = boot_info.listen_on or DEFAULT_LISTEN
        logger.info(f"About to listen. bind={listen_on}:{boot_info.port}")
        await self.sock.bind((str(listen_on), boot_info.port))

        with self.sock:
            self.manager.run_daemon_child_service(self.client)

            for _ in range(self.concurrency):
                self.manager.run_daemon_task(self.read_from_queue_until_done)

            # When it is time to quit one of the `read_from_queue_until_done` tasks will
            # notice and trigger a clean shutdown.
            await self.manager.wait_finished()

        successes = len(self.seen_nodeids) - self.bad_enr_count
        logger.info(
            f"Finished crawling. found_enrs={len(self.seen_nodeids)} "
            f"bad_enrs={self.bad_enr_count} successful_connects={successes}"
        )


class ActiveTaskCounter:
    def __init__(self) -> None:
        self.active_tasks: int = 0

    @contextlib.contextmanager
    def enter(self) -> Iterator[None]:
        try:
            self.active_tasks += 1
            yield
        finally:
            self.active_tasks -= 1

    def __len__(self) -> int:
        return self.active_tasks
