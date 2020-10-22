import argparse
import contextlib
import logging
from typing import Iterator, Set, Tuple

from eth_enr import ENRAPI, ENRDB, ENRManager, default_identity_scheme_registry
from eth_enr.exceptions import OldSequenceNumber
from eth_typing import NodeID
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


class _StopScanning(Exception):
    pass


class Crawler(BaseApplication):
    def __init__(self, args: argparse.Namespace, boot_info: BootInfo) -> None:
        super().__init__(args, boot_info)
        self.concurrency = args.crawl_concurrency
        if not self.concurrency > 0:
            raise ValueError(
                f"Concurrency value while crawling must be > 0: {self.concurrency}"
            )

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

        self.seen_nodeids: Set[NodeID] = set()
        self.responsive_peers: Set[NodeID] = set()

        self.enrqueue_send, self.enrqueue_recv = trio.open_memory_channel[ENRAPI](
            10_000
        )

    async def fetch_enr_bucket(
        self, remote_enr: ENRAPI, bucket: int
    ) -> Tuple[ENRAPI, ...]:
        peer_id = remote_enr.node_id

        logger.debug(f"sending FindNode. nodeid={encode_hex(peer_id)} bucket={bucket}")

        try:
            with trio.fail_after(2):
                find_node = FindNodeMessage(
                    request_id=self.client.message_dispatcher.get_free_request_id(
                        peer_id
                    ),
                    distance=bucket,
                )

                responses = await self.client.message_dispatcher.request_nodes(
                    peer_id, find_node
                )

                return tuple(
                    enr for response in responses for enr in response.message.enrs
                )
        except trio.TooSlowError:
            logger.info(
                f"no response from peer. nodeid={encode_hex(peer_id)} bucket={bucket}"
            )
            raise _StopScanning()
        except UnexpectedMessage:
            logger.exception(
                "received a bad message from the peer. " f"nodeid={encode_hex(peer_id)}"
            )
            raise _StopScanning()

    async def scan_enr_buckets(self, remote_enr: ENRAPI) -> None:
        """
        Attempts to read ENRs from the outermost few buckets of the remote node.
        """
        peerid = remote_enr.node_id
        logger.debug(f"scanning. nodeid={encode_hex(peerid)}")

        SCAN_DEPTH = 100

        consecutive_empty_buckets = 0

        try:
            for bucket in range(256, 256 - SCAN_DEPTH, -1):
                enrs = await self.fetch_enr_bucket(remote_enr, bucket)

                novel_enrs = len(
                    [enr for enr in enrs if enr.node_id not in self.seen_nodeids]
                )

                logger.info(
                    f"received ENRs. nodeid={encode_hex(peerid)} bucket={bucket} "
                    f"enrs={len(enrs)} novel={novel_enrs}"
                )

                if len(enrs) == 0:
                    consecutive_empty_buckets += 1
                else:
                    self.responsive_peers.add(peerid)
                    consecutive_empty_buckets = 0

                # as we get deeper into the peer's routing table the chance that there are
                # any peers in the given bucket decreases exponentially. we save some time
                # by moving on to the next peer once we notice that the buckets have
                # started to think out.
                if consecutive_empty_buckets >= 5:
                    return

                for enr in enrs:
                    await self.schedule_enr_to_be_visited(enr)

                await trio.sleep(0.1)  # don't overburden this peer
        except _StopScanning:
            pass
        finally:
            # once this coro finishes we will never talk to this peer again, do some
            # cleanup now or we'll leak memory.
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
                await self.scan_enr_buckets(enr)

    async def schedule_enr_to_be_visited(self, enr: ENRAPI) -> None:
        if enr.node_id in self.seen_nodeids:
            # since each trip to a peer dumps their entire routing table there's no need
            # to visit a peer twice. This assumes that nodeids don't move around between
            # servers which is wrong but likely close enough.
            return

        if IP_V4_ADDRESS_ENR_KEY not in enr:
            logger.info(f"dropping ENR without IP address enr={enr} kv={enr.items()}")
            return

        self.seen_nodeids.add(enr.node_id)

        try:
            self.enr_db.set_enr(enr)
        except OldSequenceNumber:
            logger.info(f"dropping old ENR. enr={enr} kv={enr.items()}")
            return

        enqueued = self.enrqueue_send.statistics().current_buffer_used
        logger.info(
            f"found ENR. count={len(self.seen_nodeids)} enr={enr} node_id={enr.node_id.hex()}"
            f" sequence_number={enr.sequence_number} kv={enr.items()} queue_len={enqueued}"
        )

        await self.enrqueue_send.send(enr)

    async def run(self) -> None:
        logger.info("crawling!")

        boot_info = self._boot_info

        if boot_info.is_upnp_enabled:
            logger.info(
                "uPNP will not be used; crawling does not require listening for "
                "incoming connections."
            )

        for bootnode in boot_info.bootnodes:
            await self.schedule_enr_to_be_visited(bootnode)

        listen_on = boot_info.listen_on or DEFAULT_LISTEN
        logger.info(f"about to bind to port. bind={listen_on}:{boot_info.port}")
        await self.sock.bind((str(listen_on), boot_info.port))

        try:
            with self.sock:
                self.manager.run_daemon_child_service(self.client)

                for _ in range(self.concurrency):
                    self.manager.run_daemon_task(self.read_from_queue_until_done)

                # When it is time to quit one of the `read_from_queue_until_done` tasks will
                # notice and trigger a shutdown.
                await self.manager.wait_finished()
        finally:
            logger.info(
                f"scaning finished. found_peers={len(self.seen_nodeids)} "
                f"responsive_peers={len(self.responsive_peers)} "
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
