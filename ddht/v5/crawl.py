import logging
import math
from socket import inet_aton
import secrets
import trio
from async_service import Service, background_trio_service

from ddht.app import BaseApplication

from ddht.v5.app import get_local_private_key
from eth_enr import ENRDB, default_identity_scheme_registry, ENRManager, ENR, UnsignedENR
from ddht.base_message import AnyInboundMessage, AnyOutboundMessage
from ddht.constants import DEFAULT_LISTEN
from ddht.datagram import send_datagram, InboundDatagram, DatagramReceiver, OutboundDatagram, DatagramSender
from ddht.v5.channel_services import InboundPacket, PacketDecoder, OutboundPacket, PacketEncoder
from ddht.v5.constants import DEFAULT_BOOTNODES
from ddht.v5.messages import PingMessage, v5_registry, FindNodeMessage
from ddht.v5.message_dispatcher import MessageDispatcher
from ddht.v5.packer import Packer
from ddht.endpoint import Endpoint

from eth_keys import keys
from eth_utils import decode_hex, encode_hex
from eth_enr.exceptions import OldSequenceNumber
from trio.lowlevel import ParkingLot
from ddht.exceptions import UnexpectedMessage
from ddht.constants import IP_V4_ADDRESS_ENR_KEY
from async_service.exceptions import DaemonTaskExit

from ddht.boot_info import BootInfo

import contextvars
import random


logger = logging.getLogger("crawler")


current_task = contextvars.ContextVar('current_task')


def get_current_task():
    if current_task.get(None) is None:
        random_id = hex(random.getrandbits(16))
        current_task.set(random_id)

    return current_task.get()


class Crawler(BaseApplication):
    def __init__(self, concurrency, boot_info):
        super().__init__(boot_info)
        self.concurrency = concurrency

    async def run(self) -> None:
        logger.info("Crawling!")
        await do_crawl(self._boot_info)


async def do_crawl(boot_info: BootInfo) -> None:

    # TODO: use these...
    boot_info.port: int
    boot_info.bootnodes: Tuple[ENRAPI]
    boot_info.private_key: Optional[keys.PrivateKey]
    boot_info.listen_on

    # TODO: support UPNP?

    sock = trio.socket.socket(
        family=trio.socket.AF_INET, type=trio.socket.SOCK_DGRAM
    )

    enr_db = ENRDB(dict(), default_identity_scheme_registry)

    local_private_key = get_local_private_key(boot_info)

    enr_manager = ENRManager(private_key=local_private_key, enr_db=enr_db)
    enr_manager.update((b"udp", boot_info.port))

    outbound_datagram_channels = trio.open_memory_channel[OutboundDatagram](0)
    inbound_datagram_channels = trio.open_memory_channel[InboundDatagram](0)
    outbound_packet_channels = trio.open_memory_channel[OutboundPacket](0)
    inbound_packet_channels = trio.open_memory_channel[InboundPacket](0)
    outbound_message_channels = trio.open_memory_channel[AnyOutboundMessage](0)
    inbound_message_channels = trio.open_memory_channel[AnyInboundMessage](0)

    datagram_sender = DatagramSender(  # type: ignore
        outbound_datagram_channels[1], sock
    )
    datagram_receiver = DatagramReceiver(  # type: ignore
        sock, inbound_datagram_channels[0]
    )

    packet_encoder = PacketEncoder(  # type: ignore
        outbound_packet_channels[1], outbound_datagram_channels[0]
    )
    packet_decoder = PacketDecoder(  # type: ignore
        inbound_datagram_channels[1], inbound_packet_channels[0]
    )

    packer = Packer(
        local_private_key=local_private_key.to_bytes(),
        local_node_id=enr_manager.enr.node_id,
        enr_db=enr_db,
        message_type_registry=v5_registry,
        inbound_packet_receive_channel=inbound_packet_channels[1],
        inbound_message_send_channel=inbound_message_channels[0],
        outbound_message_receive_channel=outbound_message_channels[1],
        outbound_packet_send_channel=outbound_packet_channels[0],
    )

    message_dispatcher = MessageDispatcher(
        enr_db=enr_db,
        inbound_message_receive_channel=inbound_message_channels[1],
        outbound_message_send_channel=outbound_message_channels[0],
    )

    services = (
        packet_encoder, datagram_sender,
        datagram_receiver, packet_decoder,
        packer,
        message_dispatcher,
    )

    CONCURRENCY = 32
    queue = ENRQueue(CONCURRENCY)

    # 1. Queue up some ENRs to be crawled

    seen_nodeids = set()

    async def queue_enr_to_be_visited(enr):
        task = get_current_task()

        if enr.node_id in seen_nodeids:
            return

        if IP_V4_ADDRESS_ENR_KEY not in enr:
            logger.info(f"[{task}] Dropping ENR without IP address enr={enr} kv={enr.items()}")
            return

        seen_nodeids.add(enr.node_id)

        try:
            enr_db.set_enr(enr)
        except OldSequenceNumber:
            logger.info(f"[{task}] Dropping old ENR. enr={enr} kv={enr.items()}")
            return

        logger.info(f"[{task}] Found ENR. count={len(seen_nodeids)} enr={enr} kv={enr.items()}")

        await queue.send(enr)

    to_enr = lambda bootnode: ENR.from_repr(bootnode, default_identity_scheme_registry)
    bootnodes = [to_enr(bootnode) for bootnode in DEFAULT_BOOTNODES[2:3]]

    for bootnode in bootnodes:
        await queue_enr_to_be_visited(bootnode)

    bad_enr_count = 0

    async def visit_enr(remote_enr):
        nonlocal bad_enr_count

        task = get_current_task()

        logger.info(f"[{task}] sending FindNode(256). nodeid={encode_hex(remote_enr.node_id)}")
        try:
            with trio.fail_after(2):
                find_node = FindNodeMessage(request_id=0, distance=256)

                responses = await message_dispatcher.request_nodes(
                    remote_enr.node_id, find_node
                )

                logger.info(f"[{task}] successful handshake and response from peer.  nodeid={encode_hex(remote_enr.node_id)} enr={remote_enr}")

                for resp in responses:
                    enr_count = resp.message.total
                    received_enrs = resp.message.enrs
                    logger.info(f"[{task}] Received response. enr_count={enr_count}")

                    for enr in received_enrs:
                        await queue_enr_to_be_visited(enr)

                # we only send one packet per peer, so do some cleanup now or else we'll
                # leak memory.
                await packer.managed_peer_packers[remote_enr.node_id].manager.stop()

        except trio.TooSlowError:
            logger.error(f"[{task}] no response from peer. nodeid={encode_hex(remote_enr.node_id)} enr={remote_enr}")
            bad_enr_count += 1
        except UnexpectedMessage as error:
            # TODO: Nodes sometimes send us Nodes messages with an unexpectedly large
            # number of peers. 12 or 15 of them. We should probably accept these messages!
            logger.exception("[{task}] Received a bad message from the peer.  nodeid={encode_hex(remote_enr.node_id)} enr={remote_enr}")
            bad_enr_count += 1

    async def visitor(manager):
        task = get_current_task()
        async for enr in queue:
            await visit_enr(enr)
        await manager.stop()

    class CrawlService(Service):
        async def run(self) -> None:
            for service in services:
                self.manager.run_daemon_child_service(service)
            for _ in range(CONCURRENCY):
                self.manager.run_daemon_task(visitor, self.manager)
            await self.manager.wait_finished()

    listen_on = boot_info.listen_on or DEFAULT_LISTEN
    logger.info(f"About to listen. bind={listen_on}:{boot_info.port}")
    await sock.bind(("0.0.0.0", boot_info.port))

    with sock:
        async with background_trio_service(CrawlService()) as manager:
            await manager.wait_finished()

    successes = len(seen_nodeids) - bad_enr_count
    logger.info(f"Finished crawling. found_enrs={len(seen_nodeids)} bad_enrs={bad_enr_count} successful_connects={successes}")


class ENRQueue:
    """
    The script should quit when there is nothing left to crawl.

    However, it can't quit when the ENR queue is empty, because the ENR queue might
    be temporarily empty while some packets are still in-flight.

    It is time to quit when every coro is waiting on a new ENR from the queue. Not only is
    the queue empty, but there is no work in progress.

    Note that this class takes no responsibility for shutting down some corors if one
    of them crashes. If a coro crashes then this class will cause the others to hang
    forever, so you probably want to put them into some kind of nursury which handles
    cancellation.
    """
    logger = logging.getLogger("enrqueue")

    def __init__(self, concurrency_count):
        self.lot = ParkingLot()
        self.concurrency_count = concurrency_count
        self.done = trio.Event()

        self._send, self._recv = trio.open_memory_channel[ENR](math.inf)

    async def send(self, enr: ENR):
        if self.done.is_set():
            raise Exception('cannot add ENR, Queue has already finished')

        await self._send.send(enr)
        self.lot.unpark_all()

    def __aiter__(self):
        return self

    async def __anext__(self):
        task = get_current_task()

        if self.done.is_set():
            raise Exception(f'[{task}] cannot add ENR, Queue has already finished')

        while True:
            try:
                return self._recv.receive_nowait()
            except trio.WouldBlock:
                pass

            if len(self.lot) + 1 >= self.concurrency_count:
                # Every coro is waiting in new data. That means we're done!
                self.done.set()
                self.logger.debug(f'[{task}] exiting because everyone is waiting.')
                raise StopAsyncIteration

            # wait for more data to come in.
            await self.lot.park()

            if self.done.is_set():
                self.logger.debug(f'[{task}] exiting because someone else decided it was time')
                raise StopAsyncIteration
