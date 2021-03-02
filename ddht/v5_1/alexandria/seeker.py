from typing import Set, List

from async_service import Service
from eth_enr import ENRAPI
from eth_typing import NodeID
from eth_utils import get_extended_debug_logger
import trio

from ddht._utils import reduce_enrs
from ddht.kademlia import compute_distance
from ddht.v5_1.alexandria.abc import AlexandriaNetworkAPI
from ddht.v5_1.alexandria.content import content_key_to_content_id
from ddht.v5_1.alexandria.typing import ContentID, ContentKey


class Seeker(Service):
    content_key: ContentKey
    content_id: ContentID

    _content: bytes
    _content_ready: trio.Event

    def __init__(self,
                 network: AlexandriaNetworkAPI,
                 content_key: ContentKey,
                 concurrency: int = 3) -> None:
        self.logger = get_extended_debug_logger('ddht.Seeker')

        self.concurrency = concurrency

        self.content_key = content_key
        self.content_id = content_key_to_content_id(content_key)

        self._network = network

        self._content_send, self.content_receive = trio.open_memory_channel[bytes](0)

    async def run(self) -> None:
        async with self.content_receive, self._content_send:
            raw_enr_send, raw_enr_receive = trio.open_memory_channel[ENRAPI](32)
            sorted_enr_send, sorted_enr_receive = trio.open_memory_channel[ENRAPI](0)

            self.manager.run_daemon_task(self._explore_for_enrs, raw_enr_send)
            self.manager.run_daemon_task(self._collate, sorted_enr_send, raw_enr_receive)

            for _ in range(self.concurrency):
                self.manager.run_daemon_task(
                    self._worker,
                    raw_enr_send,
                    sorted_enr_receive,
                )

            await self.manager.wait_finished()

    #
    # Machinery
    #
    async def _explore_for_enrs(self,
                                enr_send: trio.abc.SendChannel[ENRAPI]) -> None:
        async with self._network.explore(self.content_id) as enr_aiter:
            async for enr in enr_aiter:
                await enr_send.send(enr)

    async def _collate(self,
                       enr_send: trio.abc.SendChannel[ENRAPI],
                       enr_receive: trio.abc.ReceiveChannel[ENRAPI],
                       ) -> None:
        enr_buffer: List[ENRAPI] = []
        yielded_node_ids: Set[NodeID] = {self._network.local_node_id}

        while self.manager.is_running:
            # First wait for at least one ENR to be available...
            enr = await enr_receive.receive()
            if enr.node_id in yielded_node_ids:
                continue

            enr_buffer.append(enr)

            # Next, empty any additional values that are in the channel buffer.
            while True:
                try:
                    enr = enr_receive.receive_nowait()
                except trio.WouldBlock:
                    break
                else:
                    if enr.node_id in yielded_node_ids:
                        continue
                    enr_buffer.append(enr)

            # deduplicate
            enr_buffer = list(reduce_enrs(enr_buffer))

            # sort and pull off closest
            closest_enr, *enr_buffer = list(sorted(
                enr_buffer,
                key=lambda enr: compute_distance(self._network.local_node_id, enr.node_id),
            ))
            yielded_node_ids.add(closest_enr.node_id)
            await enr_send.send(closest_enr)

    async def _worker(self,
                      enr_send: trio.abc.SendChannel[ENRAPI],
                      enr_receive: trio.abc.ReceiveChannel[ENRAPI],
                      ) -> None:
        async for enr in enr_receive:
            self.logger.debug2(
                "Seeking: content_key=%s  node_id=%s",
                self.content_key.hex(),
                enr.node_id.hex(),
            )
            response = await self._network.find_content(enr.node_id, content_key=self.content_key)
            if response.is_content:
                self.logger.debug2(
                    "Got: content_key=%s  node_id=%s  content=%s",
                    self.content_key.hex(),
                    enr.node_id.hex(),
                    response.content.hex(),
                )
                await self._content_send.send(response.content)
            else:
                self.logger.debug2(
                    "Redirect: content_key=%s  node_id=%s  enrs=%s",
                    self.content_key.hex(),
                    enr.node_id.hex(),
                    '|'.join((enr.node_id.hex() for enr in response.enrs)),
                )
                for enr in response.enrs:
                    await enr_send.send(enr)
