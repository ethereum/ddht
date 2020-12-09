import logging

from async_service import Service
from async_service import external_trio_api as external_api
from eth_typing import Hash32
from eth_utils import ValidationError
from ssz.constants import CHUNK_SIZE
import trio

from ddht.resource_queue import ResourceQueue
from ddht.v5_1.alexandria.abc import AlexandriaNetworkAPI, ContentRetrievalAPI
from ddht.v5_1.alexandria.content import content_key_to_content_id
from ddht.v5_1.alexandria.partials._utils import get_chunk_count_for_data_length
from ddht.v5_1.alexandria.partials.chunking import (
    MissingSegment,
    slice_segments_to_max_chunk_count,
)
from ddht.v5_1.alexandria.partials.proof import Proof, validate_proof
from ddht.v5_1.alexandria.typing import ContentKey


class ContentRetrieval(Service, ContentRetrievalAPI):
    """
    Rough sketch of retrieval algorithm.

    1. Fetch the proof for the first chunk:

        - Maybe early exit if the content is small and we get the full data
          in one request.

    2. Extract the content length from the first chunk and create a queue
       of the chunks we still need.
    3. Create a queue for the nodes that we will fetch data from.
    4. Start up to `concurrency` *workers* processes:
        - worker grabs available node from queue
        - worker grabs chunk range from queue
        - worker requests proof, validates proof contains data starting at
          the requested `start_chunk_index`.
        - *IF* proof is valid
            - push proof into the proof queue
            - push node back onto available nodes queue
            - push any needed chunks that aren't in the proof onto the proof queue.
        - *OTHERWISE*
            - penalize node???
            - place needed chunk back in the queue
    5. Outer process collects proofs from proof queue, merging them
       together.  Once merged proof contains full data, stop workers and
       return the merged data.

    """

    logger = logging.getLogger("ddht.ContentRetrieval")

    _content_proof: Proof
    _segment_queue: ResourceQueue[MissingSegment]

    def __init__(
        self,
        network: AlexandriaNetworkAPI,
        content_key: ContentKey,
        hash_tree_root: Hash32,
        max_node_ids: int = 32,
        concurrency: int = 3,
    ) -> None:
        self._network = network
        self._concurrency = concurrency

        self.content_key = content_key
        self.hash_tree_root = hash_tree_root
        self.content_id = content_key_to_content_id(content_key)

        self.node_queue = ResourceQueue((), max_resource_count=max_node_ids)
        self._content_ready = trio.Event()

    @external_api
    async def wait_content_proof(self) -> Proof:
        await self._content_ready.wait()
        return self._content_proof

    async def run(self) -> None:
        while self.manager.is_running:
            async with self.node_queue.reserve() as node_id:
                try:
                    proof = await self._network.get_content_proof(
                        node_id,
                        hash_tree_root=self.hash_tree_root,
                        content_key=self.content_key,
                        start_chunk_index=0,
                        max_chunks=10,
                    )
                except trio.TooSlowError:
                    continue
                else:
                    break
        else:
            raise trio.TooSlowError(
                "Unable to retrieve initial proof from any of the provide nodes"
            )

        # The size of the `segment_queue` mitigates against an attack scenario
        # where a malicious node gives us back proofs that contain the first
        # chunk which is required, but then contains as many gaps as possible
        # in the requested data.  At a `max_chunk_count` of 16, a valid proof
        # can push four new sub-segments onto the queue.  If *all* of our
        # connected peers are giving us this type of malicious response, our
        # queue will quickly grow to the maximum size at which point the
        # deadlock protection will be triggered within the worker process.
        #
        # This situation is mitigated by both setting our queue size to a
        # sufficiently large size that we expect it to not be able to be
        # filled, as well as allowing our worker process to timeout while
        # trying to add sub-segments back into the queue.
        missing_segments = proof.get_missing_segments()
        bite_size_missing_segments = slice_segments_to_max_chunk_count(
            missing_segments, max_chunk_count=16,
        )

        self._segment_queue = ResourceQueue(bite_size_missing_segments * 4)

        send_channel, receive_channel = trio.open_memory_channel[Proof](
            self._concurrency
        )

        if not proof.is_complete:
            content_length = proof.get_content_length()

            async with trio.open_nursery() as nursery:
                for worker_id in range(self._concurrency):
                    nursery.start_soon(self._worker, worker_id, send_channel)

                async with receive_channel:
                    async for partial_proof in receive_channel:
                        proof = proof.merge(partial_proof)
                        still_missing = sum(
                            segment.length for segment in proof.get_missing_segments()
                        )
                        percent_complete = (
                            (content_length - still_missing) * 100 / content_length
                        )
                        self.logger.info(
                            "combined proof: content_key=%s  proof=%s  progress=%.2f%%",
                            self.content_key.hex(),
                            proof,
                            percent_complete,
                        )
                        if proof.is_complete:
                            break

                # shut the workers down
                nursery.cancel_scope.cancel()

        self._content_proof = proof
        self._content_ready.set()

    async def _worker(
        self, worker_id: int, send_channel: trio.abc.SendChannel[Proof]
    ) -> None:
        worker_name = f"Worker[{self.content_key.hex()}:{worker_id}]"
        while True:
            async with self.node_queue.reserve() as node_id:
                self.logger.debug("%s: reserved node: %s", worker_name, node_id.hex())
                async with self._segment_queue.reserve() as segment:
                    start_data_index, data_length = segment

                    start_chunk_index = start_data_index // CHUNK_SIZE
                    max_chunks = get_chunk_count_for_data_length(data_length)

                    self.logger.debug(
                        "%s: reserved chunk: start_index=%d  max_chunks=%d",
                        worker_name,
                        start_chunk_index,
                        max_chunks,
                    )

                    # TODO: timeout and other exception handling
                    proof = await self._network.get_content_proof(
                        node_id,
                        hash_tree_root=self.hash_tree_root,
                        content_key=self.content_key,
                        start_chunk_index=start_chunk_index,
                        max_chunks=max_chunks,
                    )

                    try:
                        validate_proof(proof)
                    except ValidationError:
                        self.logger.debug(
                            "%s: removing node for sending invalid proof: node=%s",
                            worker_name,
                            node_id.hex(),
                        )
                        # If a peer gives us an invalid proof, remove them
                        # from rotation.
                        self.node_queue.remove(node_id)
                        continue

                    # check that the proof contains at minimum the first chunk we requested.
                    if not proof.has_chunk(start_chunk_index):
                        self.logger.debug(
                            "%s: removing node for not returning requested chunk: node=%s",
                            worker_name,
                            node_id.hex(),
                        )
                        # If the peer didn't include the start chunk,
                        # remove them from rotation.
                        self.node_queue.remove(node_id)
                        continue

                    self.logger.debug(
                        "%s: sending partial proof: node=%s",
                        worker_name,
                        node_id.hex(),
                    )
                    await send_channel.send(proof)

                    # Determine if there are any subsections to this
                    # segment that are still missing.
                    remaining_segments = segment.intersection(
                        tuple(proof.get_missing_segments())
                    )

                    # This *timeout* ensures that the workers will not deadlock on
                    # a full `segment_queue`.  In the case where we hit this
                    # timeout we may end up re-requesting a proof that we already
                    # have but that is *ok* and doesn't cause anything to break.
                    with trio.move_on_after(2):
                        # Remove the segment and push any still missing
                        # sub-segments onto the queue
                        for sub_segment in remaining_segments:
                            await self._segment_queue.add(sub_segment)

                        # It is important that the removal happen *after*
                        # we push the sub-segments on, otherwise, if we
                        # timeout after the main segment has been removed
                        # but before the sub-segments have been added,
                        # we'll lose track of the still missing
                        # sub-segments.
                        self._segment_queue.remove(segment)
