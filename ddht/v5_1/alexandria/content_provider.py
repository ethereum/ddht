import logging
from typing import Collection, Tuple

from async_service import Service
import trio

from ddht.base_message import InboundMessage
from ddht.v5_1.alexandria.abc import (
    AlexandriaClientAPI,
    ContentProviderAPI,
    ContentStorageAPI,
)
from ddht.v5_1.alexandria.messages import GetContentMessage
from ddht.v5_1.alexandria.partials.proof import compute_proof
from ddht.v5_1.alexandria.payloads import GetContentPayload
from ddht.v5_1.alexandria.sedes import content_sedes

MAX_CONTENT_PAYLOAD_SIZE = 1024


class _InvalidRequest(Exception):
    """
    Internal exception class signaling something about the request was invalid
    """


class ContentProvider(Service, ContentProviderAPI):
    logger = logging.getLogger("ddht.ContentProvider")

    def __init__(
        self,
        client: AlexandriaClientAPI,
        content_storages: Collection[ContentStorageAPI],
        concurrency: int = 3,
        max_chunks_per_request: int = 16,
    ) -> None:
        self._client = client
        self._content_storages = tuple(content_storages)
        self._concurrency_lock = trio.Semaphore(concurrency)
        self._ready = trio.Event()
        self._max_chunks_per_request = max_chunks_per_request

    async def ready(self) -> None:
        await self._ready.wait()

    async def run(self) -> None:
        async with trio.open_nursery() as nursery:
            async with self._client.subscribe(GetContentMessage) as subscription:
                self._ready.set()
                async for request in subscription:
                    nursery.start_soon(self.serve_request, request)

    def _get_payload_for_request(
        self, payload: GetContentPayload, content_storage: ContentStorageAPI
    ) -> Tuple[bool, bytes]:
        content = content_storage.get_content(payload.content_key)
        content_length = len(content)

        if content_length <= MAX_CONTENT_PAYLOAD_SIZE:
            is_proof = False
            content_data = content
        else:
            start_at = payload.start_chunk_index * 32

            if start_at >= content_length:
                raise _InvalidRequest

            max_chunks = min(self._max_chunks_per_request, payload.max_chunks)
            end_at = min(content_length, start_at + max_chunks * 32)

            # TODO: computationally expensive
            proof = compute_proof(content, sedes=content_sedes)
            partial = proof.to_partial(start_at, end_at - start_at)

            is_proof = True
            content_data = partial.serialize()

        return (is_proof, content_data)

    async def serve_request(self, request: InboundMessage[GetContentMessage]) -> None:
        self.logger.debug("Serving request: id=%s", request.request_id.hex())
        with trio.move_on_after(3) as scope:
            for content_storage in self._content_storages:
                if not content_storage.has_content(request.message.payload.content_key):
                    self.logger.debug(
                        "Ignoring content request for unknown key: content_key=%s",
                        request.message.payload.content_key.hex(),
                    )
                    return

                # This lock ensures that we are never trying to generate too many
                # proofs concurrently since proof generation is CPU bound.
                async with self._concurrency_lock:
                    # TODO: computationally expensive
                    is_proof, payload = self._get_payload_for_request(
                        payload=request.message.payload,
                        content_storage=content_storage,
                    )

                await self._client.send_content(
                    request.sender_node_id,
                    request.sender_endpoint,
                    is_proof=is_proof,
                    payload=payload,
                    request_id=request.request_id,
                )

        if scope.cancelled_caught:
            self.logger.debug(
                "Timeout serving request: id=%s", request.request_id.hex()
            )
