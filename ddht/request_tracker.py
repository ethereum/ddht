from contextlib import contextmanager
import secrets
from typing import Iterator, Optional, Set, Tuple

from eth_typing import NodeID

from ddht.abc import RequestTrackerAPI

MAX_REQUEST_ID_ATTEMPTS = 3


def _get_random_request_id() -> bytes:
    return secrets.token_bytes(4)


class RequestTracker(RequestTrackerAPI):
    def __init__(self) -> None:
        self._reserved_request_ids: Set[Tuple[NodeID, bytes]] = set()

    def get_free_request_id(self, node_id: NodeID) -> bytes:
        for _ in range(MAX_REQUEST_ID_ATTEMPTS):
            request_id = _get_random_request_id()

            if (node_id, request_id) in self._reserved_request_ids:
                continue
            else:
                return request_id
        else:
            # The improbability of picking three already used request ids in a
            # row is sufficiently improbable that we can generally assume it
            # just will not ever happen (< 1/2**96)
            raise ValueError(
                f"Failed to get free request id ({len(self._reserved_request_ids)} "
                f"handlers added right now)"
            )

    @contextmanager
    def reserve_request_id(
        self, node_id: NodeID, request_id: Optional[bytes] = None
    ) -> Iterator[bytes]:
        """
        Reserve a `request_id` during the lifecycle of the context block.

        If a `request_id` is not provided, one will be generated lazily.

        .. note::

            If an explicit `request_id` is provided, it is not guaranteed to be
            collision free.
        """
        if request_id is None:
            request_id = self.get_free_request_id(node_id)
        try:
            self._reserved_request_ids.add((node_id, request_id))
            yield request_id
        finally:
            self._reserved_request_ids.remove((node_id, request_id))

    def is_request_id_active(self, node_id: NodeID, request_id: bytes) -> bool:
        return (node_id, request_id) in self._reserved_request_ids
