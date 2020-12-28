import hashlib
import sqlite3
import time
from typing import Optional

from eth_typing import Hash32, NodeID
from eth_utils import to_bytes, to_int

from ddht.tools.lru_sql_dict import LRUSQLDict
from ddht.v5_1.alexandria.abc import BroadcastLogAPI
from ddht.v5_1.alexandria.advertisements import Advertisement
from ddht.v5_1.alexandria.constants import ONE_HOUR


def key_encoder(key: bytes) -> bytes:
    return key


def key_decoder(key: bytes) -> bytes:
    return key


def value_encoder(value: int) -> bytes:
    return to_bytes(value)


def value_decoder(value: bytes) -> int:
    return to_int(value)


class BroadcastLog(BroadcastLogAPI):
    """
    Tracks the last time each advertisement was broadcast to each peer.
    """

    def __init__(self, conn: sqlite3.Connection, max_records: int = 8192):
        self._log = LRUSQLDict(
            conn, key_encoder, key_decoder, value_encoder, value_decoder, max_records
        )

    @property
    def count(self) -> int:
        return len(self._log)

    @property
    def cache_size(self) -> Optional[int]:
        return self._log.cache_size

    def log(self, node_id: NodeID, advertisement: Advertisement) -> None:
        key = Hash32(
            hashlib.sha256(node_id + advertisement.signature.to_bytes()).digest()
        )
        self._log[key] = int(time.monotonic())

    def was_logged(
        self, node_id: NodeID, advertisement: Advertisement, max_age: int = ONE_HOUR
    ) -> bool:
        key = Hash32(
            hashlib.sha256(node_id + advertisement.signature.to_bytes()).digest()
        )
        try:
            last_logged_at = int(self._log[key])
        except KeyError:
            return False
        else:
            return time.monotonic() - last_logged_at < max_age
