import contextlib
import sqlite3
from typing import Any, ContextManager, Iterable, Iterator, Optional, Set, Tuple

from eth_typing import NodeID

from ddht.v5_1.alexandria.abc import ContentStorageAPI
from ddht.v5_1.alexandria.content import (
    content_key_to_content_id,
)
from ddht.v5_1.alexandria.typing import ContentKey


class ContentNotFound(Exception):
    pass


class ContentAlreadyExists(Exception):
    pass


class BatchDecommissioned(Exception):
    pass


class _AtomicBatch(ContentStorageAPI):
    _deleted: Set[ContentKey]
    is_decommissioned: bool

    def __init__(self, storage: ContentStorageAPI) -> None:
        self._storage = storage
        self._batch = ContentStorage.memory()
        self._deleted = set()
        self.is_decommissioned = False

    def __len__(self) -> int:
        raise NotImplementedError("Cannot query length batch length")

    def has_content(self, content_key: ContentKey) -> bool:
        if self.is_decommissioned:
            raise BatchDecommissioned

        if content_key in self._deleted:
            return False
        elif self._batch.has_content(content_key):
            return True
        else:
            return self._storage.has_content(content_key)

    def get_content(self, content_key: ContentKey) -> bytes:
        if self.is_decommissioned:
            raise BatchDecommissioned

        if content_key in self._deleted:
            raise ContentNotFound(f"Not Found: content_key={content_key.hex()}")

        try:
            return self._batch.get_content(content_key)
        except ContentNotFound:
            return self._storage.get_content(content_key)

    def set_content(
        self, content_key: ContentKey, content: bytes, exists_ok: bool = False
    ) -> None:
        if self.is_decommissioned:
            raise BatchDecommissioned

        if self.has_content(content_key) and not exists_ok:
            raise ContentAlreadyExists(
                f"Content already exists for key: content_key={content_key.hex()}"
            )

        self._batch.set_content(content_key, content, exists_ok=exists_ok)
        self._deleted.discard(content_key)

    def delete_content(self, content_key: ContentKey) -> None:
        if self.is_decommissioned:
            raise BatchDecommissioned

        if not self.has_content(content_key):
            raise ContentNotFound(f"Not Found: content_key={content_key.hex()}")

        if self._batch.has_content(content_key):
            self._batch.delete_content(content_key)

        if self._storage.has_content(content_key):
            self._deleted.add(content_key)

    def enumerate_keys(
        self,
        start_key: Optional[ContentKey] = None,
        end_key: Optional[ContentKey] = None,
    ) -> Iterator[ContentKey]:
        if self.is_decommissioned:
            raise BatchDecommissioned

        base_keys = set(self._storage.enumerate_keys(start_key, end_key)).difference(
            self._deleted
        )
        batch_keys = set(self._batch.enumerate_keys(start_key, end_key))
        yield from sorted(base_keys | batch_keys)

    def atomic(self) -> ContextManager[ContentStorageAPI]:
        raise NotImplementedError("Atomic batch recursion not supported")

    def finalize(self) -> Tuple[Set[ContentKey], Tuple[Tuple[ContentKey, bytes], ...]]:
        if self.is_decommissioned:
            raise BatchDecommissioned

        self.is_decommissioned = True
        to_write = tuple(
            (content_key, self._batch.get_content(content_key))
            for content_key in self._batch.enumerate_keys()
        )
        return (
            self._deleted,
            to_write,
        )

    def iter_furthest(self, target: NodeID) -> Iterable[ContentKey]:
        raise NotImplementedError("Proximate iteration not supported")

    def iter_closest(self, target: NodeID) -> Iterable[ContentKey]:
        raise NotImplementedError("Proximate iteration not supported")

    def total_size(self) -> int:
        raise NotImplementedError("`total_size` not support")


STORAGE_CREATE_STATEMENT = """CREATE TABLE storage (
    content_key BLOB NOT NULL PRIMARY KEY,
    short_content_id INTEGER NOT NULL,
    content BLOB NOT NULL
    CONSTRAINT _content_not_empty CHECK (length(content) > 0)
)
"""


def create_tables(conn: sqlite3.Connection) -> None:
    record_table_exists = (
        conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            ("storage",),
        ).fetchone()
        is not None
    )

    if record_table_exists:
        return

    with conn:
        conn.execute(STORAGE_CREATE_STATEMENT)
        conn.commit()


STORAGE_INSERT_QUERY = """INSERT INTO storage
    (
        content_key,
        short_content_id,
        content
    )
    VALUES (?, ?, ?)
"""


def insert_content(
    conn: sqlite3.Connection,
    content_key: ContentKey,
    content: bytes,
) -> None:
    content_id = content_key_to_content_id(content_key)
    # The high 64 bits of the content id for doing proximate queries
    short_content_id = int.from_bytes(content_id, "big") >> 193
    with conn:
        conn.execute(
            STORAGE_INSERT_QUERY,
            (content_key, short_content_id, content),
        )


STORAGE_EXISTS_QUERY = """SELECT EXISTS (
    SELECT 1
    FROM storage
    WHERE storage.content_key = ?
)
"""


def check_content_exists(conn: sqlite3.Connection, content_key: ContentKey) -> bool:
    row = conn.execute(STORAGE_EXISTS_QUERY, (content_key,)).fetchone()
    return row == (1,)  # type: ignore


STORAGE_GET_PATH_QUERY = """SELECT
    storage.content AS storage_content

    FROM storage
    WHERE storage.content_key = ?
    LIMIT 1
"""


def retrieve_content(conn: sqlite3.Connection, content_key: ContentKey) -> bytes:
    row = conn.execute(STORAGE_GET_PATH_QUERY, (content_key,)).fetchone()
    if row is None:
        raise ContentNotFound(f"No content found: content_key={content_key.hex()}")

    (content,) = row
    return content


DELETE_CONTENT_QUERY = """DELETE FROM storage WHERE storage.content_key = ?"""


def delete_content(conn: sqlite3.Connection, content_key: ContentKey) -> bool:
    with conn:
        cursor = conn.execute(DELETE_CONTENT_QUERY, (content_key,))
    return bool(cursor.rowcount)


ENUMERATE_CONTENT_KEYS_QUERY = """SELECT
    storage.content_key AS storage_content_key

    FROM storage
    {where_clause}
    ORDER BY storage.content_key
"""


def enumerate_content_keys(
    conn: sqlite3.Connection,
    left_bound: Optional[ContentKey],
    right_bound: Optional[ContentKey],
) -> Iterator[ContentKey]:
    query: str
    params: Tuple[Any, ...]

    if left_bound is None and right_bound is None:
        query = ENUMERATE_CONTENT_KEYS_QUERY.format(where_clause="")
        params = ()
    elif left_bound is None:
        where_clause = "WHERE storage.content_key <= ?"
        params = (right_bound,)
        query = ENUMERATE_CONTENT_KEYS_QUERY.format(where_clause=where_clause)
    elif right_bound is None:
        where_clause = "WHERE storage.content_key >= ?"
        params = (left_bound,)
        query = ENUMERATE_CONTENT_KEYS_QUERY.format(where_clause=where_clause)
    else:  # neither left_bound or right_bound are null
        where_clause = "WHERE storage.content_key >= ? AND storage.content_key <= ?"
        params = (left_bound, right_bound)
        query = ENUMERATE_CONTENT_KEYS_QUERY.format(where_clause=where_clause)

    for row in conn.execute(query, params):
        (content_key,) = row
        yield content_key


def get_row_count(conn: sqlite3.Connection) -> int:
    row = conn.execute("SELECT count(*) FROM storage").fetchone()
    (row_count,) = row
    return row_count  # type: ignore


CONTENT_PROXIMATE_QUERY = """SELECT
    storage.content_key AS storage_content_key

    FROM storage
    ORDER BY ((?1 | storage.short_content_id) - (?1 & storage.short_content_id)) {order}
"""


def get_proximate_content_keys(
    conn: sqlite3.Connection, node_id: NodeID, reverse: bool
) -> Iterable[ContentKey]:
    short_node_id = int.from_bytes(node_id, "big") >> 193
    query = CONTENT_PROXIMATE_QUERY.format(order="DESC" if reverse else "")
    for row in conn.execute(query, (short_node_id,)):
        (content_key,) = row
        yield content_key


def get_total_storage_size(conn: sqlite3.Connection):
    row = conn.execute("SELECT sum(length(storage.content)) FROM storage").fetchone()
    (total_size,) = row
    return total_size or 0


class ContentStorage(ContentStorageAPI):
    def __init__(
        self, conn: sqlite3.Connection
    ) -> None:
        create_tables(conn)
        self._conn = conn

    def __len__(self) -> int:
        return get_row_count(self._conn)

    @classmethod
    def memory(cls) -> 'ContentStorageAPI':
        conn = sqlite3.connect(':memory:')
        return cls(conn)

    def has_content(self, content_key: ContentKey) -> bool:
        return check_content_exists(self._conn, content_key)

    def get_content(self, content_key: ContentKey) -> bytes:
        return retrieve_content(self._conn, content_key)

    def set_content(
        self, content_key: ContentKey, content: bytes, exists_ok: bool = False
    ) -> None:
        """
        /content_id.hex()[:2]/content_id.hex()[2:4]/content_id.hex()
        """
        if self.has_content(content_key):
            if exists_ok:
                self.delete_content(content_key)
            else:
                raise ContentAlreadyExists(
                    f"Content already exists for key: content_key={content_key.hex()}"
                )

        insert_content(self._conn, content_key, content)

    def delete_content(self, content_key: ContentKey) -> None:
        was_deleted = delete_content(self._conn, content_key)
        if not was_deleted:
            raise ContentNotFound(f"No content found: content_key={content_key.hex()}")

    def enumerate_keys(
        self,
        start_key: Optional[ContentKey] = None,
        end_key: Optional[ContentKey] = None,
    ) -> Iterator[ContentKey]:
        yield from enumerate_content_keys(self._conn, start_key, end_key)

    @contextlib.contextmanager
    def atomic(self) -> Iterator[ContentStorageAPI]:
        batch = _AtomicBatch(self)

        yield batch

        # It is possible that any of these lines could raise an exception which
        # would leave us in an intermediate state.  The atomicity requirements
        # for this API are not critical and thus we accept this as a complexity
        # trade-off.
        to_delete, to_write = batch.finalize()
        for content_key in to_delete:
            self.delete_content(content_key)
        for content_key, content in to_write:
            self.set_content(content_key, content, exists_ok=True)

    def iter_furthest(self, target: NodeID) -> Iterable[ContentKey]:
        yield from get_proximate_content_keys(self._conn, target, reverse=True)

    def iter_closest(self, target: NodeID) -> Iterable[ContentKey]:
        yield from get_proximate_content_keys(self._conn, target, reverse=False)

    def total_size(self) -> int:
        return get_total_storage_size(self._conn)
