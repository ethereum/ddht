import bisect
import contextlib
import pathlib
import sqlite3
from typing import Any, ContextManager, Dict, Iterator, Optional, Set, Tuple

from ddht.v5_1.alexandria.abc import ContentStorageAPI
from ddht.v5_1.alexandria.content import content_key_to_content_id
from ddht.v5_1.alexandria.typing import ContentKey


class ContentNotFound(Exception):
    pass


class ContentAlreadyExists(Exception):
    pass


class BatchDecomissioned(Exception):
    pass


class _AtomicBatch(ContentStorageAPI):
    _deleted: Set[ContentKey]
    is_decomissioned: bool

    def __init__(self, storage: ContentStorageAPI) -> None:
        self._storage = storage
        self._batch = MemoryContentStorage()
        self._deleted = set()
        self.is_decomissioned = False

    def has_content(self, content_key: ContentKey) -> bool:
        if self.is_decomissioned:
            raise BatchDecomissioned

        if content_key in self._deleted:
            return False
        elif self._batch.has_content(content_key):
            return True
        else:
            return self._storage.has_content(content_key)

    def get_content(self, content_key: ContentKey) -> bytes:
        if self.is_decomissioned:
            raise BatchDecomissioned

        if content_key in self._deleted:
            raise ContentNotFound(f"Not Found: content_key={content_key.hex()}")

        try:
            return self._batch.get_content(content_key)
        except ContentNotFound:
            return self._storage.get_content(content_key)

    def set_content(
        self, content_key: ContentKey, content: bytes, exists_ok: bool = False
    ) -> None:
        if self.is_decomissioned:
            raise BatchDecomissioned

        if self.has_content(content_key) and not exists_ok:
            raise ContentAlreadyExists(
                f"Content already exists for key: content_key={content_key.hex()}"
            )

        self._batch.set_content(content_key, content, exists_ok=exists_ok)
        self._deleted.discard(content_key)

    def delete_content(self, content_key: ContentKey) -> None:
        if self.is_decomissioned:
            raise BatchDecomissioned

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
        if self.is_decomissioned:
            raise BatchDecomissioned

        base_keys = set(self._storage.enumerate_keys(start_key, end_key)).difference(
            self._deleted
        )
        batch_keys = set(self._batch.enumerate_keys(start_key, end_key))
        yield from sorted(base_keys | batch_keys)

    def atomic(self) -> ContextManager[ContentStorageAPI]:
        raise NotImplementedError("Atomic batch recursion not supported")

    def finalize(self) -> Tuple[Set[ContentKey], Tuple[Tuple[ContentKey, bytes], ...]]:
        if self.is_decomissioned:
            raise BatchDecomissioned

        self.is_decomissioned = True
        to_write = tuple(
            (content_key, self._batch.get_content(content_key))
            for content_key in self._batch.enumerate_keys()
        )
        return (
            self._deleted,
            to_write,
        )


class MemoryContentStorage(ContentStorageAPI):
    def __init__(self, db: Optional[Dict[ContentKey, bytes]] = None) -> None:
        if db is None:
            db = {}
        self._db = db

    def has_content(self, content_key: ContentKey) -> bool:
        return content_key in self._db

    def get_content(self, content_key: ContentKey) -> bytes:
        try:
            return self._db[content_key]
        except KeyError:
            raise ContentNotFound(f"Not Found: content_key={content_key.hex()}")

    def set_content(
        self, content_key: ContentKey, content: bytes, exists_ok: bool = False
    ) -> None:
        if content_key in self._db and exists_ok is False:
            raise ContentAlreadyExists(
                f"Content already exists for key: content_key={content_key.hex()}"
            )
        self._db[content_key] = content

    def delete_content(self, content_key: ContentKey) -> None:
        try:
            del self._db[content_key]
        except KeyError:
            raise ContentNotFound(f"Not Found: content_key={content_key.hex()}")

    def enumerate_keys(
        self,
        start_key: Optional[ContentKey] = None,
        end_key: Optional[ContentKey] = None,
    ) -> Iterator[ContentKey]:
        all_keys = sorted(self._db.keys())

        if start_key is None:
            left = 0
        else:
            left = bisect.bisect_left(all_keys, start_key)

        if end_key is None:
            right = None
        else:
            right = bisect.bisect_right(all_keys, end_key)

        yield from all_keys[left:right]

    @contextlib.contextmanager
    def atomic(self) -> Iterator[ContentStorageAPI]:
        batch = _AtomicBatch(self)

        yield batch

        to_delete, to_write = batch.finalize()
        for content_key in to_delete:
            self.delete_content(content_key)
        for content_key, content in to_write:
            self.set_content(content_key, content, exists_ok=True)


STORAGE_CREATE_STATEMENT = """CREATE TABLE storage (
    content_key BLOB NOT NULL PRIMARY KEY,
    path TEXT NOT NULL
    CONSTRAINT _path_not_empty CHECK (length(path) > 0)
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
        path
    )
    VALUES (?, ?)
"""


def insert_content(
    conn: sqlite3.Connection, content_key: ContentKey, path: pathlib.Path,
) -> None:
    with conn:
        conn.execute(STORAGE_INSERT_QUERY, (content_key, str(path)))


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
    storage.path AS storage_path

    FROM storage
    WHERE storage.content_key = ?
    LIMIT 1
"""


def get_content_path(conn: sqlite3.Connection, content_key: ContentKey) -> pathlib.Path:
    row = conn.execute(STORAGE_GET_PATH_QUERY, (content_key,)).fetchone()
    if row is None:
        raise ContentNotFound(f"No content found: content_key={content_key.hex()}")

    (raw_path,) = row
    path = pathlib.Path(raw_path)
    return path


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


class FileSystemContentStorage(ContentStorageAPI):
    base_dir: pathlib.Path

    def __init__(
        self, base_dir: pathlib.Path, conn: Optional[sqlite3.Connection] = None
    ) -> None:
        if conn is None:
            file_db_path = base_dir / "db.sqlite3"
            conn = sqlite3.connect(file_db_path)
        create_tables(conn)
        self._conn = conn
        self.base_dir = base_dir.resolve()

    def has_content(self, content_key: ContentKey) -> bool:
        return check_content_exists(self._conn, content_key)

    def get_content(self, content_key: ContentKey) -> bytes:
        content_path_rel = get_content_path(self._conn, content_key)
        content_path = self.base_dir / content_path_rel
        return content_path.read_bytes()

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
        content_id = content_key_to_content_id(content_key)
        content_id_hex = content_id.hex()

        # For some content_id: 0xdeadbeef12345...
        # The directory is: <base-dir>/de/ad/deadbeef1234...
        content_path = (
            self.base_dir / content_id_hex[:2] / content_id_hex[2:4] / content_id_hex
        )
        content_path_rel = content_path.relative_to(self.base_dir)

        # Lazily create the directory structure
        content_path.parent.mkdir(parents=True, exist_ok=True)

        # We have already checked that the file doesn't exist and that the
        # `content_key` is not present in the database, however, there is the
        # possibility for a race condition at the filesystem level where the
        # file appears between the check and writing to it.  In the case of any
        # error we want to avoid our filesystem and database being out-of-sync.
        try:
            with content_path.open("wb") as content_file:
                content_file.write(content)

            insert_content(self._conn, content_key, content_path_rel)
        except Exception:
            content_path.unlink(missing_ok=True)
            delete_content(self._conn, content_key)
            raise

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

        to_delete, to_write = batch.finalize()
        for content_key in to_delete:
            self.delete_content(content_key)
        for content_key, content in to_write:
            self.set_content(content_key, content, exists_ok=True)
