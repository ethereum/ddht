import datetime
import logging
import sqlite3
from typing import Any, Iterable, Optional, Tuple

from eth_keys import keys
from eth_typing import Hash32, NodeID
from eth_utils import to_tuple

from ddht.v5_1.alexandria.abc import AdvertisementDatabaseAPI
from ddht.v5_1.alexandria.payloads import Advertisement
from ddht.v5_1.alexandria.typing import ContentID

logger = logging.getLogger("ddht.advertisement.sqlite3")


ADVERTISEMENT_CREATE_STATEMENT = """CREATE TABLE advertisement (
    node_id BLOB NOT NULL,
    content_id BLOB NOT NULL,
    short_content_id INTEGER NOT NULL,
    content_key BLOB NOT NULL,
    hash_tree_root BLOB NOT NULL,
    signature BLOB UNIQUE NOT NULL PRIMARY KEY,
    expires_at DATETIME NOT NULL,
    created_at DATETIME NOT NULL,
    CONSTRAINT _node_id_length_32 CHECK (length(node_id) == 32),
    CONSTRAINT _content_id_length_32 CHECK (length(content_id) == 32),
    CONSTRAINT _content_key_length_lte_160 CHECK (length(content_key) <= 160),
    CONSTRAINT _hash_tree_root_length_32 CHECK (length(hash_tree_root) == 32),
    CONSTRAINT _signature_length_65 CHECK (length(signature) == 65)
)
"""


def create_tables(conn: sqlite3.Connection) -> None:
    record_table_exists = (
        conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            ("advertisement",),
        ).fetchone()
        is not None
    )

    if record_table_exists:
        return

    with conn:
        conn.execute(ADVERTISEMENT_CREATE_STATEMENT)
        conn.commit()


ADVERTISEMENT_INSERT_QUERY = """INSERT INTO advertisement
    (
        node_id,
        content_id,
        short_content_id,
        content_key,
        hash_tree_root,
        signature,
        expires_at,
        created_at
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
"""


DB_DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"


def insert_advertisement(
    conn: sqlite3.Connection, advertisement: Advertisement
) -> None:
    with conn:
        params = (
            advertisement.node_id,
            advertisement.content_id,
            # The high 64 bits of the content id for doing proximate queries
            int.from_bytes(advertisement.content_id, "big") >> 193,
            advertisement.content_key,
            advertisement.hash_tree_root,
            advertisement.signature.to_bytes(),
            advertisement.expires_at,
            datetime.datetime.utcnow().replace(microsecond=0),
        )
        conn.execute(ADVERTISEMENT_INSERT_QUERY, params)


ADVERTISEMENT_EXISTS_QUERY = """SELECT EXISTS (
    SELECT 1
    FROM advertisement
    WHERE advertisement.signature = ?
)
"""


def check_advertisement_exists(
    conn: sqlite3.Connection, signature: keys.Signature
) -> bool:
    row = conn.execute(ADVERTISEMENT_EXISTS_QUERY, (signature.to_bytes(),)).fetchone()
    return row == (1,)  # type: ignore


ADVERTISEMENT_GET_BY_SIGNATURE_QUERY = """SELECT
    advertisement.content_key AS advertisement_content_key,
    advertisement.hash_tree_root AS advertisement_hash_tree_root,
    advertisement.signature AS advertisement_signature,
    advertisement.expires_at AS advertisement_expires_at

    FROM advertisement
    WHERE advertisement.signature = ?
    LIMIT 1
"""


class AdvertisementNotFound(Exception):
    pass


def get_advertisement_by_signature(
    conn: sqlite3.Connection, signature: keys.Signature
) -> Advertisement:
    row = conn.execute(
        ADVERTISEMENT_GET_BY_SIGNATURE_QUERY, (signature.to_bytes(),)
    ).fetchone()
    if row is None:
        raise AdvertisementNotFound(f"No advertisement found: signature={signature}")

    content_key, hash_tree_root, signature_bytes, raw_expires_at = row
    expires_at = datetime.datetime.strptime(raw_expires_at, DB_DATETIME_FORMAT)
    signature = keys.Signature(signature_bytes)
    return Advertisement(
        content_key, hash_tree_root, expires_at, signature.v, signature.r, signature.s,
    )


DELETE_ADVERTISEMENT_QUERY = (
    """DELETE FROM advertisement WHERE advertisement.signature = ?"""
)


def delete_advertisement(conn: sqlite3.Connection, signature: keys.Signature) -> bool:
    cursor = conn.execute(DELETE_ADVERTISEMENT_QUERY, (signature.to_bytes(),))
    return bool(cursor.rowcount)


ADVERTISEMENT_QUERY = """SELECT
    advertisement.content_key AS advertisement_content_key,
    advertisement.hash_tree_root AS advertisement_hash_tree_root,
    advertisement.signature AS advertisement_signature,
    advertisement.expires_at AS advertisement_expires_at

    FROM advertisement
    {where_clause}
"""


@to_tuple
def _build_clauses(
    node_id: Optional[NodeID],
    content_id: Optional[ContentID],
    content_key: Optional[bytes],
    hash_tree_root: Optional[Hash32],
) -> Iterable[Tuple[str, Any]]:
    if node_id is not None:
        yield "advertisement.node_id == ?", node_id
    if content_id is not None:
        yield "advertisement.content_id == ?", content_id
    if content_key is not None:
        yield "advertisement_content_key == ?", content_key
    if hash_tree_root is not None:
        yield "advertisement_hash_tree_root == ?", hash_tree_root


def _build_where_clause(
    node_id: Optional[NodeID],
    content_id: Optional[ContentID],
    content_key: Optional[bytes],
    hash_tree_root: Optional[Hash32],
) -> Tuple[str, Tuple[Any, ...]]:
    if not any((node_id, content_key, content_id, hash_tree_root)):
        raise Exception("Must provide at least one parameter to filter on")

    clauses_and_params = _build_clauses(
        node_id, content_id, content_key, hash_tree_root
    )
    clauses, params = zip(*clauses_and_params)
    combined_clauses = " AND ".join(clauses)
    where_clause = f"WHERE {combined_clauses}"
    return where_clause, params


def query_advertisements(
    conn: sqlite3.Connection,
    node_id: Optional[NodeID],
    content_id: Optional[ContentID],
    content_key: Optional[bytes],
    hash_tree_root: Optional[Hash32],
) -> Iterable[Advertisement]:
    if any((node_id, content_id, content_key, hash_tree_root)):
        where_clause, params = _build_where_clause(
            node_id, content_id, content_key, hash_tree_root
        )
        query = ADVERTISEMENT_QUERY.format(where_clause=where_clause)
    else:
        query = ADVERTISEMENT_QUERY.format(where_clause="")
        params = ()

    for row in conn.execute(query, params):
        ad_content_key, ad_hash_tree_root, signature_bytes, raw_expires_at = row

        expires_at = datetime.datetime.strptime(raw_expires_at, DB_DATETIME_FORMAT)
        signature = keys.Signature(signature_bytes)
        yield Advertisement(
            ad_content_key,
            ad_hash_tree_root,
            expires_at,
            signature.v,
            signature.r,
            signature.s,
        )


ADVERTISEMENT_CLOSEST_QUERY = """SELECT
    advertisement.content_key AS advertisement_content_key,
    advertisement.hash_tree_root AS advertisement_hash_tree_root,
    advertisement.signature AS advertisement_signature,
    advertisement.expires_at AS advertisement_expires_at

    FROM advertisement
    ORDER BY ((?1 | advertisement.short_content_id) - (?1 & advertisement.short_content_id)) {order}
"""


def get_proximate_advertisements(
    conn: sqlite3.Connection, node_id: NodeID, reverse: bool
) -> Iterable[Advertisement]:
    short_node_id = int.from_bytes(node_id, "big") >> 193
    query = ADVERTISEMENT_CLOSEST_QUERY.format(order="DESC" if reverse else "")
    for row in conn.execute(query, (short_node_id,)):
        content_key, hash_tree_root, signature_bytes, raw_expires_at = row

        expires_at = datetime.datetime.strptime(raw_expires_at, DB_DATETIME_FORMAT)
        signature = keys.Signature(signature_bytes)
        yield Advertisement(
            content_key,
            hash_tree_root,
            expires_at,
            signature.v,
            signature.r,
            signature.s,
        )


HASH_TREE_ROOTS_QUERY = """SELECT DISTINCT
    advertisement.hash_tree_root AS advertisement_hash_tree_root

    FROM advertisement
    WHERE advertisement.content_id == ?
"""


def get_hash_tree_roots_for_content_id(
    conn: sqlite3.Connection, content_id: ContentID
) -> Iterable[Hash32]:
    for row in conn.execute(HASH_TREE_ROOTS_QUERY, (content_id,)):
        (hash_tree_root,) = row
        yield Hash32(hash_tree_root)


ADVERTISEMENT_COUNT_QUERY = """SELECT count(*) FROM advertisement"""


def get_advertisement_count(conn: sqlite3.Connection) -> int:
    row = conn.execute(ADVERTISEMENT_COUNT_QUERY).fetchone()
    (row_count,) = row
    return row_count  # type: ignore


class AdvertisementDatabase(AdvertisementDatabaseAPI):
    def __init__(self, conn: sqlite3.Connection) -> None:
        create_tables(conn)
        self._conn = conn

    def exists(self, advertisement: Advertisement) -> bool:
        return check_advertisement_exists(self._conn, advertisement.signature)

    def add(self, advertisement: Advertisement) -> None:
        try:
            insert_advertisement(self._conn, advertisement)
        except sqlite3.IntegrityError:
            # The IntegrityError here could either mean that this advertisement
            # is already in the database, **or** that one of the fields isn't
            # valid.  This `exists(...)` check ensures that we only swallow the
            # exception if the advertisement is not already present.
            if self.exists(advertisement):
                pass
            else:
                raise

    def remove(self, advertisement: Advertisement) -> bool:
        return delete_advertisement(self._conn, advertisement.signature)

    def query(
        self,
        node_id: Optional[NodeID] = None,
        content_id: Optional[ContentID] = None,
        content_key: Optional[bytes] = None,
        hash_tree_root: Optional[Hash32] = None,
    ) -> Iterable[Advertisement]:
        yield from query_advertisements(
            self._conn, node_id, content_id, content_key, hash_tree_root,
        )

    def closest(self, node_id: NodeID) -> Iterable[Advertisement]:
        yield from get_proximate_advertisements(self._conn, node_id, reverse=False)

    def furthest(self, node_id: NodeID) -> Iterable[Advertisement]:
        yield from get_proximate_advertisements(self._conn, node_id, reverse=True)

    def get_hash_tree_roots_for_content_id(
        self, content_id: ContentID
    ) -> Iterable[Hash32]:
        yield from get_hash_tree_roots_for_content_id(self._conn, content_id)

    def count(self) -> int:
        return get_advertisement_count(self._conn)

    def expired(self) -> Iterable[Advertisement]:
        # TODO
        ...
