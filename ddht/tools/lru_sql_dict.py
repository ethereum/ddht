import sqlite3
from typing import (
    AbstractSet,
    Any,
    Callable,
    Iterator,
    MutableMapping,
    NamedTuple,
    Tuple,
    TypeVar,
    ValuesView,
)

from eth_utils.toolz import first

#
# SQL Schema
#
# key (primary key): bytes
# value: bytes
# pref: bytes - None if node is head, else points to the previous node's key
# nref: bytes - None if node is tail, else points to the next node's key


CREATE_CACHE_QUERY = """
    CREATE TABLE IF NOT EXISTS cache (
        key BLOB NOT NULL PRIMARY KEY,
        value BLOB NOT NULL,
        pref BLOB UNIQUE,
        nref BLOB UNIQUE
    )
"""

TKey = TypeVar("TKey")
TValue = TypeVar("TValue")


class Node(NamedTuple):
    key: bytes
    value: bytes
    pref: bytes
    nref: bytes


class LRUSQLDict(MutableMapping[TKey, TValue]):
    """
    SQLite3-backed dictionary that implements an LRU cache.
    """

    def __init__(
        self,
        conn: sqlite3.Connection,
        key_encoder: Callable[[TKey], bytes],
        key_decoder: Callable[[bytes], TKey],
        value_encoder: Callable[[TValue], bytes],
        value_decoder: Callable[[bytes], TValue],
        cache_size: int = None,
    ) -> None:
        self._conn = conn
        self.cache_size = cache_size
        self.key_encoder = key_encoder
        self.key_decoder = key_decoder
        self.value_encoder = value_encoder
        self.value_decoder = value_decoder

        self._execute(CREATE_CACHE_QUERY)

    def __iter__(self) -> Iterator[TKey]:
        with self._conn:
            for key in self._conn.execute("SELECT key FROM cache"):
                yield self.key_decoder(first(key))

    def __len__(self) -> int:
        result = self._fetch_single_query("SELECT COUNT(*) FROM cache;")
        # ignore b/c mypy cannot interpret the result as an integer
        return first(result)  # type: ignore

    def __setitem__(self, key: TKey, value: TValue) -> None:
        # updates LRU cache
        try:
            self.__getitem__(key)
        except KeyError:
            self._insert_item(key, value)
        else:
            self._update_item(key, value)

    def __getitem__(self, key: TKey) -> TValue:
        # updates LRU cache
        serialized_key = self.key_encoder(key)
        lookup_result = self._fetch_single_query(
            "SELECT value FROM cache WHERE key=?;", (serialized_key,),
        )

        if not lookup_result:
            raise KeyError(str(key))

        (value,) = lookup_result

        deserialized_value = self.value_decoder(value)

        # update cache
        self.__delitem__(key)
        self._insert_item(key, deserialized_value)

        return deserialized_value

    def __delitem__(self, key: TKey) -> None:
        serialized_key = self.key_encoder(key)
        result = self._fetch_single_query(
            "SELECT key,value,pref,nref FROM cache WHERE key=?;", (serialized_key,),
        )

        if not result:
            raise KeyError(str(key))

        node = Node(*result)

        # delete key from cache
        self._execute(
            "DELETE FROM cache WHERE key=?;", (node.key,),
        )

        # update any nrefs/prefs in cache
        nref_node = self._fetch_single_query(
            "SELECT key,value,pref,nref FROM cache WHERE nref=?;", (node.key,),
        )
        pref_node = self._fetch_single_query(
            "SELECT key,value,pref,nref FROM cache WHERE pref=?;", (node.key,),
        )

        if nref_node and pref_node:
            nref = Node(*nref_node)
            pref = Node(*pref_node)
            self._execute(
                "UPDATE cache SET nref=? WHERE key=?;", (pref.key, nref.key),
            )
            self._execute(
                "UPDATE cache SET pref=? WHERE key=?;", (nref.key, pref.key),
            )
        elif nref_node:
            nref = Node(*nref_node)
            self._execute(
                "UPDATE cache SET nref=? WHERE key=?;", (None, nref.key),
            )
        elif pref_node:
            pref = Node(*pref_node)
            self._execute(
                "UPDATE cache SET pref=? WHERE key=?;", (None, pref.key),
            )

    def _insert_item(self, key: TKey, value: TValue) -> None:
        # insert new item into map and cache
        serialized_key = self.key_encoder(key)
        serialized_value = self.value_encoder(value)
        if self.is_empty:
            new_pref = None
            new_nref = None

            self._execute(
                "INSERT INTO cache VALUES (?, ?, ?, ?);",
                (serialized_key, serialized_value, new_pref, new_nref),
            )

        else:
            # evict least recently used key/value
            if self.is_full:
                self.__delitem__(self.key_decoder(self.tail.key))

            # get old head
            old_head_key, _, old_head_pref, _ = self.head

            # add new head to cache
            self._execute(
                "INSERT INTO cache VALUES (?, ?, ?, ?);",
                (serialized_key, serialized_value, None, old_head_key),
            )

            # update old head in cache
            self._execute(
                "UPDATE cache SET pref=? WHERE key=?;", (serialized_key, old_head_key),
            )

    def _update_item(self, key: TKey, value: TValue) -> None:
        serialized_key = self.key_encoder(key)
        serialized_value = self.value_encoder(value)
        self._execute(
            "UPDATE cache SET value=? WHERE key=?;", (serialized_value, serialized_key)
        )

    def _fetch_single_query(self, query: str, args: Tuple[Any, ...] = None) -> Any:
        with self._conn:
            if args:
                cursor = self._conn.execute(query, args).fetchall()
            else:
                cursor = self._conn.execute(query).fetchall()
        if len(cursor) > 1:
            raise Exception(
                f"Invalid db state. More than one result found for query: {query}."
            )
        if not cursor:
            return None
        return first(cursor)

    def _execute(self, query: str, args: Tuple[Any, ...] = None) -> None:
        with self._conn:
            if args:
                self._conn.execute(query, args)
            else:
                self._conn.execute(query)

    @property
    def is_full(self) -> bool:
        if not self.cache_size:
            return False
        return self.__len__() >= self.cache_size

    @property
    def is_empty(self) -> bool:
        return self.__len__() == 0

    @property
    def head(self) -> Node:
        head = self._fetch_single_query(
            "SELECT key,value,pref,nref FROM cache WHERE pref IS NULL;"
        )
        if not head:
            raise Exception("No head found.")
        return Node(*head)

    @property
    def tail(self) -> Node:
        tail = self._fetch_single_query(
            "SELECT key,value,pref,nref FROM cache WHERE nref IS NULL;"
        )
        if not tail:
            raise Exception("No tail found.")
        return Node(*tail)

    # custom iterator type not compatible with supertype
    def values(self) -> ValuesView[TValue]:  # type: ignore
        for key in self.__iter__():
            result = self._fetch_single_query(
                "SELECT value FROM cache WHERE key=?;", (key,)
            )
            yield self.value_decoder(first(result))

    # custom iterator type not compatible with supertype
    def items(self) -> AbstractSet[Tuple[TKey, TValue]]:  # type: ignore
        for key in self.__iter__():
            result = self._fetch_single_query(
                "SELECT value FROM cache WHERE key=?;", (key,)
            )
            yield key, self.value_decoder(first(result))

    def iter_lru_cache(self) -> Iterator[Tuple[TKey, TValue]]:
        if self.is_empty:
            raise Exception("Cannot iterate over empty dict.")

        head = self.head
        yield self.key_decoder(head.key), self.value_decoder(head.value)
        nref = head.nref

        for _ in range(self.__len__() - 1):
            result = self._fetch_single_query(
                "SELECT key,value,pref,nref FROM cache WHERE key=?;", (nref,),
            )
            node = Node(*result)
            yield self.key_decoder(node.key), self.value_decoder(node.value)
            nref = node.nref
