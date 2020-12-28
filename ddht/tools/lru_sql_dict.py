import sqlite3
from typing import (
    Any,
    Callable,
    Generic,
    ItemsView,
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


class Node(Generic[TKey, TValue], NamedTuple):
    key: TKey
    value: TValue
    pref: TKey
    nref: TKey


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
        self._key_encoder = key_encoder
        self._key_decoder = key_decoder
        self._value_encoder = value_encoder
        self._value_decoder = value_decoder

        self._execute(CREATE_CACHE_QUERY)

        # evict lru key/value if local db size > current cache size
        if self.cache_size:
            while self.__len__() > self.cache_size:
                self.__delitem__(self.tail.key)

    def __iter__(self) -> Iterator[TKey]:
        with self._conn:
            for key in self._conn.execute("SELECT key FROM cache"):
                yield self._key_decoder(first(key))

    def __len__(self) -> int:
        (result,) = self._fetch_single_query("SELECT COUNT(*) FROM cache;")
        # ignore b/c mypy cannot interpret the result as an integer
        return result  # type: ignore

    def __setitem__(self, key: TKey, value: TValue) -> None:
        # setting / updating a key/value will move the pair to the head of the lru cache
        try:
            self.__getitem__(key)
        except KeyError:
            self._insert_item(key, value)
        else:
            self._update_item(key, value)

    def __getitem__(self, key: TKey) -> TValue:
        # accessing a key/value will move the pair to the head of the lru cache
        serialized_key = self._key_encoder(key)
        lookup_result = self._fetch_single_query(
            "SELECT value FROM cache WHERE key=?;", (serialized_key,),
        )

        if not lookup_result:
            raise KeyError(key)

        (value,) = lookup_result

        deserialized_value = self._value_decoder(value)

        # update cache
        # TODO: rather than move kv pair to head by deleting and re-inserting,
        # change this to update all outdated references directly
        self.__delitem__(key)
        self._insert_item(key, deserialized_value)

        return deserialized_value

    def __delitem__(self, key: TKey) -> None:
        serialized_key = self._key_encoder(key)
        result = self._fetch_single_query(
            "SELECT key FROM cache WHERE key=?;", (serialized_key,),
        )

        if not result:
            raise KeyError(key)

        node_key = first(result)

        # delete key from cache
        self._execute(
            "DELETE FROM cache WHERE key=?;", (node_key,),
        )

        # update any nrefs/prefs in cache
        nref_result = self._fetch_single_query(
            "SELECT key FROM cache WHERE nref=?;", (node_key,),
        )
        pref_result = self._fetch_single_query(
            "SELECT key FROM cache WHERE pref=?;", (node_key,),
        )

        if nref_result and pref_result:
            nref_key = first(nref_result)
            pref_key = first(pref_result)
            self._execute(
                "UPDATE cache SET nref=? WHERE key=?;", (pref_key, nref_key),
            )
            self._execute(
                "UPDATE cache SET pref=? WHERE key=?;", (nref_key, pref_key),
            )
        elif nref_result:
            self._execute(
                "UPDATE cache SET nref=? WHERE key=?;", (None, first(nref_result)),
            )
        elif pref_result:
            self._execute(
                "UPDATE cache SET pref=? WHERE key=?;", (None, first(pref_result)),
            )

    def _insert_item(self, key: TKey, value: TValue) -> None:
        # insert new item into map and cache
        serialized_key = self._key_encoder(key)
        serialized_value = self._value_encoder(value)
        if self.is_empty:
            new_pref = None
            new_nref = None

            self._execute(
                "INSERT INTO cache VALUES (?, ?, ?, ?);",
                (serialized_key, serialized_value, new_pref, new_nref),
            )

        else:
            # evict lru key/value if local db size >= current cache size
            while self.is_full:
                self.__delitem__(self.tail.key)

            # get old head
            old_head_key = self._key_encoder(self.head.key)

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
        serialized_key = self._key_encoder(key)
        serialized_value = self._value_encoder(value)
        self._execute(
            "UPDATE cache SET value=? WHERE key=?;", (serialized_value, serialized_key)
        )

    def _fetch_single_query(self, query: str, args: Tuple[Any, ...] = ()) -> Any:
        with self._conn:
            cursor = self._conn.execute(query, args).fetchall()
        if len(cursor) > 1:
            raise Exception(
                f"Invalid db state. More than one result found for query: {query}."
            )
        if not cursor:
            return None
        return first(cursor)

    def _execute(self, query: str, args: Tuple[Any, ...] = ()) -> None:
        with self._conn:
            self._conn.execute(query, args)

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
            raise KeyError("No head found.")
        deserialized_key = self._key_decoder(head[0])
        deserialized_value = self._value_decoder(head[1])
        return Node(deserialized_key, deserialized_value, head[2], head[3])

    @property
    def tail(self) -> Node:
        tail = self._fetch_single_query(
            "SELECT key,value,pref,nref FROM cache WHERE nref IS NULL;"
        )
        if not tail:
            raise KeyError("No tail found.")
        deserialized_key = self._key_decoder(tail[0])
        deserialized_value = self._value_decoder(tail[1])
        return Node(deserialized_key, deserialized_value, tail[2], tail[3])

    # custom iterator type not compatible with supertype
    def values(self) -> ValuesView[TValue]:  # type: ignore
        for key in self.__iter__():
            result = self._fetch_single_query(
                "SELECT value FROM cache WHERE key=?;", (key,)
            )
            yield self._value_decoder(first(result))

    # custom iterator type not compatible with supertype
    def items(self) -> ItemsView[TKey, TValue]:  # type: ignore
        for key in self.__iter__():
            result = self._fetch_single_query(
                "SELECT value FROM cache WHERE key=?;", (key,)
            )
            yield key, self._value_decoder(first(result))

    def iter_lru_cache(self) -> Iterator[Tuple[TKey, TValue]]:
        if self.is_empty:
            raise IndexError("Cannot iterate over empty dict.")

        head = self.head
        yield head.key, head.value
        nref = head.nref

        for _ in range(self.__len__() - 1):
            result = self._fetch_single_query(
                "SELECT key,value,pref,nref FROM cache WHERE key=?;", (nref,),
            )
            deserialized_key = self._key_decoder(result[0])
            deserialized_value = self._value_decoder(result[1])
            node = Node(deserialized_key, deserialized_value, result[2], result[3])
            yield node.key, node.value
            nref = node.nref
