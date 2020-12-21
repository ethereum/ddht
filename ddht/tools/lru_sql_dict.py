import pickle
import sqlite3
from typing import Any, Iterator, MutableMapping, NamedTuple, Tuple, Union

from eth_utils.toolz import first

CREATE_CACHE_QUERY = """
    CREATE TABLE IF NOT EXISTS cache (
        id INTEGER NOT NULL PRIMARY KEY,
        key BLOB NOT NULL UNIQUE,
        value BLOB NOT NULL,
        pref INTEGER UNIQUE,
        nref INTEGER UNIQUE
    )
"""

CREATE_MAP_QUERY = """
    CREATE TABLE IF NOT EXISTS map (
        key BLOB NOT NULL PRIMARY KEY,
        pointer INTEGER NOT NULL UNIQUE
    )
"""

DEFAULT_CACHE_SIZE = 100000


class Node(NamedTuple):
    id: int
    key: Union[str, bytes]
    value: Any
    pref: int
    nref: int


class LRUSQLDict(MutableMapping[Union[str, bytes], Any]):
    """
    SQLite3-backed dictionary that implements an LRU cache.
    """

    def __init__(
        self, conn: sqlite3.Connection, cache_size: int = DEFAULT_CACHE_SIZE
    ) -> None:
        self._conn = conn
        self._execute(CREATE_CACHE_QUERY)
        self._execute(CREATE_MAP_QUERY)
        self.cache_size = cache_size

    def __iter__(self) -> Iterator[Union[str, bytes]]:
        with self._conn:
            for key in self._conn.execute("SELECT key FROM cache"):
                yield first(key)

    def __len__(self) -> int:
        result = self._fetch_single_query("SELECT COUNT(*) FROM cache;")
        return int(first(result))

    def __setitem__(self, key: Union[str, bytes], value: Any) -> None:
        # updates LRU cache
        try:
            self.__getitem__(key)
        except KeyError:
            self._insert_item(key, value)
        else:
            self._update_item(key, value)

    def __getitem__(self, key: Union[str, bytes]) -> Any:
        # updates LRU cache
        pointer_result = self._fetch_single_query(
            "SELECT pointer FROM map WHERE key=?;", (key,),
        )

        if not pointer_result:
            raise KeyError(f"Key ({str(key)}) not found in db.")

        (map_pointer,) = pointer_result

        lookup_result = self._fetch_single_query(
            "SELECT value FROM cache WHERE id=?;", (map_pointer,),
        )

        (value,) = lookup_result

        unpickled_value = self._unpickle(value)

        # update cache
        self.__delitem__(key)
        self._insert_item(key, unpickled_value)

        return unpickled_value

    def __delitem__(self, key: Union[str, bytes]) -> None:
        result = self._fetch_single_query(
            "SELECT id,key,value,pref,nref FROM cache WHERE key=?;", (key,),
        )

        if not result:
            raise KeyError(f"Cannot delete key. Key: {str(key)} not found in db.")

        node = Node(*result)

        # delete key from cache & map
        self._execute(
            "DELETE FROM map WHERE pointer=?;", (node.id,),
        )
        self._execute(
            "DELETE FROM cache WHERE id=?;", (node.id,),
        )

        # update any nrefs/prefs in cache
        nref_node = self._fetch_single_query(
            "SELECT id,key,value,pref,nref FROM cache WHERE nref=?;", (node.id,),
        )
        pref_node = self._fetch_single_query(
            "SELECT id,key,value,pref,nref FROM cache WHERE pref=?;", (node.id,),
        )

        if nref_node and pref_node:
            nref = Node(*nref_node)
            pref = Node(*pref_node)
            self._execute(
                "UPDATE cache SET nref=? WHERE id=?;", (pref.id, nref.id),
            )
            self._execute(
                "UPDATE cache SET pref=? WHERE id=?;", (nref.id, pref.id),
            )
        elif nref_node:
            nref = Node(*nref_node)
            self._execute(
                "UPDATE cache SET nref=? WHERE id=?;", (None, nref.id),
            )
        elif pref_node:
            pref = Node(*pref_node)
            self._execute(
                "UPDATE cache SET pref=? WHERE id=?;", (None, pref.id),
            )

    def _insert_item(self, key: Union[str, bytes], value: Any) -> None:
        # insert new item into map and cache
        pickled_value = self._pickle(value)
        if self.is_empty:
            new_pref = None
            new_nref = None
            new_pkey = 0

            self._execute(
                "INSERT INTO cache VALUES (?, ?, ?, ?, ?);",
                (new_pkey, key, pickled_value, new_pref, new_nref),
            )

            self._execute("INSERT INTO map VALUES (?, ?);", (key, new_pkey))

        else:
            # evict least recently used key/value
            if self.is_full:
                self.__delitem__(self.tail.key)

            # get old head
            old_head_pkey, _, _, old_head_pref, _ = self.head

            # get attributes for new head
            (max_pkey,) = self._fetch_single_query(
                "SELECT id FROM cache ORDER BY id DESC LIMIT 1;"
            )
            new_pkey = max_pkey + 1
            new_pref = None
            new_nref = old_head_pkey

            # add new head to cache
            self._execute(
                "INSERT INTO cache VALUES (?, ?, ?, ?, ?);",
                (new_pkey, key, pickled_value, new_pref, new_nref),
            )

            # add new head to map
            self._execute(
                "INSERT INTO map VALUES (?, ?);", (key, new_pkey),
            )

            # update old head in cache
            self._execute(
                "UPDATE cache SET pref=? WHERE id=?;", (new_pkey, old_head_pkey),
            )

    def _update_item(self, key: Union[str, bytes], value: Any) -> None:
        pickled_value = self._pickle(value)
        self._execute("UPDATE cache SET value=? WHERE key=?;", (pickled_value, key))

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

    def _pickle(self, value: Any) -> bytes:
        return sqlite3.Binary(pickle.dumps(value, -1))

    def _unpickle(self, value: bytes) -> Any:
        return pickle.loads(value)

    @property
    def is_full(self) -> bool:
        return self.__len__() >= self.cache_size

    @property
    def is_empty(self) -> bool:
        return self.__len__() == 0

    @property
    def head(self) -> Node:
        head = self._fetch_single_query(
            "SELECT id,key,value,pref,nref FROM cache WHERE pref IS NULL;"
        )
        if not head:
            raise Exception("No head found.")
        return Node(*head)

    @property
    def tail(self) -> Node:
        tail = self._fetch_single_query(
            "SELECT id,key,value,pref,nref FROM cache WHERE nref IS NULL;"
        )
        if not tail:
            raise Exception("No tail found.")
        return Node(*tail)

    # custom iterator type not compatible with supertype
    def values(self) -> Iterator[Any]:  # type: ignore
        for key in self.__iter__():
            result = self._fetch_single_query(
                "SELECT value FROM cache WHERE key=?;", (key,)
            )
            yield self._unpickle(first(result))

    # custom iterator type not compatible with supertype
    def items(self) -> Iterator[Tuple[Union[str, bytes], Any]]:  # type: ignore
        for key in self.__iter__():
            result = self._fetch_single_query(
                "SELECT value FROM cache WHERE key=?;", (key,)
            )
            yield key, self._unpickle(first(result))

    def iter_lru_cache(self) -> Iterator[Any]:
        if self.is_empty:
            raise Exception("Cannot iterate over empty dict.")

        head = self.head
        yield (head.key, self._unpickle(head.value))
        nref = head.nref

        for _ in range(self.__len__() - 1):
            result = self._fetch_single_query(
                "SELECT id,key,value,pref,nref FROM cache WHERE id=?;", (nref,),
            )
            node = Node(*result)
            yield (node.key, self._unpickle(node.value))
            nref = node.nref
