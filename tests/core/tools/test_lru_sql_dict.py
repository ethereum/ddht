import sqlite3

from eth_utils import is_text, to_bytes, to_int
from hypothesis import given
from hypothesis import strategies as st
from lru import LRU
import pytest

from ddht.tools.lru_sql_dict import LRUSQLDict


def key_encoder(key) -> bytes:
    if is_text(key):
        return to_bytes(text=key)
    return key


def key_decoder(key) -> bytes:
    return key


def value_encoder(value) -> bytes:
    # int to bytes
    return to_bytes(value)


def value_decoder(value) -> int:
    # bytes to int
    return to_int(value)


@pytest.fixture
def sqldict():
    return LRUSQLDict(
        sqlite3.connect(":memory:"),
        key_encoder,
        key_decoder,
        value_encoder,
        value_decoder,
        3,
    )


@given(
    key=st.text(), value=st.integers(min_value=0, max_value=(2 ** 63 - 1)),
)
def test_sqldict_with_string_keys(key, value, sqldict):
    sqldict[key] = value
    assert sqldict[key] == value


@given(
    key=st.binary(), value=st.integers(min_value=0, max_value=(2 ** 63 - 1)),
)
def test_sqldict_with_byte_keys(key, value, sqldict):
    sqldict[key] = value
    assert sqldict[key] == value


def test_add_item_to_sqldict(sqldict):
    sqldict["one"] = 1

    assert sqldict["one"] == 1
    assert sqldict.head.key == b"one"
    assert sqldict.tail.key == b"one"
    assert len(sqldict) == 1

    sqldict["one"] = 100

    assert sqldict["one"] == 100
    assert sqldict.head.key == b"one"
    assert sqldict.tail.key == b"one"
    assert len(sqldict) == 1

    sqldict["two"] = 2

    assert sqldict["two"] == 2
    assert sqldict.head.key == b"two"
    assert sqldict.tail.key == b"one"
    assert len(sqldict) == 2

    sqldict["three"] = 3

    assert sqldict["three"] == 3
    assert sqldict.head.key == b"three"
    assert sqldict.tail.key == b"one"
    assert len(sqldict) == 3
    assert sqldict.is_full

    # test update
    sqldict["two"] = 20

    assert sqldict["two"] == 20
    assert sqldict.head.key == b"two"
    assert sqldict.tail.key == b"one"
    assert len(sqldict) == 3
    assert sqldict.is_full

    sqldict["four"] = 4

    assert sqldict["four"] == 4
    assert sqldict.is_full
    assert sqldict.head.key == b"four"
    assert sqldict.tail.key == b"three"
    assert len(sqldict) == 3


def test_delete_item_from_sqldict():
    sqldict = LRUSQLDict(
        sqlite3.connect(":memory:"),
        key_encoder,
        key_decoder,
        value_encoder,
        value_decoder,
        4,
    )
    sqldict["one"] = 1
    sqldict["two"] = 2
    sqldict["three"] = 3
    sqldict["four"] = 4

    assert sqldict.is_full
    assert not sqldict.is_empty

    # delete middle item
    del sqldict["two"]

    assert not sqldict.is_full
    assert sqldict.head.key == b"four"
    assert sqldict.tail.key == b"one"
    assert len(sqldict) == 3

    # delete head
    del sqldict["four"]

    assert not sqldict.is_full
    assert sqldict.head.key == b"three"
    assert sqldict.tail.key == b"one"
    assert len(sqldict) == 2

    # delete tail
    del sqldict["one"]

    assert not sqldict.is_full
    assert sqldict.head.key == b"three"
    assert sqldict.tail.key == b"three"
    assert len(sqldict) == 1

    # empty dict
    del sqldict["three"]

    assert sqldict.is_empty
    assert len(sqldict) == 0


def test_lru_cache(sqldict):
    sqldict["one"] = 1
    sqldict["two"] = 2
    sqldict["three"] = 3

    assert sqldict.head.key == b"three"
    assert sqldict.tail.key == b"one"
    assert (list(sqldict.iter_lru_cache())) == [
        (b"three", 3),
        (b"two", 2),
        (b"one", 1),
    ]

    # test lru cache updates with getitem
    sqldict["one"]

    assert sqldict.head.key == b"one"
    assert sqldict.tail.key == b"two"
    assert list(sqldict.iter_lru_cache()) == [
        (b"one", 1),
        (b"three", 3),
        (b"two", 2),
    ]

    # test lru cache updates with updateitem
    sqldict["two"] = 20

    assert sqldict.head.key == b"two"
    assert sqldict.tail.key == b"three"
    assert list(sqldict.iter_lru_cache()) == [
        (b"two", 20),
        (b"one", 1),
        (b"three", 3),
    ]


def test_dict_properties(sqldict):
    sqldict["one"] = 1
    sqldict["two"] = 2
    sqldict["three"] = 3

    assert sqldict
    assert sqldict.keys()
    assert len(list(sqldict.keys())) == 3
    assert len(list(sqldict.values())) == 3
    assert len(list(sqldict.items())) == 3

    key = sqldict.pop("one")
    assert key == 1
    assert len(sqldict) == 2


def test_dict_behavior_matches_LRU_implementation():
    lru = LRU(100)
    lru_sql_dict = LRUSQLDict(
        sqlite3.connect(":memory:"),
        key_encoder,
        key_decoder,
        value_encoder,
        value_decoder,
        100,
    )
    kv_pairs = ((to_bytes(number), number) for number in range(20))
    for pair in kv_pairs:
        lru[pair[0]] = pair[1]
        lru_sql_dict[pair[0]] = pair[1]

        assert lru.peek_first_item() == (lru_sql_dict.head.key, lru_sql_dict.head.value)
        assert lru.peek_last_item() == (lru_sql_dict.tail.key, lru_sql_dict.tail.value)

    lru[to_bytes(10)]
    lru_sql_dict[to_bytes(10)]

    assert lru.peek_first_item() == (lru_sql_dict.head.key, lru_sql_dict.head.value)
    assert lru.peek_last_item() == (lru_sql_dict.tail.key, lru_sql_dict.tail.value)

    lru[to_bytes(15)] = 100
    lru_sql_dict[to_bytes(15)] = 100

    assert lru.peek_first_item() == (lru_sql_dict.head.key, lru_sql_dict.head.value)
    assert lru.peek_last_item() == (lru_sql_dict.tail.key, lru_sql_dict.tail.value)

    del lru[to_bytes(0)]
    del lru_sql_dict[to_bytes(0)]

    assert lru.peek_first_item() == (lru_sql_dict.head.key, lru_sql_dict.head.value)
    assert lru.peek_last_item() == (lru_sql_dict.tail.key, lru_sql_dict.tail.value)

    lru[to_bytes(100)] = 100
    lru_sql_dict[to_bytes(100)] = 100

    assert lru.peek_first_item() == (lru_sql_dict.head.key, lru_sql_dict.head.value)
    assert lru.peek_last_item() == (lru_sql_dict.tail.key, lru_sql_dict.tail.value)

    lru[to_bytes(5)]
    lru_sql_dict[to_bytes(5)]

    assert lru.peek_first_item() == (lru_sql_dict.head.key, lru_sql_dict.head.value)
    assert lru.peek_last_item() == (lru_sql_dict.tail.key, lru_sql_dict.tail.value)


def test_dict_handles_variable_cache_sizes(tmpdir):
    initial_dict = LRUSQLDict(
        sqlite3.connect(tmpdir / "test_db"),
        key_encoder,
        key_decoder,
        value_encoder,
        value_decoder,
        3,
    )

    kv_pairs = ((to_bytes(number), number) for number in range(3))
    for pair in kv_pairs:
        initial_dict[pair[0]] = pair[1]

    assert len(initial_dict) == 3

    updated_dict = LRUSQLDict(
        sqlite3.connect(tmpdir / "test_db"),
        key_encoder,
        key_decoder,
        value_encoder,
        value_decoder,
        2,
    )

    assert len(updated_dict) == 2


def test_get_keyerror_with_empty_dict(sqldict):
    assert sqldict.is_empty
    with pytest.raises(KeyError):
        sqldict["one"]


def test_delete_keyerror_with_empty_dict(sqldict):
    assert sqldict.is_empty
    with pytest.raises(KeyError):
        del sqldict["one"]


def test_iter_lru_cache_raises_exception_with_empty_dict(sqldict):
    assert sqldict.is_empty
    with pytest.raises(Exception):
        list(sqldict.iter_lru_cache())


def test_head_raises_exception_with_empty_dict(sqldict):
    assert sqldict.is_empty
    with pytest.raises(Exception):
        sqldict.head


def test_tail_raises_exception_with_empty_dict(sqldict):
    assert sqldict.is_empty
    with pytest.raises(Exception):
        sqldict.tail
