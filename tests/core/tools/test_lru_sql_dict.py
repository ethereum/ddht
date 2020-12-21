import sqlite3

from hypothesis import given
from hypothesis import strategies as st
import pytest

from ddht.tools.lru_sql_dict import LRUSQLDict


@pytest.fixture
def sqldict():
    return LRUSQLDict(sqlite3.connect(":memory:"), cache_size=3)


@given(
    key=st.text(),
    value=st.integers(min_value=(-(2 ** 63) - 1), max_value=(2 ** 63 - 1)),
)
def test_sqldict_with_string_keys(key, value, sqldict):
    sqldict[key] = value
    assert sqldict[key] == value


@given(
    key=st.binary(),
    value=st.integers(min_value=(-(2 ** 63) - 1), max_value=(2 ** 63 - 1)),
)
def test_sqldict_with_byte_keys(key, value, sqldict):
    sqldict[key] = value
    assert sqldict[key] == value


@pytest.mark.parametrize(
    "value",
    (None, True, False, 1, 1.2, "string", b"bytes", ("tuple",), ["list"], set("set"),),
)
def test_sqldict_set_accepts_picklable_items(sqldict, value):
    sqldict["key"] = value
    assert sqldict["key"] == value


def test_add_item_to_sqldict(sqldict):
    sqldict["one"] = "abc"

    assert sqldict["one"] == "abc"
    assert sqldict.head.key == "one"
    assert sqldict.tail.key == "one"
    assert len(sqldict) == 1

    sqldict["two"] = "xyz"

    assert sqldict["two"] == "xyz"
    assert sqldict.head.key == "two"
    assert sqldict.tail.key == "one"
    assert len(sqldict) == 2

    sqldict["three"] = "def"

    assert sqldict["three"] == "def"
    assert sqldict.head.key == "three"
    assert sqldict.tail.key == "one"
    assert len(sqldict) == 3
    assert sqldict.is_full

    # test update
    sqldict["two"] = "xyz-1"

    assert sqldict["two"] == "xyz-1"
    assert sqldict.head.key == "two"
    assert sqldict.tail.key == "one"
    assert len(sqldict) == 3
    assert sqldict.is_full

    sqldict["four"] = "ghi"

    assert sqldict["four"] == "ghi"
    assert sqldict.is_full
    assert sqldict.head.key == "four"
    assert sqldict.tail.key == "three"
    assert len(sqldict) == 3


def test_delete_item_from_sqldict():
    sqldict = LRUSQLDict(sqlite3.connect(":memory:"), cache_size=4)
    sqldict["one"] = "abc"
    sqldict["two"] = "def"
    sqldict["three"] = "ghi"
    sqldict["four"] = "jkl"

    assert sqldict.is_full
    assert not sqldict.is_empty

    # delete middle item
    del sqldict["two"]

    assert not sqldict.is_full
    assert sqldict.head.key == "four"
    assert sqldict.tail.key == "one"
    assert len(sqldict) == 3

    # delete head
    del sqldict["four"]

    assert not sqldict.is_full
    assert sqldict.head.key == "three"
    assert sqldict.tail.key == "one"
    assert len(sqldict) == 2

    # delete tail
    del sqldict["one"]

    assert not sqldict.is_full
    assert sqldict.head.key == "three"
    assert sqldict.tail.key == "three"
    assert len(sqldict) == 1

    # empty dict
    del sqldict["three"]

    assert sqldict.is_empty
    assert len(sqldict) == 0


def test_lru_cache(sqldict):
    sqldict["one"] = "abc"
    sqldict["two"] = "def"
    sqldict["three"] = "ghi"

    assert sqldict.head.key == "three"
    assert sqldict.tail.key == "one"
    assert (list(sqldict.iter_lru_cache())) == [
        ("three", "ghi"),
        ("two", "def"),
        ("one", "abc"),
    ]

    # test lru cache updates with getitem
    sqldict["one"]

    assert sqldict.head.key == "one"
    assert sqldict.tail.key == "two"
    assert list(sqldict.iter_lru_cache()) == [
        ("one", "abc"),
        ("three", "ghi"),
        ("two", "def"),
    ]

    # test lru cache updates with updateitem
    sqldict["two"] = "xyz"

    assert sqldict.head.key == "two"
    assert sqldict.tail.key == "three"
    assert list(sqldict.iter_lru_cache()) == [
        ("two", "xyz"),
        ("one", "abc"),
        ("three", "ghi"),
    ]


def test_dict_properties(sqldict):
    sqldict["one"] = "abc"
    sqldict["two"] = "def"
    sqldict["three"] = "ghi"

    assert sqldict
    assert sqldict.keys()
    assert len(list(sqldict.keys())) == 3
    assert len(list(sqldict.values())) == 3
    assert len(list(sqldict.items())) == 3

    key = sqldict.pop("one")
    assert key == "abc"
    assert len(sqldict) == 2


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
