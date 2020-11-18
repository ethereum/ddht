import pathlib
import random
import tempfile

import pytest

from ddht.tools.factories.content import ContentFactory
from ddht.v5_1.alexandria.content_storage import (
    ContentNotFound,
    FileSystemContentStorage,
    MemoryContentStorage,
)


@pytest.fixture()
def filesystem_base_dir():
    with tempfile.TemporaryDirectory() as base_dir:
        yield pathlib.Path(base_dir)


@pytest.fixture(params=("memory", "filesystem"))
def content_storage(request):
    if request.param == "memory":
        return MemoryContentStorage()
    elif request.param == "filesystem":
        base_dir = request.getfixturevalue("filesystem_base_dir")
        return FileSystemContentStorage(base_dir=base_dir)
    else:
        raise Exception(f"Unhandled parameter: {request.param}")


def test_content_storage_read_write_exists_delete(content_storage):
    content_key = b"\x00test-key"
    content = ContentFactory(128)

    assert content_storage.has_content(content_key) is False

    with pytest.raises(ContentNotFound):
        content_storage.get_content(content_key)
    with pytest.raises(ContentNotFound):
        content_storage.delete_content(content_key)

    content_storage.set_content(content_key, content)

    assert content_storage.has_content(content_key) is True

    assert content_storage.get_content(content_key) == content
    content_storage.delete_content(content_key)

    assert content_storage.has_content(content_key) is False

    with pytest.raises(ContentNotFound):
        content_storage.get_content(content_key)
    with pytest.raises(ContentNotFound):
        content_storage.delete_content(content_key)


def test_content_storage_enumerate_keys(content_storage):
    KEY_A = b"key-a"
    KEY_AA = b"key-aa"
    KEY_B = b"key-b"
    KEY_BB = b"key-bb"
    KEY_BB_1 = b"key-bb-1"
    KEY_BB_2 = b"key-bb-2"
    KEY_C = b"key-c"
    KEY_D = b"key-d"

    all_keys = (
        KEY_A,
        KEY_AA,
        KEY_B,
        KEY_BB,
        KEY_BB_1,
        KEY_BB_2,
        KEY_C,
        KEY_D,
    )

    shuffled_keys = list(all_keys)
    random.shuffle(shuffled_keys)

    for content_key in shuffled_keys:
        content_storage.set_content(content_key, b"dummy-content")

    between_none_none = tuple(content_storage.enumerate_keys())
    assert between_none_none == all_keys

    between_A_none = tuple(content_storage.enumerate_keys(start_key=KEY_A))
    assert between_A_none == all_keys

    between_A_D = tuple(content_storage.enumerate_keys(start_key=KEY_A, end_key=KEY_D))
    assert between_A_D == all_keys

    between_A_C = tuple(content_storage.enumerate_keys(start_key=KEY_A, end_key=KEY_C))
    assert between_A_C == all_keys[:-1]

    between_B_BB = tuple(
        content_storage.enumerate_keys(start_key=KEY_B, end_key=KEY_BB)
    )
    assert between_B_BB == (KEY_B, KEY_BB)
