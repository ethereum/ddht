import enum
import random
from typing import Optional

from hypothesis import given, settings
from hypothesis import strategies as st
import pytest

from ddht.tools.factories.content import ContentFactory
from ddht.tools.factories.node_id import NodeIDFactory
from ddht.v5_1.alexandria.abc import ContentStorageAPI
from ddht.v5_1.alexandria.content import (
    compute_content_distance,
    content_key_to_content_id,
)
from ddht.v5_1.alexandria.content_storage import (
    BatchDecommissioned,
    ContentAlreadyExists,
    ContentNotFound,
    ContentStorage,
)
from ddht.v5_1.alexandria.typing import ContentKey


@pytest.fixture
def base_storage():
    return ContentStorage.memory()


@pytest.fixture(params=("base", "batch"))
def content_storage(request, base_storage):
    if request.param == "base":
        yield base_storage
    elif request.param == "batch":
        with base_storage.atomic() as batch:
            yield batch
    else:
        raise Exception(f"Unhandled parameter: {request.param}")


def test_content_storage_sized(base_storage):
    assert len(base_storage) == 0

    base_storage.set_content(b"key-0", b"content-0")

    assert len(base_storage) == 1

    base_storage.set_content(b"key-0", b"content-0-updated", exists_ok=True)

    assert len(base_storage) == 1

    base_storage.set_content(b"key-1", b"content-1")

    assert len(base_storage) == 2

    base_storage.delete_content(b"key-0")

    assert len(base_storage) == 1


def test_content_storage_disk_usage(base_storage):
    content_keys = tuple(bytes([i]) for i in range(32))
    content_values = tuple(b"\x00" * 32 for i in range(32))
    expected = 32 * 32

    assert base_storage.total_size() == 0

    for content_key, content in zip(content_keys, content_values):
        base_storage.set_content(content_key, content)

    actual_size = base_storage.total_size()
    assert actual_size == expected


def test_content_storage_closest_and_furthest_iteration(base_storage):
    content_keys = tuple(b"key-" + bytes([i]) for i in range(32))
    for idx, content_key in enumerate(content_keys):
        base_storage.set_content(content_key, b"dummy-" + bytes([idx]))

    target = NodeIDFactory()

    expected_closest = tuple(
        sorted(
            content_keys,
            key=lambda content_key: compute_content_distance(
                target, content_key_to_content_id(content_key)
            ),
        )
    )
    expected_furthest = tuple(
        sorted(
            content_keys,
            key=lambda content_key: compute_content_distance(
                target, content_key_to_content_id(content_key)
            ),
            reverse=True,
        )
    )

    actual_closest = tuple(base_storage.iter_closest(target))
    actual_furthest = tuple(base_storage.iter_furthest(target))

    assert actual_closest == expected_closest
    assert actual_furthest == expected_furthest


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


def test_content_storage_set_content_exists_ok(content_storage):
    content_key = b"\x00test-key"
    content = ContentFactory(128)

    assert content_storage.has_content(content_key) is False

    content_storage.set_content(content_key, content)

    with pytest.raises(ContentAlreadyExists):
        content_storage.set_content(content_key, content)
    with pytest.raises(ContentAlreadyExists):
        content_storage.set_content(content_key, content, exists_ok=False)

    # sanity check
    assert content_storage.get_content(content_key) == content

    new_content = ContentFactory(256)
    assert new_content != content

    content_storage.set_content(content_key, new_content, exists_ok=True)

    # sanity check
    assert content_storage.get_content(content_key) == new_content


def test_content_storage_atomic_batch(base_storage):
    base_storage.set_content(b"key-a", b"content-a")
    base_storage.set_content(b"key-b", b"content-b")
    base_storage.set_content(b"key-c", b"content-c")

    with base_storage.atomic() as batch:
        # known keys should exist
        assert batch.has_content(b"key-a")
        assert batch.has_content(b"key-b")
        assert batch.has_content(b"key-c")

        # unkown keys shouldn't
        assert not batch.has_content(b"key-d")
        assert not batch.has_content(b"key-e")

        # set new keys
        batch.set_content(b"key-d", b"content-d")
        batch.set_content(b"key-e", b"content-e")

        # new keys should now exist in batch but not base
        assert batch.has_content(b"key-d")
        assert batch.has_content(b"key-e")
        assert not base_storage.has_content(b"key-d")
        assert not base_storage.has_content(b"key-e")

        # delete an unknown key
        with pytest.raises(ContentNotFound):
            batch.delete_content(b"key-f")

        # delete an pre-existing key
        batch.delete_content(b"key-a")

        # key should now be gone in batch but still present in underlying storage
        assert not batch.has_content(b"key-a")
        assert base_storage.has_content(b"key-a")

        with pytest.raises(ContentNotFound):
            batch.get_content(b"key-a")
        base_storage.get_content(b"key-a")

        # delete a key only in the batch
        batch.delete_content(b"key-d")

        assert not batch.has_content(b"key-d")
        assert not base_storage.has_content(b"key-d")

        # delete a pre-existing key and then write something new to it
        batch.delete_content(b"key-b")
        batch.set_content(b"key-b", b"updated-content-b")

        assert batch.get_content(b"key-b") == b"updated-content-b"
        assert base_storage.get_content(b"key-b") == b"content-b"

        batch_keys = set(batch.enumerate_keys())
        assert batch_keys == {b"key-b", b"key-c", b"key-e"}

        base_keys = set(base_storage.enumerate_keys())
        assert base_keys == {b"key-a", b"key-b", b"key-c"}

    # batch operations should now raise exceptions
    with pytest.raises(BatchDecommissioned):
        batch.has_content(b"key-a")
    with pytest.raises(BatchDecommissioned):
        batch.get_content(b"key-a")
    with pytest.raises(BatchDecommissioned):
        batch.set_content(b"key-a", b"dummy")
    with pytest.raises(BatchDecommissioned):
        batch.delete_content(b"key-a")
    with pytest.raises(BatchDecommissioned):
        set(batch.enumerate_keys())

    assert not base_storage.has_content(b"key-a")
    assert base_storage.get_content(b"key-b") == b"updated-content-b"
    assert base_storage.has_content(b"key-c")
    assert not base_storage.has_content(b"key-d")
    assert base_storage.has_content(b"key-e")

    final_keys = set(base_storage.enumerate_keys())
    assert final_keys == {b"key-b", b"key-c", b"key-e"}


class Action(enum.Enum):
    GET_RANDOM_KEY = enum.auto()
    SET_RANDOM_KEY = enum.auto()
    SET_RANDOM_KEY_EXISTS_OK = enum.auto()
    CHECK_RANDOM_KEY = enum.auto()
    DELETE_RANDOM_KEY = enum.auto()

    GET_KNOWN_KEY = enum.auto()
    SET_KNOWN_KEY = enum.auto()
    SET_KNOWN_KEY_EXISTS_OK = enum.auto()
    CHECK_KNOWN_KEY = enum.auto()
    DELETE_KNOWN_KEY = enum.auto()

    ENUMERATE_KEYS = enum.auto()


RANDOM_KEY_ACTIONS = {
    Action.GET_RANDOM_KEY,
    Action.SET_RANDOM_KEY,
    Action.SET_RANDOM_KEY_EXISTS_OK,
    Action.CHECK_RANDOM_KEY,
    Action.DELETE_RANDOM_KEY,
}
KNOWN_KEY_ACTIONS = {
    Action.GET_KNOWN_KEY,
    Action.SET_KNOWN_KEY,
    Action.SET_KNOWN_KEY_EXISTS_OK,
    Action.CHECK_KNOWN_KEY,
    Action.DELETE_KNOWN_KEY,
}


GET_ACTIONS = {
    Action.GET_RANDOM_KEY,
    Action.GET_KNOWN_KEY,
}
SET_ACTIONS = {
    Action.SET_RANDOM_KEY,
    Action.SET_RANDOM_KEY_EXISTS_OK,
    Action.SET_KNOWN_KEY,
    Action.SET_KNOWN_KEY_EXISTS_OK,
}
CHECK_ACTIONS = {
    Action.CHECK_RANDOM_KEY,
    Action.CHECK_KNOWN_KEY,
}
DELETE_ACTIONS = {
    Action.DELETE_RANDOM_KEY,
    Action.DELETE_KNOWN_KEY,
}


def apply_action(
    action: Action,
    content_key: Optional[ContentKey],
    content: Optional[bytes],
    *storages: ContentStorageAPI,
) -> None:
    """
    Apply an action to some set of `ContentStorageAPI`.  The first *storage*
    dictates the behavior expected on the other storages.  If an exception is
    thrown, the other storages are expected to throw the same exception.
    Otherwise, the return value is expected ot be equivalent.
    """
    base, *others = storages

    if action in GET_ACTIONS:
        assert content is None
        assert content_key is not None

        try:
            expected_content = base.get_content(content_key)
        except Exception as err:
            for other in others:
                with pytest.raises(type(err)):
                    other.get_content(content_key)
        else:
            for other in others:
                actual_content = other.get_content(content_key)
                assert actual_content == expected_content
    elif action in SET_ACTIONS:
        assert content is not None
        assert content_key is not None

        exists_ok = (
            action is Action.SET_RANDOM_KEY_EXISTS_OK
            or action is Action.SET_KNOWN_KEY_EXISTS_OK
        )
        try:
            base.set_content(content_key, content, exists_ok=exists_ok)
        except Exception as err:
            for other in others:
                with pytest.raises(type(err)):
                    other.set_content(content_key, content, exists_ok=exists_ok)
        else:
            for other in others:
                other.set_content(content_key, content, exists_ok=exists_ok)
    elif action in CHECK_ACTIONS:
        assert content is None
        assert content_key is not None

        expected = base.has_content(content_key)
        for other in others:
            actual = other.has_content(content_key)
            assert actual == expected
    elif action in DELETE_ACTIONS:
        assert content is None
        assert content_key is not None

        try:
            expected_content = base.delete_content(content_key)
        except Exception as err:
            for other in others:
                with pytest.raises(type(err)):
                    other.delete_content(content_key)
        else:
            for other in others:
                other.delete_content(content_key)
    elif action is Action.ENUMERATE_KEYS:
        assert content is None
        assert content_key is None

        expected_keys = set(base.enumerate_keys())
        for other in others:
            actual_keys = set(other.enumerate_keys())
            assert actual_keys == expected_keys
    else:
        raise NotImplementedError(f"Unsupported action: {action}")


def assert_storages_equal(left: ContentStorageAPI, right: ContentStorageAPI) -> None:
    """
    Helper to assert that two storages are identical.
    """
    left_keys = set(left.enumerate_keys())
    right_keys = set(right.enumerate_keys())

    assert left_keys == right_keys

    for content_key in left_keys:
        left_value = left.get_content(content_key)
        right_value = right.get_content(content_key)

        assert left_value == right_value


class _RevertBatch(Exception):
    """
    Exception just used to test atomic batch reversion to ensure we aren't
    catching any other legitimate exception my mistake.
    """

    pass


actions_st = st.lists(st.sampled_from(Action), min_size=0, max_size=100)
content_key_st = st.binary(min_size=4, max_size=8)
content_st = st.binary(min_size=8, max_size=8)


@settings(deadline=500)
@given(data=st.data(),)
def test_content_storage_atomic_batch_fuzzy(data):
    """
    Fuzz test the `ContentStorageAPI.atomic()` API.
    """
    base_storage = ContentStorage.memory()

    before_batch_actions = data.draw(actions_st)
    during_batch_actions = data.draw(actions_st)
    # throw about 5% of the time
    should_throw = st.sampled_from((False,) * 19 + (True,))

    # storage_a is managed such that it should be equivalent to
    # `base_storage` **after** the batch operations.
    storage_a = ContentStorage.memory()

    # storage_a is managed such that it should be equivalent to
    # `base_storage` **during** the batch operations.
    storage_b = ContentStorage.memory()

    # pre-populate with a single known key so that `KNOWN` actions don't
    # have to special case creating the first key.
    known_keys = [b"sentinal"]

    def apply_action_sequence(actions, *storages):
        for action in actions:
            if action in RANDOM_KEY_ACTIONS:
                content_key = data.draw(content_key_st)
                known_keys.append(content_key)
            elif action in KNOWN_KEY_ACTIONS:
                content_key = data.draw(st.sampled_from(known_keys))
            else:
                content_key = None

            if action in SET_ACTIONS:
                content = data.draw(content_st)
            else:
                content = None

            apply_action(action, content_key, content, *storages)

    apply_action_sequence(before_batch_actions, base_storage, storage_a, storage_b)

    # should be the same before the batch
    assert_storages_equal(base_storage, storage_a)
    assert_storages_equal(base_storage, storage_b)

    try:
        with base_storage.atomic() as batch:
            if should_throw:
                storages = (batch, storage_b)
            else:
                storages = (batch, storage_a, storage_b)

            apply_action_sequence(during_batch_actions, *storages)

            assert_storages_equal(batch, storage_b)

            if should_throw:
                raise _RevertBatch
    except _RevertBatch:
        pass

    # should be the same after the batch.
    assert_storages_equal(base_storage, storage_a)


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
