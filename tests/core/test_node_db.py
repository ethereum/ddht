import time

from eth.db.backends.memory import MemoryDB
import pytest

from ddht.exceptions import OldSequenceNumber, UnknownIdentityScheme
from ddht.identity_schemes import (
    IdentitySchemeRegistry,
    default_identity_scheme_registry,
)
from ddht.node_db import NodeDB
from ddht.tools.factories.enr import ENRFactory
from ddht.tools.factories.keys import PrivateKeyFactory


@pytest.fixture
def node_db():
    return NodeDB(default_identity_scheme_registry, MemoryDB())


def test_checks_identity_scheme():
    db = NodeDB(IdentitySchemeRegistry(), MemoryDB())
    enr = ENRFactory()

    with pytest.raises(UnknownIdentityScheme):
        db.set_enr(enr)


def test_get_and_set_enr(node_db):
    private_key = PrivateKeyFactory().to_bytes()
    db = node_db
    enr = ENRFactory(private_key=private_key)

    with pytest.raises(KeyError):
        db.get_enr(enr.node_id)

    db.set_enr(enr)
    assert db.get_enr(enr.node_id) == enr

    updated_enr = ENRFactory(
        private_key=private_key, sequence_number=enr.sequence_number + 1
    )
    db.set_enr(updated_enr)
    assert db.get_enr(enr.node_id) == updated_enr

    with pytest.raises(OldSequenceNumber):
        db.set_enr(enr)


def test_delete_enr(node_db):
    db = node_db
    enr = ENRFactory()

    with pytest.raises(KeyError):
        db.delete_enr(enr.node_id)

    db.set_enr(enr)
    db.delete_enr(enr.node_id)

    with pytest.raises(KeyError):
        db.get_enr(enr.node_id)


def test_get_and_set_last_pong_time(node_db):
    db = node_db
    enr = ENRFactory()

    with pytest.raises(KeyError):
        db.get_last_pong_time(enr.node_id)

    pong_time = int(time.monotonic())
    db.set_last_pong_time(enr.node_id, pong_time)

    assert db.get_last_pong_time(enr.node_id) == pong_time


def test_delete_last_pong_time(node_db):
    db = node_db
    enr = ENRFactory()

    with pytest.raises(KeyError):
        db.delete_last_pong_time(enr.node_id)

    pong_time = int(time.monotonic())
    db.set_last_pong_time(enr.node_id, pong_time)

    db.delete_last_pong_time(enr.node_id)

    with pytest.raises(KeyError):
        db.get_last_pong_time(enr.node_id)
