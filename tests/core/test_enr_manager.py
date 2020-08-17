from eth.db.backends.memory import MemoryDB
import pytest

from ddht.enr_manager import ENRManager
from ddht.identity_schemes import default_identity_scheme_registry
from ddht.node_db import NodeDB
from ddht.tools.factories.discovery import ENRFactory
from ddht.tools.factories.keys import PrivateKeyFactory


@pytest.fixture
def node_db():
    return NodeDB(default_identity_scheme_registry, MemoryDB())


def test_enr_manager_creates_enr_if_not_present(node_db):
    enr_manager = ENRManager(node_db, PrivateKeyFactory())
    assert enr_manager.enr

    assert node_db.get_enr(enr_manager.enr.node_id) == enr_manager.enr


def test_enr_manager_handles_existing_enr(node_db):
    private_key = PrivateKeyFactory()
    enr = ENRFactory(private_key=private_key.to_bytes(), sequence_number=0)
    node_db.set_enr(enr)

    enr_manager = ENRManager(node_db, private_key)
    assert enr_manager.enr == enr

    assert node_db.get_enr(enr_manager.enr.node_id) == enr_manager.enr


def test_enr_manager_updates_existing_enr(node_db):
    private_key = PrivateKeyFactory()
    enr = ENRFactory(
        private_key=private_key.to_bytes(),
        sequence_number=0,
        custom_kv_pairs={b"unicorns": b"rainbows"},
    )
    assert enr[b"unicorns"] == b"rainbows"
    node_db.set_enr(enr)

    enr_manager = ENRManager(node_db, private_key, kv_pairs={b"unicorns": b"cupcakes"})
    assert enr_manager.enr != enr
    assert enr_manager.enr.sequence_number == enr.sequence_number + 1
    assert enr_manager.enr[b"unicorns"] == b"cupcakes"

    assert node_db.get_enr(enr_manager.enr.node_id) == enr_manager.enr


def test_enr_manager_update_api(node_db):
    enr_manager = ENRManager(node_db, PrivateKeyFactory())
    assert b"unicorns" not in enr_manager.enr
    base_enr = enr_manager.enr

    enr_a = enr_manager.update((b"unicorns", b"rainbows"))
    assert enr_a.sequence_number == base_enr.sequence_number + 1

    assert enr_manager.enr[b"unicorns"] == b"rainbows"

    enr_b = enr_manager.update((b"unicorns", b"cupcakes"))
    assert enr_b.sequence_number == enr_a.sequence_number + 1

    assert enr_manager.enr[b"unicorns"] == b"cupcakes"
