from eth.db.backends.memory import MemoryDB
import factory

from ddht.identity_schemes import default_identity_scheme_registry
from ddht.node_db import NodeDB


class NodeDBFactory(factory.Factory):  # type: ignore
    class Meta:
        model = NodeDB

    identity_scheme_registry = default_identity_scheme_registry
    db = factory.LazyFunction(lambda: MemoryDB())
