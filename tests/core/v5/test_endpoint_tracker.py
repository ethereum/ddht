from async_service import background_trio_service
from eth.db.backends.memory import MemoryDB
import pytest
import pytest_trio
import trio
from trio.testing import wait_all_tasks_blocked

from ddht.constants import IP_V4_ADDRESS_ENR_KEY, UDP_PORT_ENR_KEY
from ddht.identity_schemes import default_identity_scheme_registry
from ddht.node_db import NodeDB
from ddht.tools.factories.discovery import EndpointVoteFactory
from ddht.tools.factories.endpoint import EndpointFactory
from ddht.tools.factories.enr import ENRFactory
from ddht.tools.factories.keys import PrivateKeyFactory
from ddht.v5.endpoint_tracker import EndpointTracker


@pytest.fixture
def private_key():
    return PrivateKeyFactory().to_bytes()


@pytest.fixture
def initial_enr(private_key):
    return ENRFactory(private_key=private_key,)


@pytest_trio.trio_fixture
async def node_db(initial_enr):
    node_db = NodeDB(default_identity_scheme_registry, MemoryDB())
    node_db.set_enr(initial_enr)
    return node_db


@pytest.fixture
def vote_channels():
    return trio.open_memory_channel(0)


@pytest.fixture
async def endpoint_tracker(private_key, initial_enr, node_db, vote_channels):
    endpoint_tracker = EndpointTracker(
        local_private_key=private_key,
        local_node_id=initial_enr.node_id,
        node_db=node_db,
        identity_scheme_registry=default_identity_scheme_registry,
        vote_receive_channel=vote_channels[1],
    )
    async with background_trio_service(endpoint_tracker):
        yield endpoint_tracker


@pytest.mark.trio
async def test_endpoint_tracker_updates_enr(
    endpoint_tracker, initial_enr, node_db, vote_channels
):
    endpoint = EndpointFactory()
    endpoint_vote = EndpointVoteFactory(endpoint=endpoint)
    await vote_channels[0].send(endpoint_vote)
    await wait_all_tasks_blocked()  # wait until vote has been processed

    updated_enr = node_db.get_enr(initial_enr.node_id)
    assert updated_enr.sequence_number == initial_enr.sequence_number + 1
    assert updated_enr[IP_V4_ADDRESS_ENR_KEY] == endpoint.ip_address
    assert updated_enr[UDP_PORT_ENR_KEY] == endpoint.port
