import pytest
import trio

from ddht.tools.driver import Network
from ddht.tools.factories.endpoint import EndpointFactory
from ddht.tools.factories.node_id import NodeIDFactory
from ddht.v5_1.events import Events
from ddht.v5_1.exceptions import SessionNotFound
from ddht.v5_1.session import SessionInitiator, SessionRecipient


@pytest.fixture
def network():
    return Network()


@pytest.fixture
def events():
    return Events()


@pytest.fixture
async def initiator(network, events):
    return network.node(events=events)


@pytest.fixture
async def recipient(network):
    return network.node()


@pytest.fixture
async def pool(initiator, events):
    return initiator.pool


@pytest.mark.trio
async def test_pool_remove_session(recipient, events, pool):
    async with events.session_created.subscribe_and_wait():
        session = pool.initiate_session(recipient.endpoint, recipient.node_id)

    removed_session = pool.remove_session(session.id)
    assert session == removed_session

    with pytest.raises(SessionNotFound):
        pool.remove_session(session.id)


@pytest.mark.trio
async def test_pool_initiate_session(recipient, events, pool):
    async with events.session_created.subscribe_and_wait():
        initiator_session = pool.initiate_session(recipient.endpoint, recipient.node_id)
    assert isinstance(initiator_session, SessionInitiator)

    assert initiator_session.is_initiator is True
    assert initiator_session.remote_node_id == recipient.node_id
    assert initiator_session.remote_endpoint == recipient.endpoint
    assert initiator_session.is_before_handshake

    assert initiator_session._events is events


@pytest.mark.trio
async def test_pool_receive_session(initiator, recipient, pool, events):
    # juxtopose these real quick to test things in the other direction
    recipient, initiator = initiator, recipient

    async with events.session_created.subscribe_and_wait():
        recipient_session = pool.receive_session(initiator.endpoint)
    assert isinstance(recipient_session, SessionRecipient)

    assert recipient_session.is_initiator is False
    assert recipient_session.remote_endpoint == initiator.endpoint
    assert recipient_session.is_before_handshake

    assert recipient_session._events is events


@pytest.mark.trio
async def test_pool_get_session_by_endpoint(network, initiator, pool, events):
    endpoint = EndpointFactory()

    # A: initiated locally, handshake incomplete
    remote_a = network.node(endpoint=endpoint)
    session_a = pool.initiate_session(endpoint, remote_a.node_id)

    # B: initiated locally, handshake complete
    remote_b = network.node(endpoint=endpoint)
    driver_b = network.session_pair(initiator, remote_b)
    with trio.fail_after(1):
        await driver_b.handshake()
    session_b = driver_b.initiator.session

    # C: initiated remotely, handshake incomplete
    session_c = pool.receive_session(endpoint)

    # D: initiated remotely, handshake complete
    remote_d = network.node(endpoint=endpoint)
    driver_d = network.session_pair(remote_d, initiator)
    await driver_d.handshake()
    session_d = driver_d.recipient.session

    # Some other sessions with non-matching endpoints before handshake
    session_e = pool.receive_session(EndpointFactory())
    session_f = pool.initiate_session(EndpointFactory(), NodeIDFactory())

    # Some other sessions with non-matching endpoints after handshake
    driver_g = network.session_pair(initiator)
    await driver_g.handshake()
    session_g = driver_g.initiator.session

    driver_h = network.session_pair(recipient=initiator)
    await driver_h.handshake()
    session_h = driver_h.recipient.session

    endpoint_matches = pool.get_sessions_for_endpoint(endpoint)
    assert len(endpoint_matches) == 4
    assert session_a in endpoint_matches
    assert session_b in endpoint_matches
    assert session_c in endpoint_matches
    assert session_d in endpoint_matches

    assert session_e not in endpoint_matches
    assert session_f not in endpoint_matches
    assert session_g not in endpoint_matches
    assert session_h not in endpoint_matches
