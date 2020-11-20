import pytest
import trio

from ddht.tools.factories.endpoint import EndpointFactory
from ddht.tools.factories.node_id import NodeIDFactory
from ddht.tools.factories.v5_1 import SessionChannels
from ddht.v5_1.events import Events
from ddht.v5_1.exceptions import SessionNotFound
from ddht.v5_1.pool import Pool
from ddht.v5_1.session import SessionInitiator, SessionRecipient

TEST_CACHE_SESSION_SIZE = 128


@pytest.fixture
def events():
    return Events()


@pytest.fixture
async def initiator(tester, events):
    return tester.node(events=events)


@pytest.fixture
async def recipient(tester):
    return tester.node()


@pytest.fixture
async def channels():
    return SessionChannels.init()


@pytest.fixture
async def pool(tester, initiator, events, channels):
    pool = Pool(
        local_private_key=initiator.private_key,
        local_node_id=initiator.enr.node_id,
        enr_db=initiator.enr_db,
        outbound_envelope_send_channel=channels.outbound_envelope_send_channel,
        inbound_message_send_channel=channels.inbound_message_send_channel,
        session_cache_size=TEST_CACHE_SESSION_SIZE,
        events=initiator.events,
    )
    tester.register_pool(pool, channels)
    return pool


@pytest.mark.trio
async def test_pool_lru_caches_sessions(tester, events, pool):
    recipients = []
    for _ in range(0, 150):
        recipients.append(tester.node())

    async with events.session_created.subscribe_and_wait():
        for recipient in recipients:
            pool.initiate_session(recipient.endpoint, recipient.node_id)

    session_count_by_endpoint = [
        len(pool._sessions_by_endpoint[key])
        for key in pool._sessions_by_endpoint.keys()
    ]

    assert len(pool._sessions) == TEST_CACHE_SESSION_SIZE
    assert sum(session_count_by_endpoint) == TEST_CACHE_SESSION_SIZE


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
async def test_pool_get_session_by_endpoint(tester, initiator, pool, events):
    endpoint = EndpointFactory()

    # A: initiated locally, handshake incomplete
    remote_a = tester.node(endpoint=endpoint)
    session_a = pool.initiate_session(endpoint, remote_a.node_id)

    # B: initiated locally, handshake complete
    remote_b = tester.node(endpoint=endpoint)
    driver_b = tester.session_pair(initiator, remote_b,)
    with trio.fail_after(1):
        await driver_b.handshake()
    session_b = driver_b.initiator.session

    # C: initiated remotely, handshake incomplete
    session_c = pool.receive_session(endpoint)

    # D: initiated remotely, handshake complete
    remote_d = tester.node(endpoint=endpoint)
    driver_d = tester.session_pair(remote_d, initiator,)
    await driver_d.handshake()
    session_d = driver_d.recipient.session

    # Some other sessions with non-matching endpoints before handshake
    session_e = pool.receive_session(EndpointFactory())
    session_f = pool.initiate_session(EndpointFactory(), NodeIDFactory())

    # Some other sessions with non-matching endpoints after handshake
    driver_g = tester.session_pair(initiator,)
    await driver_g.handshake()
    session_g = driver_g.initiator.session

    driver_h = tester.session_pair(recipient=initiator,)
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
