import pytest
import trio


@pytest.fixture
async def alice_client(alice, bob):
    alice.node_db.set_enr(bob.enr)
    async with alice.client() as alice_client:
        yield alice_client


@pytest.fixture
async def bob_client(alice, bob):
    bob.node_db.set_enr(alice.enr)
    async with bob.client() as bob_client:
        yield bob_client


@pytest.mark.trio
async def test_client_send_ping(alice, bob, alice_client, bob_client):
    with trio.fail_after(2):
        async with alice.events.ping_sent.subscribe_and_wait():
            async with bob.events.ping_received.subscribe_and_wait():
                await alice_client.send_ping(bob.endpoint, bob.node_id)


@pytest.mark.trio
async def test_client_send_pong(alice, bob, alice_client, bob_client):
    with trio.fail_after(2):
        async with alice.events.pong_sent.subscribe_and_wait():
            async with bob.events.pong_received.subscribe_and_wait():
                await alice_client.send_pong(bob.endpoint, bob.node_id, request_id=1234)
