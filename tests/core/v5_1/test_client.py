import itertools

import pytest
import trio

from ddht.tools.factories.enr import ENRFactory


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


@pytest.mark.trio
async def test_client_send_find_nodes(alice, bob, alice_client, bob_client):
    with trio.fail_after(2):
        async with alice.events.find_nodes_sent.subscribe_and_wait():
            async with bob.events.find_nodes_received.subscribe_and_wait():
                await alice_client.send_find_nodes(
                    bob.endpoint, bob.node_id, distance=0
                )


@pytest.mark.parametrize(
    "enrs",
    (
        ENRFactory.create_batch(1),
        ENRFactory.create_batch(2),
        ENRFactory.create_batch(10),
        ENRFactory.create_batch(50),
    ),
)
@pytest.mark.trio
async def test_client_send_found_nodes(alice, bob, alice_client, bob_client, enrs):
    with trio.fail_after(2):
        async with alice.events.found_nodes_sent.subscribe_and_wait():
            async with bob.events.found_nodes_received.subscribe() as subscription:
                await alice_client.send_found_nodes(
                    bob.endpoint, bob.node_id, enrs=enrs, request_id=1234,
                )

                with trio.fail_after(2):
                    first_message = await subscription.receive()
                    remaining_messages = []
                    for _ in range(first_message.message.total - 1):
                        remaining_messages.append(await subscription.receive())
        all_received_enrs = tuple(first_message.message.enrs) + tuple(
            itertools.chain(*(message.message.enrs for message in remaining_messages))
        )
        expected_enrs_by_node_id = {enr.node_id: enr for enr in enrs}
        assert len(expected_enrs_by_node_id) == len(enrs)
        actual_enrs_by_node_id = {enr.node_id: enr for enr in all_received_enrs}

        assert expected_enrs_by_node_id == actual_enrs_by_node_id


@pytest.mark.trio
async def test_client_send_talk_request(alice, bob, alice_client, bob_client):
    with trio.fail_after(2):
        async with alice.events.talk_request_sent.subscribe_and_wait():
            async with bob.events.talk_request_received.subscribe_and_wait():
                await alice_client.send_talk_request(
                    bob.endpoint,
                    bob.node_id,
                    protocol=b"test",
                    request=b"test-request",
                )


@pytest.mark.trio
async def test_client_send_talk_response(alice, bob, alice_client, bob_client):
    with trio.fail_after(2):
        async with alice.events.talk_response_sent.subscribe_and_wait():
            async with bob.events.talk_response_received.subscribe_and_wait():
                await alice_client.send_talk_response(
                    bob.endpoint,
                    bob.node_id,
                    response=b"test-response",
                    request_id=1234,
                )


@pytest.mark.trio
async def test_client_send_register_topic(alice, bob, alice_client, bob_client):
    with trio.fail_after(2):
        async with alice.events.register_topic_sent.subscribe_and_wait():
            async with bob.events.register_topic_received.subscribe_and_wait():
                await alice_client.send_register_topic(
                    bob.endpoint,
                    bob.node_id,
                    topic=b"unicornsrainbowsunicornsrainbows",
                    enr=alice.enr,
                    ticket=b"test-ticket",
                    request_id=1234,
                )


@pytest.mark.trio
async def test_client_send_ticket(alice, bob, alice_client, bob_client):
    with trio.fail_after(2):
        async with alice.events.ticket_sent.subscribe_and_wait():
            async with bob.events.ticket_received.subscribe_and_wait():
                await alice_client.send_ticket(
                    bob.endpoint,
                    bob.node_id,
                    ticket=b"test-ticket",
                    wait_time=600,
                    request_id=1234,
                )


@pytest.mark.trio
async def test_client_send_registration_confirmation(
    alice, bob, alice_client, bob_client
):
    with trio.fail_after(2):
        async with alice.events.registration_confirmation_sent.subscribe_and_wait():
            async with bob.events.registration_confirmation_received.subscribe_and_wait():
                await alice_client.send_registration_confirmation(
                    bob.endpoint,
                    bob.node_id,
                    topic=b"unicornsrainbowsunicornsrainbows",
                    request_id=1234,
                )


@pytest.mark.trio
async def test_client_send_topic_query(alice, bob, alice_client, bob_client):
    with trio.fail_after(2):
        async with alice.events.topic_query_sent.subscribe_and_wait():
            async with bob.events.topic_query_received.subscribe_and_wait():
                await alice_client.send_topic_query(
                    bob.endpoint,
                    bob.node_id,
                    topic=b"unicornsrainbowsunicornsrainbows",
                    request_id=1234,
                )
