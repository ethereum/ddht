import itertools

from hypothesis import given
from hypothesis import strategies as st
import pytest
import trio

from ddht.datagram import OutboundDatagram
from ddht.kademlia import KademliaRoutingTable
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
                    bob.endpoint, bob.node_id, distances=[255]
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


#
# Request/Response
#
@pytest.mark.trio
async def test_client_request_response_ping_pong(alice, bob, alice_client, bob_client):
    async with trio.open_nursery() as nursery:
        async with bob.events.ping_received.subscribe() as subscription:

            async def _send_response():
                ping = await subscription.receive()
                await bob_client.send_pong(
                    alice.endpoint, alice.node_id, request_id=ping.message.request_id,
                )

            nursery.start_soon(_send_response)

            with trio.fail_after(2):
                pong = await alice_client.ping(bob.endpoint, bob.node_id)
                assert pong.message.enr_seq == bob.enr.sequence_number
                assert pong.message.packet_ip == alice.endpoint.ip_address
                assert pong.message.packet_port == alice.endpoint.port


@pytest.mark.trio
async def test_client_request_response_find_nodes_found_nodes(
    alice, bob, alice_client, bob_client
):
    table = KademliaRoutingTable(bob.node_id, 256)
    for i in range(1000):
        enr = ENRFactory()
        table.update(enr.node_id)
        bob.node_db.set_enr(enr)

    checked_bucket_indexes = []

    for distance in range(256, 1, -1):
        async with trio.open_nursery() as nursery:
            async with bob.events.find_nodes_received.subscribe() as subscription:
                bucket = table.buckets[distance - 1]
                if not len(bucket):
                    break

                expected_enrs = tuple(
                    bob.node_db.get_enr(node_id) for node_id in bucket
                )

                async def _send_response():
                    find_nodes = await subscription.receive()
                    checked_bucket_indexes.append(distance)
                    await bob_client.send_found_nodes(
                        alice.endpoint,
                        alice.node_id,
                        enrs=expected_enrs,
                        request_id=find_nodes.message.request_id,
                    )

                nursery.start_soon(_send_response)

                with trio.fail_after(2):
                    found_nodes_messages = await alice_client.find_nodes(
                        bob.endpoint, bob.node_id, distances=[distance],
                    )
                    found_node_ids = {
                        enr.node_id
                        for message in found_nodes_messages
                        for enr in message.message.enrs
                    }
                    expected_node_ids = {enr.node_id for enr in expected_enrs}
                    assert found_node_ids == expected_node_ids

    assert len(checked_bucket_indexes) > 4


@given(datagram_bytes=st.binary(max_size=1024))
@pytest.mark.trio
async def test_client_handles_malformed_datagrams(tester, datagram_bytes):
    bob = tester.node()
    async with bob.client() as bob_client:
        alice = tester.node()
        alice.node_db.set_enr(bob.enr)
        bob.node_db.set_enr(alice.enr)

        async with alice.client():
            async with alice.events.ping_received.subscribe() as subscription:
                await bob_client._outbound_datagram_send_channel.send(
                    OutboundDatagram(datagram_bytes, alice.endpoint)
                )
                request_id = await bob_client.send_ping(alice.endpoint, alice.node_id)

                ping = await subscription.receive()
                assert ping.message.request_id == request_id
