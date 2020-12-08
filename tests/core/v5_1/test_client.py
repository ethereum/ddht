from contextlib import AsyncExitStack
import itertools

from eth_enr.tools.factories import ENRFactory
from eth_utils import ValidationError
from hypothesis import given, settings
from hypothesis import strategies as st
import pytest
import trio

from ddht.base_message import AnyOutboundMessage
from ddht.datagram import OutboundDatagram
from ddht.enr import partition_enrs
from ddht.kademlia import KademliaRoutingTable, compute_log_distance
from ddht.v5_1.constants import FOUND_NODES_MAX_PAYLOAD_SIZE, REQUEST_RESPONSE_TIMEOUT
from ddht.v5_1.messages import FindNodeMessage, FoundNodesMessage, TalkRequestMessage


@pytest.mark.trio
async def test_client_send_ping(alice, bob, alice_client, bob_client):
    with trio.fail_after(2):
        async with alice.events.ping_sent.subscribe_and_wait():
            async with bob.events.ping_received.subscribe_and_wait():
                await alice_client.send_ping(bob.node_id, bob.endpoint)


@pytest.mark.trio
async def test_client_send_pong(alice, bob, alice_client, bob_client):
    with trio.fail_after(2):
        async with alice.events.pong_sent.subscribe_and_wait():
            async with bob.events.pong_received.subscribe_and_wait():
                await alice_client.send_pong(
                    bob.node_id, bob.endpoint, request_id=b"\x12"
                )


@pytest.mark.trio
async def test_client_send_find_nodes(alice, bob, alice_client, bob_client):
    with trio.fail_after(2):
        async with alice.events.find_nodes_sent.subscribe_and_wait():
            async with bob.events.find_nodes_received.subscribe_and_wait():
                await alice_client.send_find_nodes(
                    bob.node_id, bob.endpoint, distances=[255]
                )


@pytest.mark.parametrize(
    "enrs",
    (
        (),
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
                    bob.node_id, bob.endpoint, enrs=enrs, request_id=b"\x12",
                )

                with trio.fail_after(2):
                    first_message = await subscription.receive()
                    remaining_messages = []
                    assert first_message.message.total >= 1
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
                    bob.node_id,
                    bob.endpoint,
                    protocol=b"test",
                    payload=b"test-request",
                )


@pytest.mark.trio
async def test_client_send_talk_response(alice, bob, alice_client, bob_client):
    with trio.fail_after(2):
        async with alice.events.talk_response_sent.subscribe_and_wait():
            async with bob.events.talk_response_received.subscribe_and_wait():
                await alice_client.send_talk_response(
                    bob.node_id,
                    bob.endpoint,
                    payload=b"test-response",
                    request_id=b"\x12",
                )


@pytest.mark.trio
async def test_client_send_register_topic(alice, bob, alice_client, bob_client):
    with trio.fail_after(2):
        async with alice.events.register_topic_sent.subscribe_and_wait():
            async with bob.events.register_topic_received.subscribe_and_wait():
                await alice_client.send_register_topic(
                    bob.node_id,
                    bob.endpoint,
                    topic=b"unicornsrainbowsunicornsrainbows",
                    enr=alice.enr,
                    ticket=b"test-ticket",
                    request_id=b"\x12",
                )


@pytest.mark.trio
async def test_client_send_ticket(alice, bob, alice_client, bob_client):
    with trio.fail_after(2):
        async with alice.events.ticket_sent.subscribe_and_wait():
            async with bob.events.ticket_received.subscribe_and_wait():
                await alice_client.send_ticket(
                    bob.node_id,
                    bob.endpoint,
                    ticket=b"test-ticket",
                    wait_time=600,
                    request_id=b"\x12",
                )


@pytest.mark.trio
async def test_client_send_registration_confirmation(
    alice, bob, alice_client, bob_client
):
    with trio.fail_after(2):
        async with alice.events.registration_confirmation_sent.subscribe_and_wait():
            async with bob.events.registration_confirmation_received.subscribe_and_wait():
                await alice_client.send_registration_confirmation(
                    bob.node_id,
                    bob.endpoint,
                    topic=b"unicornsrainbowsunicornsrainbows",
                    request_id=b"\x12",
                )


@pytest.mark.trio
async def test_client_send_topic_query(alice, bob, alice_client, bob_client):
    with trio.fail_after(2):
        async with alice.events.topic_query_sent.subscribe_and_wait():
            async with bob.events.topic_query_received.subscribe_and_wait():
                await alice_client.send_topic_query(
                    bob.node_id,
                    bob.endpoint,
                    topic=b"unicornsrainbowsunicornsrainbows",
                    request_id=b"\x12",
                )


#
# Request/Response
#
@pytest.mark.trio
async def test_client_request_response_ping_pong(alice, bob, alice_client, bob_client):
    async with bob.events.ping_received.subscribe() as subscription:
        async with trio.open_nursery() as nursery:

            async def _send_response():
                ping = await subscription.receive()
                await bob_client.send_pong(
                    alice.node_id, alice.endpoint, request_id=ping.message.request_id,
                )

            nursery.start_soon(_send_response)

            with trio.fail_after(2):
                pong = await alice_client.ping(bob.node_id, bob.endpoint)
                assert pong.message.enr_seq == bob.enr.sequence_number
                assert pong.message.packet_ip == alice.endpoint.ip_address
                assert pong.message.packet_port == alice.endpoint.port


@pytest.mark.trio
async def test_client_ping_timeout(alice, bob_client, autojump_clock):
    with trio.fail_after(60):
        with pytest.raises(trio.TooSlowError):
            await bob_client.ping(alice.node_id, alice.endpoint)


@pytest.mark.trio
async def test_client_request_response_find_nodes_found_nodes(
    alice, bob, alice_client, bob_client
):
    table = KademliaRoutingTable(bob.node_id, 256)
    for i in range(1000):
        enr = ENRFactory()
        table.update(enr.node_id)
        bob.enr_db.set_enr(enr)

    checked_bucket_indexes = []

    for distance in range(256, 1, -1):
        bucket = table.buckets[distance - 1]
        if not len(bucket):
            break

        async with trio.open_nursery() as nursery:
            async with bob.events.find_nodes_received.subscribe() as subscription:
                expected_enrs = tuple(bob.enr_db.get_enr(node_id) for node_id in bucket)

                async def _send_response():
                    find_nodes = await subscription.receive()
                    checked_bucket_indexes.append(distance)
                    await bob_client.send_found_nodes(
                        alice.node_id,
                        alice.endpoint,
                        enrs=expected_enrs,
                        request_id=find_nodes.message.request_id,
                    )

                nursery.start_soon(_send_response)

                with trio.fail_after(2):
                    found_nodes_messages = await alice_client.find_nodes(
                        bob.node_id, bob.endpoint, distances=[distance],
                    )
                    found_node_ids = {
                        enr.node_id
                        for message in found_nodes_messages
                        for enr in message.message.enrs
                    }
                    expected_node_ids = {enr.node_id for enr in expected_enrs}
                    assert found_node_ids == expected_node_ids

    assert len(checked_bucket_indexes) > 4


@pytest.mark.trio
async def test_client_request_response_find_nodes_invalid_total(
    alice, bob, alice_client, bob_client
):
    async with trio.open_nursery() as nursery:
        async with bob_client.dispatcher.subscribe(FindNodeMessage) as subscription:

            async def _respond():
                request = await subscription.receive()
                message = request.to_response(
                    FoundNodesMessage(request.request_id, 0, ())
                )
                await bob_client.dispatcher.send_message(message)

            nursery.start_soon(_respond)

            with trio.fail_after(2):
                with pytest.raises(ValidationError, match="total=0"):
                    await alice_client.find_nodes(
                        bob.node_id, bob.endpoint, distances=[123],
                    )


@pytest.mark.trio
async def test_client_request_response_stream_find_nodes_found_nodes(
    alice, bob, alice_client, bob_client
):
    enrs = tuple(ENRFactory() for _ in range(FOUND_NODES_MAX_PAYLOAD_SIZE + 1))
    distances = set([compute_log_distance(enr.node_id, bob.node_id) for enr in enrs])

    async with trio.open_nursery() as nursery:
        async with bob.events.find_nodes_received.subscribe() as subscription:

            async def _send_response():
                find_nodes = await subscription.receive()
                await bob_client.send_found_nodes(
                    alice.node_id,
                    alice.endpoint,
                    enrs=enrs,
                    request_id=find_nodes.message.request_id,
                )

            nursery.start_soon(_send_response)

            with trio.fail_after(2):
                async with alice_client.stream_find_nodes(
                    bob.node_id, bob.endpoint, distances=distances
                ) as resp_aiter:
                    found_nodes_messages = tuple([resp async for resp in resp_aiter])
            found_node_ids = {
                enr.node_id
                for message in found_nodes_messages
                for enr in message.message.enrs
            }
            expected_node_ids = {enr.node_id for enr in enrs}
            assert found_node_ids == expected_node_ids

            expected_total = len(
                partition_enrs(enrs, max_payload_size=FOUND_NODES_MAX_PAYLOAD_SIZE)
            )
            assert set((msg.message.total) for msg in found_nodes_messages) == set(
                [expected_total]
            )

            nursery.cancel_scope.cancel()


@pytest.mark.trio
async def test_client_request_response_stream_find_nodes_handles_premature_exit(
    alice, bob, alice_client, bob_client, autojump_clock
):
    enrs = tuple(ENRFactory() for _ in range(FOUND_NODES_MAX_PAYLOAD_SIZE + 1))

    async with trio.open_nursery() as nursery:
        async with bob.events.find_nodes_received.subscribe() as subscription:
            enr_batches = partition_enrs(
                enrs, max_payload_size=FOUND_NODES_MAX_PAYLOAD_SIZE
            )
            first_enr_batch = enr_batches[0]
            distances = set(
                [
                    compute_log_distance(enr.node_id, bob.node_id)
                    for enr in first_enr_batch
                ]
            )
            num_batches = len(enr_batches)

            async def _respond():
                find_nodes = await subscription.receive()
                assert num_batches > 1
                message = AnyOutboundMessage(
                    FoundNodesMessage(
                        find_nodes.message.request_id, num_batches, first_enr_batch,
                    ),
                    alice.endpoint,
                    alice.node_id,
                )
                await bob_client.dispatcher.send_message(message)

            nursery.start_soon(_respond)

            found_nodes_messages = []
            with trio.fail_after(REQUEST_RESPONSE_TIMEOUT + 1):
                async with alice_client.stream_find_nodes(
                    bob.node_id, bob.endpoint, distances=distances
                ) as resp_aiter:
                    async for resp in resp_aiter:
                        found_nodes_messages.append(resp)
                        # prematurely close the context after first response
                        await resp_aiter.aclose()

            assert len(found_nodes_messages) == 1

            nursery.cancel_scope.cancel()


@pytest.mark.trio
async def test_client_request_response_stream_find_nodes_timeout_with_incomplete_response(
    alice, bob, alice_client, bob_client, autojump_clock
):
    enrs = tuple(ENRFactory() for _ in range(FOUND_NODES_MAX_PAYLOAD_SIZE + 1))

    async with trio.open_nursery() as nursery:
        async with bob.events.find_nodes_received.subscribe() as subscription:
            enr_batches = partition_enrs(
                enrs, max_payload_size=FOUND_NODES_MAX_PAYLOAD_SIZE
            )
            first_enr_batch = enr_batches[0]
            distances = set(
                [
                    compute_log_distance(enr.node_id, bob.node_id)
                    for enr in first_enr_batch
                ]
            )
            num_batches = len(enr_batches)

            async def _respond():
                find_nodes = await subscription.receive()
                assert num_batches > 1
                message = AnyOutboundMessage(
                    FoundNodesMessage(
                        find_nodes.message.request_id, num_batches, first_enr_batch,
                    ),
                    alice.endpoint,
                    alice.node_id,
                )
                # only send first batch of responses, then nothing
                await bob_client.dispatcher.send_message(message)

            nursery.start_soon(_respond)

            with trio.fail_after(REQUEST_RESPONSE_TIMEOUT + 1):
                with pytest.raises(trio.TooSlowError):
                    async with alice_client.stream_find_nodes(
                        bob.node_id, bob.endpoint, distances=distances
                    ) as resp_aiter:
                        found_nodes_messages = tuple(
                            [resp async for resp in resp_aiter]
                        )

            assert len(found_nodes_messages) == 1

            nursery.cancel_scope.cancel()


@pytest.mark.trio
async def test_client_request_response_stream_find_nodes_catches_invalid_distances(
    alice, bob, alice_client, bob_client
):
    enrs = tuple(ENRFactory() for _ in range(50))

    async with trio.open_nursery() as nursery:
        async with bob_client.dispatcher.subscribe(FindNodeMessage) as subscription:
            invalid_enrs = tuple(
                enr
                for enr in enrs
                if compute_log_distance(enr.node_id, bob.node_id) > 100
            )
            enr_batches = partition_enrs(
                invalid_enrs, max_payload_size=FOUND_NODES_MAX_PAYLOAD_SIZE
            )
            num_batches = len(enr_batches)

            async def _respond():
                request = await subscription.receive()
                message = AnyOutboundMessage(
                    FoundNodesMessage(
                        request.message.request_id, num_batches, enr_batches[0]
                    ),
                    alice.endpoint,
                    alice.node_id,
                )
                await bob_client.dispatcher.send_message(message)

            nursery.start_soon(_respond)

            with pytest.raises(ValidationError, match="Invalid response: distance"):
                async with alice_client.stream_find_nodes(
                    bob.node_id, bob.endpoint, distances=[100]
                ) as resp_aiter:
                    tuple([resp async for resp in resp_aiter])

            nursery.cancel_scope.cancel()


@pytest.mark.trio
async def test_client_request_response_stream_find_nodes_catches_zero_distances(
    alice, bob, alice_client, bob_client
):
    enrs = [
        bob.enr,
    ]

    async with trio.open_nursery() as nursery:
        async with bob_client.dispatcher.subscribe(FindNodeMessage) as subscription:

            async def _respond():
                request = await subscription.receive()
                message = AnyOutboundMessage(
                    FoundNodesMessage(request.message.request_id, 1, enrs),
                    alice.endpoint,
                    alice.node_id,
                )
                await bob_client.dispatcher.send_message(message)

            nursery.start_soon(_respond)

            with pytest.raises(ValidationError, match="Invalid response: distance=0"):
                async with alice_client.stream_find_nodes(
                    bob.node_id, bob.endpoint, distances=[100]
                ) as resp_aiter:
                    tuple([resp async for resp in resp_aiter])

            nursery.cancel_scope.cancel()


@pytest.mark.trio
async def test_client_request_response_stream_find_nodes_catches_invalid_response_total(
    alice, bob, alice_client, bob_client
):
    enrs = tuple(ENRFactory() for _ in range(10))

    async with trio.open_nursery() as nursery:
        async with bob_client.dispatcher.subscribe(FindNodeMessage) as subscription:

            async def _respond():
                request = await subscription.receive()
                message = AnyOutboundMessage(
                    FoundNodesMessage(request.message.request_id, 0, enrs),
                    alice.endpoint,
                    alice.node_id,
                )
                await bob_client.dispatcher.send_message(message)

            nursery.start_soon(_respond)

            with pytest.raises(
                ValidationError, match="Invalid `total` counter in response: total=0"
            ):
                async with alice_client.stream_find_nodes(
                    bob.node_id, bob.endpoint, distances=[256]
                ) as resp_aiter:
                    tuple([resp async for resp in resp_aiter])

            nursery.cancel_scope.cancel()


@pytest.mark.trio
async def test_client_request_response_stream_find_nodes_inconsistent_message_total(
    alice, bob, alice_client, bob_client
):
    enrs = tuple(ENRFactory() for _ in range(FOUND_NODES_MAX_PAYLOAD_SIZE + 1))
    distances = set([compute_log_distance(enr.node_id, bob.node_id) for enr in enrs])

    async with trio.open_nursery() as nursery:
        async with bob_client.dispatcher.subscribe(FindNodeMessage) as subscription:
            enr_batches = partition_enrs(
                enrs, max_payload_size=FOUND_NODES_MAX_PAYLOAD_SIZE
            )
            num_batches = len(enr_batches)

            async def _respond():
                request = await subscription.receive()
                assert len(enr_batches) > 1

                head_message = AnyOutboundMessage(
                    FoundNodesMessage(
                        request.message.request_id, num_batches, enr_batches[0],
                    ),
                    alice.endpoint,
                    alice.node_id,
                )
                await bob_client.dispatcher.send_message(head_message)

                invalid_message = AnyOutboundMessage(
                    FoundNodesMessage(
                        request.message.request_id, num_batches + 1, enr_batches[1],
                    ),
                    alice.endpoint,
                    alice.node_id,
                )
                await bob_client.dispatcher.send_message(invalid_message)

            nursery.start_soon(_respond)

            with pytest.raises(ValidationError, match="Inconsistent message total"):
                async with alice_client.stream_find_nodes(
                    bob.node_id, bob.endpoint, distances=distances
                ) as resp_aiter:
                    tuple([resp async for resp in resp_aiter])

            nursery.cancel_scope.cancel()


@pytest.mark.trio
async def test_client_talk_request_response(alice, bob, alice_client, bob_client):
    async def _do_talk_response(client):
        async with client.dispatcher.subscribe(TalkRequestMessage) as subscription:
            request = await subscription.receive()
            await client.send_talk_response(
                request.sender_node_id,
                request.sender_endpoint,
                payload=b"talk-response",
                request_id=request.message.request_id,
            )

    with trio.fail_after(2):
        async with AsyncExitStack() as stack:
            await stack.enter_async_context(
                alice.events.talk_request_sent.subscribe_and_wait()
            )
            await stack.enter_async_context(
                alice.events.talk_response_received.subscribe_and_wait()
            )
            await stack.enter_async_context(
                bob.events.talk_request_received.subscribe_and_wait()
            )
            await stack.enter_async_context(
                bob.events.talk_response_sent.subscribe_and_wait()
            )
            async with trio.open_nursery() as nursery:
                nursery.start_soon(_do_talk_response, bob_client)
                response = await alice_client.talk(
                    bob.node_id,
                    bob.endpoint,
                    protocol=b"test-talk-proto",
                    payload=b"test-request",
                )
            assert response.message.payload == b"talk-response"


@pytest.mark.trio
async def test_client_talk_request_response_timeout(alice, bob_client, autojump_clock):
    with trio.fail_after(60):
        with pytest.raises(trio.TooSlowError):
            await bob_client.talk(
                alice.node_id,
                alice.endpoint,
                protocol=b"test",
                payload=b"test-request",
            )


@settings(deadline=1000)
@given(datagram_bytes=st.binary(max_size=1024))
@pytest.mark.trio
async def test_client_handles_malformed_datagrams(tester, datagram_bytes):
    bob = tester.node()
    async with bob.client() as bob_client:
        alice = tester.node()
        alice.enr_db.set_enr(bob.enr)
        bob.enr_db.set_enr(alice.enr)

        async with alice.client():
            async with alice.events.ping_received.subscribe() as subscription:
                await bob_client._outbound_datagram_send_channel.send(
                    OutboundDatagram(datagram_bytes, alice.endpoint)
                )

                # Give the node a minute to crash if it's going to crash
                for _ in range(100):
                    await trio.lowlevel.checkpoint()

                request_id = await bob_client.send_ping(alice.node_id, alice.endpoint)

                # Now fetch the ping message that still should have come through.
                with trio.fail_after(1):
                    ping = await subscription.receive()
                assert ping.message.request_id == request_id
