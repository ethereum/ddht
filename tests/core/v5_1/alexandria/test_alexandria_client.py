import itertools

from eth_enr.tools.factories import ENRFactory
import pytest
import trio

from ddht.kademlia import KademliaRoutingTable
from ddht.v5_1.alexandria.constants import ALEXANDRIA_PROTOCOL_ID
from ddht.v5_1.alexandria.messages import (
    FindContentMessage,
    FindNodesMessage,
    FoundContentMessage,
    PingMessage,
    PongMessage,
    decode_message,
)
from ddht.v5_1.alexandria.payloads import PingPayload, PongPayload
from ddht.v5_1.exceptions import ProtocolNotSupported
from ddht.v5_1.messages import TalkRequestMessage, TalkResponseMessage


@pytest.mark.trio
async def test_alexandria_client_handles_empty_response(
    bob, bob_client, alice_alexandria_client
):
    async with bob_client.dispatcher.subscribe(TalkRequestMessage) as subscription:
        async with trio.open_nursery() as nursery:

            async def _respond_empty():
                request = await subscription.receive()
                await bob_client.send_talk_response(
                    request.sender_node_id,
                    request.sender_endpoint,
                    payload=b"",
                    request_id=request.request_id,
                )

            nursery.start_soon(_respond_empty)

            with pytest.raises(ProtocolNotSupported):
                await alice_alexandria_client.ping(
                    bob.node_id, bob.endpoint, enr_seq=0, advertisement_radius=1234,
                )


@pytest.mark.trio
async def test_request_ensures_valid_msg_type(bob, alice_alexandria_client):
    with pytest.raises(TypeError):
        await alice_alexandria_client._request(
            bob.node_id,
            bob.endpoint,
            PongMessage(PongPayload(enr_seq=1234, advertisement_radius=4321)),
            PongMessage,
            request_id=b"\x01\x02",
        )


@pytest.mark.trio
async def test_send_response_ensures_valid_msg_type(bob, alice_alexandria_client):
    with pytest.raises(TypeError):
        await alice_alexandria_client._send_response(
            bob.node_id,
            bob.endpoint,
            PingMessage(PingPayload(enr_seq=1234, advertisement_radius=4321)),
            request_id=b"\x01\x02",
        )


@pytest.mark.trio
async def test_feed_talk_requests_ignores_wrongly_typed_msgs(
    bob, bob_alexandria_client, alice_alexandria_client, autojump_clock,
):
    async with bob_alexandria_client.network.dispatcher.subscribe(
        TalkRequestMessage
    ) as sub:
        async with bob_alexandria_client.subscribe(PongMessage) as client_subscription:

            # Send a PongMessage via TALKREQ.
            msg = PongMessage(PongPayload(enr_seq=1234, advertisement_radius=4321))
            await alice_alexandria_client.network.client.send_talk_request(
                bob.node_id,
                bob.endpoint,
                protocol=ALEXANDRIA_PROTOCOL_ID,
                payload=msg.to_wire_bytes(),
                request_id=b"\x01\x02",
            )

            # The message will be received by bob.
            with trio.fail_after(1):
                inbound_msg = await sub.receive()

            assert inbound_msg.request_id == b"\x01\x02"

            # But the alexandria client will not deliver it to any subscribers. And it should
            # log a warning about it.
            with trio.move_on_after(0.5) as scope:
                await client_subscription.receive()

            assert scope.cancelled_caught


@pytest.mark.trio
async def test_feed_talk_responses_ignores_wrongly_typed_msgs(
    bob, bob_alexandria_client, alice_alexandria_client, autojump_clock,
):
    async with bob_alexandria_client.network.dispatcher.subscribe(
        TalkResponseMessage
    ) as sub:
        async with bob_alexandria_client.subscribe(PingMessage) as client_subscription:

            # Send a PingMessage via TALKRESP.
            msg = PingMessage(PingPayload(enr_seq=1234, advertisement_radius=4321))
            await alice_alexandria_client.network.client.send_talk_response(
                bob.node_id,
                bob.endpoint,
                payload=msg.to_wire_bytes(),
                request_id=b"\x01\x02",
            )

            # The message will be received by bob.
            with trio.fail_after(1):
                inbound_msg = await sub.receive()

            assert inbound_msg.request_id == b"\x01\x02"

            # But the alexandria client will not deliver it to any subscribers. And it should
            # log a warning about it.
            with trio.move_on_after(0.5) as scope:
                await client_subscription.receive()

            assert scope.cancelled_caught


@pytest.mark.trio
async def test_alexandria_client_send_ping(bob, bob_network, alice_alexandria_client):
    async with bob_network.dispatcher.subscribe(TalkRequestMessage) as subscription:
        await alice_alexandria_client.send_ping(
            bob.node_id,
            bob.endpoint,
            enr_seq=1234,
            advertisement_radius=4321,
            request_id=b"\x01\x02",
        )
        with trio.fail_after(1):
            inbound_msg = await subscription.receive()

        assert inbound_msg.request_id == b"\x01\x02"
        message = decode_message(inbound_msg.message.payload)
        assert isinstance(message, PingMessage)
        assert message.payload.enr_seq == 1234
        assert message.payload.advertisement_radius == 4321


@pytest.mark.trio
async def test_alexandria_client_send_pong(bob, bob_network, alice_alexandria_client):
    async with bob_network.dispatcher.subscribe(TalkResponseMessage) as subscription:
        await alice_alexandria_client.send_pong(
            bob.node_id,
            bob.endpoint,
            enr_seq=1234,
            advertisement_radius=4321,
            request_id=b"\x01\x02",
        )
        with trio.fail_after(1):
            talk_response = await subscription.receive()
        message = decode_message(talk_response.message.payload)
        assert isinstance(message, PongMessage)
        assert message.payload.enr_seq == 1234
        assert message.payload.advertisement_radius == 4321


@pytest.mark.trio
async def test_alexandria_client_ping_request_response(
    alice, bob, bob_network, alice_alexandria_client, bob_alexandria_client,
):
    async with bob_alexandria_client.subscribe(PingMessage) as subscription:

        async def _respond():
            request = await subscription.receive()

            assert request.message.payload.enr_seq == 6789
            assert request.message.payload.advertisement_radius == 9876
            assert request.request_id == b"\x01\x02"

            await bob_alexandria_client.send_pong(
                request.sender_node_id,
                request.sender_endpoint,
                enr_seq=1234,
                advertisement_radius=4321,
                request_id=request.request_id,
            )

        async with trio.open_nursery() as nursery:
            nursery.start_soon(_respond)

            with trio.fail_after(1):
                pong_message = await alice_alexandria_client.ping(
                    bob.node_id,
                    bob.endpoint,
                    enr_seq=6789,
                    advertisement_radius=9876,
                    request_id=b"\x01\x02",
                )
                assert isinstance(pong_message, PongMessage)
                assert pong_message.payload.enr_seq == 1234
                assert pong_message.payload.advertisement_radius == 4321


@pytest.mark.trio
async def test_alexandria_api_ping_request_response_request_id_mismatch(
    alice,
    bob,
    alice_network,
    bob_network,
    alice_alexandria_client,
    bob_alexandria_client,
    autojump_clock,
):
    async with bob_network.dispatcher.subscribe(TalkRequestMessage) as subscription:

        async def _respond_wrong_request_id():
            request = await subscription.receive()
            await bob_alexandria_client.send_pong(
                request.sender_node_id,
                request.sender_endpoint,
                enr_seq=1234,
                advertisement_radius=4321,
                request_id=alice_network.client.request_tracker.get_free_request_id(
                    bob.node_id
                ),
            )

        async with trio.open_nursery() as nursery:
            nursery.start_soon(_respond_wrong_request_id)

            with pytest.raises(trio.TooSlowError):
                with trio.fail_after(1):
                    await alice_alexandria_client.ping(
                        bob.node_id, bob.endpoint, enr_seq=0, advertisement_radius=1234,
                    )


@pytest.mark.trio
async def test_alexandria_client_send_find_nodes(
    alice, bob, bob_client, alice_alexandria_client
):
    async with bob_client.dispatcher.subscribe(TalkRequestMessage) as subscription:
        await alice_alexandria_client.send_find_nodes(
            bob.node_id, bob.endpoint, distances=(0, 255, 254),
        )
        with trio.fail_after(1):
            talk_request = await subscription.receive()
        message = decode_message(talk_request.message.payload)
        assert isinstance(message, FindNodesMessage)
        assert message.payload.distances == (0, 255, 254)


@pytest.mark.trio
async def test_alexandria_client_find_nodes_timeout(
    bob, alice_alexandria_client, autojump_clock,
):
    with pytest.raises(trio.TooSlowError):
        await alice_alexandria_client.find_nodes(
            bob.node_id, bob.endpoint, distances=(0, 255, 254),
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
async def test_alexandria_client_send_found_nodes(
    bob, bob_client, enrs, alice_alexandria_client
):
    with trio.fail_after(2):
        async with bob_client.dispatcher.subscribe(TalkResponseMessage) as subscription:
            await alice_alexandria_client.send_found_nodes(
                bob.node_id, bob.endpoint, enrs=enrs, request_id=b"\x01\x02",
            )

            with trio.fail_after(2):
                first_message = await subscription.receive()
                decoded_first_message = decode_message(first_message.message.payload)
                remaining_messages = []
                for _ in range(decoded_first_message.payload.total - 1):
                    remaining_messages.append(await subscription.receive())

        all_decoded_messages = (decoded_first_message,) + tuple(
            decode_message(message.message.payload) for message in remaining_messages
        )
        all_received_enrs = tuple(
            itertools.chain(*(message.payload.enrs for message in all_decoded_messages))
        )
        expected_enrs_by_node_id = {enr.node_id: enr for enr in enrs}
        assert len(expected_enrs_by_node_id) == len(enrs)
        actual_enrs_by_node_id = {enr.node_id: enr for enr in all_received_enrs}

        assert expected_enrs_by_node_id == actual_enrs_by_node_id


@pytest.mark.trio
async def test_client_request_response_find_nodes_found_nodes(
    alice, bob, alice_alexandria_client, bob_alexandria_client,
):
    table = KademliaRoutingTable(bob.node_id, 256)
    for i in range(1000):
        enr = ENRFactory()
        table.update(enr.node_id)
        bob.enr_db.set_enr(enr)

    checked_bucket_indexes = []

    for distance in range(256, 0, -1):
        bucket = table.buckets[distance - 1]
        if not len(bucket):
            break

        async with trio.open_nursery() as nursery:
            async with bob_alexandria_client.subscribe(
                FindNodesMessage
            ) as subscription:
                expected_enrs = tuple(bob.enr_db.get_enr(node_id) for node_id in bucket)

                async def _send_response():
                    request = await subscription.receive()
                    checked_bucket_indexes.append(distance)
                    await bob_alexandria_client.send_found_nodes(
                        alice.node_id,
                        alice.endpoint,
                        enrs=expected_enrs,
                        request_id=request.request_id,
                    )

                nursery.start_soon(_send_response)

                with trio.fail_after(2):
                    found_nodes_messages = await alice_alexandria_client.find_nodes(
                        bob.node_id, bob.endpoint, distances=[distance],
                    )

                found_node_ids = {
                    enr.node_id
                    for message in found_nodes_messages
                    for enr in message.message.payload.enrs
                }
                expected_node_ids = {enr.node_id for enr in expected_enrs}
                assert found_node_ids == expected_node_ids

    assert len(checked_bucket_indexes) > 4


@pytest.mark.trio
async def test_alexandria_client_send_find_content(
    bob, bob_network, alice_alexandria_client
):
    content_key = b"test-content-key"

    async with bob_network.dispatcher.subscribe(TalkRequestMessage) as subscription:
        await alice_alexandria_client.send_find_content(
            bob.node_id, bob.endpoint, content_key=content_key,
        )
        with trio.fail_after(1):
            talk_response = await subscription.receive()
        message = decode_message(talk_response.message.payload)
        assert isinstance(message, FindContentMessage)
        assert message.payload.content_key == content_key


@pytest.mark.trio
async def test_alexandria_client_send_found_content(
    bob, bob_network, alice_alexandria_client
):

    async with bob_network.dispatcher.subscribe(TalkResponseMessage) as subscription:
        await alice_alexandria_client.send_found_content(
            bob.node_id,
            bob.endpoint,
            enrs=None,
            content=b"test-payload",
            request_id=b"\x01",
        )
        with trio.fail_after(1):
            talk_response = await subscription.receive()
        message = decode_message(talk_response.message.payload)
        assert isinstance(message, FoundContentMessage)
        assert message.payload.content == b"test-payload"
        assert message.payload.enrs == ()
