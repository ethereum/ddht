import itertools

from eth_enr.tools.factories import ENRFactory
import pytest
import trio

from ddht.kademlia import KademliaRoutingTable
from ddht.v5_1.alexandria.advertisements import partition_advertisements
from ddht.v5_1.alexandria.constants import MAX_PAYLOAD_SIZE
from ddht.v5_1.alexandria.messages import (
    AckMessage,
    AdvertiseMessage,
    ContentMessage,
    FindNodesMessage,
    GetContentMessage,
    PingMessage,
    PongMessage,
    decode_message,
)
from ddht.v5_1.alexandria.payloads import Advertisement
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
                await alice_alexandria_client.ping(bob.node_id, bob.endpoint)


@pytest.mark.trio
async def test_alexandria_client_send_ping(bob, bob_network, alice_alexandria_client):
    async with bob_network.dispatcher.subscribe(TalkRequestMessage) as subscription:
        await alice_alexandria_client.send_ping(bob.node_id, bob.endpoint, enr_seq=100)
        with trio.fail_after(1):
            talk_response = await subscription.receive()
        message = decode_message(talk_response.message.payload)
        assert isinstance(message, PingMessage)
        assert message.payload.enr_seq == 100


@pytest.mark.trio
async def test_alexandria_client_send_pong(bob, bob_network, alice_alexandria_client):
    async with bob_network.dispatcher.subscribe(TalkResponseMessage) as subscription:
        await alice_alexandria_client.send_pong(
            bob.node_id, bob.endpoint, enr_seq=100, request_id=b"\x01\x02"
        )
        with trio.fail_after(1):
            talk_response = await subscription.receive()
        message = decode_message(talk_response.message.payload)
        assert isinstance(message, PongMessage)
        assert message.payload.enr_seq == 100


@pytest.mark.trio
async def test_alexandria_client_ping_request_response(
    alice, bob, bob_network, alice_alexandria_client, bob_alexandria_client,
):
    async with bob_network.dispatcher.subscribe(TalkRequestMessage) as subscription:

        async def _respond():
            request = await subscription.receive()
            await bob_alexandria_client.send_pong(
                alice.node_id,
                alice.endpoint,
                enr_seq=bob.enr.sequence_number,
                request_id=request.request_id,
            )

        async with trio.open_nursery() as nursery:
            nursery.start_soon(_respond)

            with trio.fail_after(1):
                pong_message = await alice_alexandria_client.ping(
                    bob.node_id, bob.endpoint
                )
                assert isinstance(pong_message, PongMessage)
                assert pong_message.payload.enr_seq == bob.enr.sequence_number


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
            await subscription.receive()
            await bob_alexandria_client.send_pong(
                alice.node_id,
                alice.endpoint,
                enr_seq=bob.enr.sequence_number,
                request_id=alice_network.client.request_tracker.get_free_request_id(
                    bob.node_id
                ),
            )

        async with trio.open_nursery() as nursery:
            nursery.start_soon(_respond_wrong_request_id)

            with pytest.raises(trio.TooSlowError):
                with trio.fail_after(1):
                    await alice_alexandria_client.ping(bob.node_id, bob.endpoint)


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
async def test_alexandria_client_send_get_content(
    bob, bob_network, alice_alexandria_client
):
    content_key = b"test-content-key"
    start_chunk_index = 5
    max_chunks = 16

    async with bob_network.dispatcher.subscribe(TalkRequestMessage) as subscription:
        await alice_alexandria_client.send_get_content(
            bob.node_id,
            bob.endpoint,
            content_key=content_key,
            start_chunk_index=start_chunk_index,
            max_chunks=max_chunks,
        )
        with trio.fail_after(1):
            talk_response = await subscription.receive()
        message = decode_message(talk_response.message.payload)
        assert isinstance(message, GetContentMessage)
        assert message.payload.content_key == content_key
        assert message.payload.start_chunk_index == start_chunk_index
        assert message.payload.max_chunks == max_chunks


@pytest.mark.trio
async def test_alexandria_client_send_content(
    bob, bob_network, alice_alexandria_client
):

    async with bob_network.dispatcher.subscribe(TalkResponseMessage) as subscription:
        await alice_alexandria_client.send_content(
            bob.node_id,
            bob.endpoint,
            is_proof=True,
            payload=b"test",
            request_id=b"\x01",
        )
        with trio.fail_after(1):
            talk_response = await subscription.receive()
        message = decode_message(talk_response.message.payload)
        assert isinstance(message, ContentMessage)
        assert message.payload.is_proof is True
        assert message.payload.payload == b"test"


@pytest.mark.trio
async def test_alexandria_client_get_content(
    alice, bob, bob_network, alice_alexandria_client, bob_alexandria_client,
):
    content_key = b"test-content-key"

    async with bob_network.dispatcher.subscribe(TalkRequestMessage) as subscription:

        async def _respond():
            request = await subscription.receive()
            await bob_alexandria_client.send_content(
                request.sender_node_id,
                request.sender_endpoint,
                is_proof=False,
                payload=b"test",
                request_id=request.request_id,
            )

        async with trio.open_nursery() as nursery:
            nursery.start_soon(_respond)

            with trio.fail_after(1):
                content_message = await alice_alexandria_client.get_content(
                    bob.node_id,
                    bob.endpoint,
                    content_key=content_key,
                    start_chunk_index=0,
                    max_chunks=1,
                )

                assert isinstance(content_message, ContentMessage)
                assert content_message.payload.is_proof is False
                assert content_message.payload.payload == b"test"


@pytest.mark.trio
async def test_alexandria_client_send_advertisements(
    alice, bob, bob_network, alice_alexandria_client
):
    advertisements = (
        Advertisement.create(
            content_key=b"\x01testkey",
            hash_tree_root=b"\x12" * 32,
            private_key=alice.private_key,
        ),
    )
    async with bob_network.dispatcher.subscribe(TalkRequestMessage) as subscription:
        await alice_alexandria_client.send_advertisements(
            bob.node_id, bob.endpoint, advertisements=advertisements,
        )
        with trio.fail_after(1):
            talk_response = await subscription.receive()
        message = decode_message(talk_response.message.payload)
        assert isinstance(message, AdvertiseMessage)
        assert message.payload == advertisements


@pytest.mark.trio
async def test_alexandria_client_send_ack(bob, bob_network, alice_alexandria_client):
    async with bob_network.dispatcher.subscribe(TalkResponseMessage) as subscription:
        await alice_alexandria_client.send_ack(
            bob.node_id, bob.endpoint, advertisement_radius=12345, request_id=b"\x01",
        )
        with trio.fail_after(1):
            talk_response = await subscription.receive()
        message = decode_message(talk_response.message.payload)
        assert isinstance(message, AckMessage)
        assert message.payload.advertisement_radius == 12345


@pytest.mark.trio
async def test_alexandria_client_advertise_single_message(
    alice, bob, bob_network, bob_alexandria_client, alice_alexandria_client
):
    advertisements = (
        Advertisement.create(
            content_key=b"\x01testkey",
            hash_tree_root=b"\x12" * 32,
            private_key=alice.private_key,
        ),
    )

    async with bob_network.dispatcher.subscribe(TalkRequestMessage) as subscription:

        async def _respond():
            request = await subscription.receive()
            await bob_alexandria_client.send_ack(
                request.sender_node_id,
                request.sender_endpoint,
                advertisement_radius=12345,
                request_id=request.request_id,
            )

        async with trio.open_nursery() as nursery:
            nursery.start_soon(_respond)

            with trio.fail_after(1):
                ack_messages = await alice_alexandria_client.advertise(
                    bob.node_id, bob.endpoint, advertisements=advertisements,
                )
                assert len(ack_messages) == 1
                ack_message = ack_messages[0]

                assert isinstance(ack_message, AckMessage)
                assert ack_message.payload.advertisement_radius == 12345


@pytest.mark.trio
async def test_alexandria_client_advertise_muliple_messages(
    alice, bob, bob_network, bob_alexandria_client, alice_alexandria_client
):
    advertisements = tuple(
        Advertisement.create(
            content_key=b"\x01testkey",
            hash_tree_root=b"\x12" * 32,
            private_key=alice.private_key,
        )
        for _ in range(10)
    )
    expected_message_count = len(
        partition_advertisements(advertisements, max_payload_size=MAX_PAYLOAD_SIZE,)
    )

    async with bob_network.dispatcher.subscribe(TalkRequestMessage) as subscription:

        async def _respond():
            for _ in range(expected_message_count):
                request = await subscription.receive()
                await bob_alexandria_client.send_ack(
                    request.sender_node_id,
                    request.sender_endpoint,
                    advertisement_radius=12345,
                    request_id=request.request_id,
                )

        async with trio.open_nursery() as nursery:
            nursery.start_soon(_respond)

            with trio.fail_after(1):
                ack_messages = await alice_alexandria_client.advertise(
                    bob.node_id, bob.endpoint, advertisements=advertisements,
                )
                assert len(ack_messages) == expected_message_count
                ack_message = ack_messages[0]

                assert isinstance(ack_message, AckMessage)
                assert ack_message.payload.advertisement_radius == 12345


@pytest.mark.trio
async def test_alexandria_client_advertise_timeout(
    alice, bob, alice_alexandria_client, autojump_clock,
):
    advertisements = (
        Advertisement.create(
            content_key=b"\x01testkey",
            hash_tree_root=b"\x12" * 32,
            private_key=alice.private_key,
        ),
    )

    with pytest.raises(trio.TooSlowError):
        await alice_alexandria_client.advertise(
            bob.node_id, bob.endpoint, advertisements=advertisements,
        )
