from contextlib import AsyncExitStack

from eth_enr.tools.factories import ENRFactory
from eth_utils import ValidationError
import pytest
import trio

from ddht.constants import ROUTING_TABLE_BUCKET_SIZE
from ddht.kademlia import (
    KademliaRoutingTable,
    at_log_distance,
    compute_distance,
    compute_log_distance,
)
from ddht.tools.factories.alexandria import AdvertisementFactory
from ddht.tools.factories.content import ContentFactory
from ddht.v5_1.alexandria.advertisements import Advertisement, partition_advertisements
from ddht.v5_1.alexandria.constants import MAX_PAYLOAD_SIZE
from ddht.v5_1.alexandria.content import compute_content_distance
from ddht.v5_1.alexandria.messages import (
    AdvertiseMessage,
    FindNodesMessage,
    GetContentMessage,
    LocateMessage,
)
from ddht.v5_1.alexandria.partials.proof import compute_proof, validate_proof
from ddht.v5_1.alexandria.payloads import AckPayload
from ddht.v5_1.alexandria.sedes import content_sedes


@pytest.mark.trio
async def test_alexandria_network_ping_api(
    alice, bob, alice_alexandria_network, bob_alexandria_network
):
    with trio.fail_after(2):
        pong = await alice_alexandria_network.ping(bob.node_id)

    assert pong.enr_seq == bob.enr.sequence_number


@pytest.mark.trio
async def test_alexandria_network_responds_to_pings(
    alice, bob, alice_alexandria_network, bob_alexandria_network
):
    with trio.fail_after(2):
        pong_message = await alice_alexandria_network.client.ping(
            bob.node_id, bob.endpoint, enr_seq=0, advertisement_radius=1,
        )

    assert pong_message.payload.enr_seq == bob.enr.sequence_number
    assert (
        pong_message.payload.advertisement_radius
        == bob_alexandria_network.local_advertisement_radius
    )


@pytest.mark.trio
async def test_alexandria_network_find_nodes_api(
    alice, bob, alice_alexandria_network, bob_alexandria_client
):
    distances = {0}

    bob_alexandria_routing_table = KademliaRoutingTable(
        bob.enr.node_id, ROUTING_TABLE_BUCKET_SIZE
    )

    for _ in range(200):
        enr = ENRFactory()
        bob.enr_db.set_enr(enr)
        bob_alexandria_routing_table.update(enr.node_id)
        distances.add(compute_log_distance(enr.node_id, bob.node_id))
        if distances.issuperset({0, 256, 255}):
            break
    else:
        raise Exception("failed")

    async with bob_alexandria_client.subscribe(FindNodesMessage) as subscription:
        async with trio.open_nursery() as nursery:

            async def _respond():
                request = await subscription.receive()
                response_enrs = []
                for distance in request.message.payload.distances:
                    if distance == 0:
                        response_enrs.append(bob.enr)
                    else:
                        for (
                            node_id
                        ) in bob_alexandria_routing_table.get_nodes_at_log_distance(
                            distance
                        ):
                            response_enrs.append(bob.enr_db.get_enr(node_id))
                await bob_alexandria_client.send_found_nodes(
                    request.sender_node_id,
                    request.sender_endpoint,
                    enrs=response_enrs,
                    request_id=request.request_id,
                )

            nursery.start_soon(_respond)

            with trio.fail_after(2):
                enrs = await alice_alexandria_network.find_nodes(
                    bob.node_id, 0, 255, 256
                )

    assert any(enr.node_id == bob.node_id for enr in enrs)
    response_distances = {
        compute_log_distance(enr.node_id, bob.node_id)
        for enr in enrs
        if enr.node_id != bob.node_id
    }
    assert response_distances == {256, 255}


@pytest.mark.trio
async def test_alexandria_network_responds_to_find_nodes(
    alice, bob, alice_alexandria_network, bob_alexandria_network,
):
    enr = ENRFactory()
    bob.enr_db.set_enr(enr)
    bob_alexandria_network.routing_table.update(enr.node_id)
    enr_distance = compute_log_distance(enr.node_id, bob.node_id)

    with trio.fail_after(2):
        enrs = await alice_alexandria_network.find_nodes(bob.node_id, enr_distance,)

    assert len(enrs) >= 1
    assert any(enr.node_id == enr.node_id for enr in enrs)


@pytest.mark.trio
async def test_alexandria_network_advertise_single_message(
    alice, bob, bob_network, bob_alexandria_client, alice_alexandria_network
):
    advertisements = (AdvertisementFactory(private_key=alice.private_key),)

    async with bob_alexandria_client.subscribe(AdvertiseMessage) as subscription:

        async def _respond():
            request = await subscription.receive()
            await bob_alexandria_client.send_ack(
                request.sender_node_id,
                request.sender_endpoint,
                advertisement_radius=12345,
                acked=(True,),
                request_id=request.request_id,
            )

        async with trio.open_nursery() as nursery:
            nursery.start_soon(_respond)

            with trio.fail_after(2):
                ack_payload = await alice_alexandria_network.advertise(
                    bob.node_id, advertisements=advertisements,
                )

            assert isinstance(ack_payload, AckPayload)
            assert ack_payload.advertisement_radius == 12345
            assert ack_payload.acked == (True,)


@pytest.mark.trio
async def test_alexandria_network_advertise_multi_message(
    alice, bob, bob_network, bob_alexandria_client, alice_alexandria_network
):
    advertisements = tuple(
        AdvertisementFactory(private_key=alice.private_key) for i in range(16)
    )
    assert len(partition_advertisements(advertisements, MAX_PAYLOAD_SIZE)) > 1
    async with bob_alexandria_client.subscribe(AdvertiseMessage) as subscription:

        async def _respond():
            async for request in subscription:
                await bob_alexandria_client.send_ack(
                    request.sender_node_id,
                    request.sender_endpoint,
                    advertisement_radius=12345,
                    acked=(True,) * len(request.message.payload),
                    request_id=request.request_id,
                )

        async with trio.open_nursery() as nursery:
            nursery.start_soon(_respond)

            with trio.fail_after(2):
                ack_payload = await alice_alexandria_network.advertise(
                    bob.node_id, advertisements=advertisements,
                )

            nursery.cancel_scope.cancel()

        assert isinstance(ack_payload, AckPayload)
        assert ack_payload.advertisement_radius == 12345
        assert ack_payload.acked == (True,) * 16


@pytest.mark.trio
async def test_alexandria_network_advertise_wrong_ack_count(
    alice, bob, bob_network, bob_alexandria_client, alice_alexandria_network
):
    advertisements = (AdvertisementFactory(private_key=alice.private_key),)

    async with bob_alexandria_client.subscribe(AdvertiseMessage) as subscription:

        async def _respond():
            request = await subscription.receive()
            await bob_alexandria_client.send_ack(
                request.sender_node_id,
                request.sender_endpoint,
                advertisement_radius=12345,
                acked=(True, False, True),  # wrong number
                request_id=request.request_id,
            )

        async with trio.open_nursery() as nursery:
            nursery.start_soon(_respond)

            with pytest.raises(ValidationError):
                with trio.fail_after(2):
                    await alice_alexandria_network.advertise(
                        bob.node_id, advertisements=advertisements,
                    )


@pytest.mark.trio
async def test_alexandria_network_advertise_invalid_signature(
    alice, bob, bob_network, bob_alexandria_client, alice_alexandria_network
):
    advertisement = AdvertisementFactory(
        signature_v=1, signature_r=1357924680, signature_s=2468013579,
    )
    assert not advertisement.is_valid

    with pytest.raises(Exception, match="invalid"):
        await alice_alexandria_network.advertise(
            bob.node_id, advertisements=(advertisement,),
        )


@pytest.mark.parametrize(
    "content_size", (512, 2048),
)
@pytest.mark.trio
async def test_alexandria_network_get_content_proof_api(
    alice, bob, alice_alexandria_network, bob_alexandria_client, content_size,
):
    content = ContentFactory(length=content_size)
    proof = compute_proof(content, sedes=content_sedes)

    async with bob_alexandria_client.subscribe(GetContentMessage) as subscription:
        async with trio.open_nursery() as nursery:

            async def _serve():
                request = await subscription.receive()
                if content_size > 1024:
                    partial = proof.to_partial(
                        request.message.payload.start_chunk_index * 32,
                        request.message.payload.max_chunks * 32,
                    )
                    payload = partial.serialize()
                    is_proof = True
                else:
                    payload = content
                    is_proof = False
                await bob_alexandria_client.send_content(
                    request.sender_node_id,
                    request.sender_endpoint,
                    is_proof=is_proof,
                    payload=payload,
                    request_id=request.request_id,
                )

            nursery.start_soon(_serve)

            with trio.fail_after(2):
                partial = await alice_alexandria_network.get_content_proof(
                    bob.node_id,
                    hash_tree_root=proof.get_hash_tree_root(),
                    content_key=b"test-content-key",
                    start_chunk_index=0,
                    max_chunks=16,
                )
                validate_proof(partial)
                partial_data = partial.get_proven_data()
                assert partial_data[0 : 16 * 32] == content[0 : 16 * 32]


@pytest.mark.trio
async def test_alexandria_network_advertise_multiple_messages(
    alice, bob, bob_network, bob_alexandria_client, alice_alexandria_network
):
    advertisements = tuple(
        AdvertisementFactory(private_key=alice.private_key) for _ in range(20)
    )
    num_messages = len(partition_advertisements(advertisements, MAX_PAYLOAD_SIZE))

    async with bob_alexandria_client.subscribe(AdvertiseMessage) as subscription:

        async def _respond():
            for _ in range(num_messages):
                request = await subscription.receive()
                await bob_alexandria_client.send_ack(
                    request.sender_node_id,
                    request.sender_endpoint,
                    advertisement_radius=12345,
                    acked=(True,) * len(request.message.payload),
                    request_id=request.request_id,
                )

        async with trio.open_nursery() as nursery:
            nursery.start_soon(_respond)

            with trio.fail_after(1):
                ack_payload = await alice_alexandria_network.advertise(
                    bob.node_id, advertisements=advertisements,
                )

            assert len(ack_payload.acked) == len(advertisements)


@pytest.mark.trio
async def test_alexandria_network_locate_single_response(
    alice, bob, bob_network, bob_alexandria_client, alice_alexandria_network
):
    advertisement = AdvertisementFactory(private_key=alice.private_key)

    async with bob_alexandria_client.subscribe(LocateMessage) as subscription:

        async def _respond():
            request = await subscription.receive()
            await bob_alexandria_client.send_locations(
                request.sender_node_id,
                request.sender_endpoint,
                advertisements=(advertisement,),
                request_id=request.request_id,
            )

        async with trio.open_nursery() as nursery:
            nursery.start_soon(_respond)

            with trio.fail_after(1):
                locations = await alice_alexandria_network.locate(
                    bob.node_id, content_key=advertisement.content_key,
                )
                assert len(locations) == 1
                location = locations[0]

                assert location == advertisement


@pytest.mark.trio
async def test_alexandria_network_locate_multiple_responses(
    alice, bob, bob_network, bob_alexandria_client, alice_alexandria_network
):
    advertisements = tuple(
        AdvertisementFactory(content_key=b"\x01test-key") for _ in range(20)
    )
    num_messages = len(partition_advertisements(advertisements, MAX_PAYLOAD_SIZE))
    assert num_messages > 1

    async with bob_alexandria_client.subscribe(LocateMessage) as subscription:

        async def _respond():
            request = await subscription.receive()
            await bob_alexandria_client.send_locations(
                request.sender_node_id,
                request.sender_endpoint,
                advertisements=advertisements,
                request_id=request.request_id,
            )

        async with trio.open_nursery() as nursery:
            nursery.start_soon(_respond)

            with trio.fail_after(1):
                locations = await alice_alexandria_network.locate(
                    bob.node_id, content_key=b"\x01test-key",
                )
                assert locations == advertisements


@pytest.mark.trio
async def test_alexandria_network_broadcast_api(
    tester, alice, alice_alexandria_network, autojump_clock,
):
    async with AsyncExitStack() as stack:
        network_group = await stack.enter_async_context(
            tester.alexandria.network_group(10)
        )

        furthest_network = max(
            network_group,
            key=lambda network: compute_distance(alice.node_id, network.local_node_id),
        )
        closest_network = min(
            network_group,
            key=lambda network: compute_distance(alice.node_id, network.local_node_id),
        )

        furthest_node_distance_from_alice = compute_distance(
            furthest_network.local_node_id, alice.node_id,
        )

        furthest_ad = AdvertisementFactory()

        for _ in range(100):
            advertisement = AdvertisementFactory()

            distance_from_alice = compute_content_distance(
                alice.node_id, advertisement.content_id
            )
            distance_from_furthest = compute_content_distance(
                alice.node_id, furthest_ad.content_id
            )

            if distance_from_alice > distance_from_furthest:
                furthest_ad = advertisement

            if distance_from_furthest >= furthest_node_distance_from_alice:
                break

        async with trio.open_nursery() as nursery:

            async def _respond(network, subscription):
                request = await subscription.receive()
                await network.client.send_ack(
                    request.sender_node_id,
                    request.sender_endpoint,
                    advertisement_radius=network.local_advertisement_radius,
                    acked=(True,) * len(request.message.payload),
                    request_id=request.request_id,
                )

            for network in network_group:
                subscription = await stack.enter_async_context(
                    network.client.subscribe(AdvertiseMessage)
                )
                nursery.start_soon(_respond, network, subscription)

            alice_alexandria_network.enr_db.set_enr(closest_network.enr_manager.enr)
            await alice_alexandria_network.bond(closest_network.local_node_id)

            for _ in range(10000):
                await trio.lowlevel.checkpoint()

            with trio.fail_after(30):
                result = await alice_alexandria_network.broadcast(advertisement)
                assert len(result) > 0

            nursery.cancel_scope.cancel()


@pytest.mark.trio
async def test_network_explore(tester, alice):
    async with AsyncExitStack() as stack:
        networks = await stack.enter_async_context(tester.alexandria.network_group(8))

        # give the the network some time to interconnect.
        with trio.fail_after(20):
            for _ in range(1000):
                await trio.lowlevel.checkpoint()

        bootnodes = tuple(network.enr_manager.enr for network in networks)
        alice_network = await stack.enter_async_context(
            alice.alexandria.network(bootnodes=bootnodes)
        )

        # give alice a little time to connect to the network as well
        with trio.fail_after(20):
            for _ in range(1000):
                await trio.lowlevel.checkpoint()

        assert len(set(alice_network.routing_table.iter_all_random())) == 8

        target_node_id = at_log_distance(alice.node_id, 256)
        node_ids_by_distance = tuple(
            sorted(
                tuple(network.local_node_id for network in networks),
                key=lambda node_id: compute_log_distance(target_node_id, node_id),
            )
        )
        best_node_ids_by_distance = set(node_ids_by_distance[:3])

        async with alice_network.explore(target_node_id) as enr_aiter:
            with trio.fail_after(60):
                found_enrs = tuple([enr async for enr in enr_aiter])

        found_node_ids = tuple(enr.node_id for enr in found_enrs)
        assert len(found_node_ids) == len(networks) + 1

        # Ensure that one of the three closest node ids was in the returned node ids
        assert best_node_ids_by_distance.intersection(found_node_ids)


@pytest.mark.trio
async def test_alexandria_network_stream_locations(
    tester, alice,
):
    content_key = b"test-key"
    async with AsyncExitStack() as stack:
        networks = await stack.enter_async_context(tester.alexandria.network_group(6))

        # put advertisements into the database
        advertisements = tuple(
            AdvertisementFactory(
                content_key=content_key,
                hash_tree_root=b"unicornsrainbowscupcakessparkles",
            )
            for _ in range(6)
        )

        for advertisement, network in zip(advertisements, networks):
            network.advertisement_db.add(advertisement)

        # give the the network some time to interconnect.
        with trio.fail_after(30):
            for _ in range(1000):
                await trio.lowlevel.checkpoint()

        bootnodes = tuple(network.enr_manager.enr for network in networks)
        alice_alexandria_network = await stack.enter_async_context(
            alice.alexandria.network(bootnodes=bootnodes)
        )

        with trio.fail_after(30):
            for _ in range(1000):
                await trio.lowlevel.checkpoint()

        advertisement_aiter_ctx = alice_alexandria_network.stream_locations(
            content_key,
        )
        with trio.fail_after(60):
            async with advertisement_aiter_ctx as advertisement_aiter:
                found_advertisements = tuple(
                    [advertisement async for advertisement in advertisement_aiter]
                )

        assert len(found_advertisements) >= 5
        for advertisement in found_advertisements:
            assert advertisement in advertisements


@pytest.mark.trio
async def test_alexandria_network_stream_locations_early_exit(
    tester, alice,
):
    content_key = b"test-key"
    async with AsyncExitStack() as stack:
        networks = await stack.enter_async_context(tester.alexandria.network_group(6))

        # put advertisements into the database
        advertisements = tuple(
            AdvertisementFactory(
                content_key=content_key,
                hash_tree_root=b"unicornsrainbowscupcakessparkles",
            )
            for _ in range(6)
        )

        for advertisement, network in zip(advertisements, networks):
            network.advertisement_db.add(advertisement)

        # give the the network some time to interconnect.
        with trio.fail_after(30):
            for _ in range(1000):
                await trio.lowlevel.checkpoint()

        bootnodes = tuple(network.enr_manager.enr for network in networks)
        alice_alexandria_network = await stack.enter_async_context(
            alice.alexandria.network(bootnodes=bootnodes)
        )

        with trio.fail_after(30):
            for _ in range(1000):
                await trio.lowlevel.checkpoint()

        advertisement_aiter_ctx = alice_alexandria_network.stream_locations(
            content_key,
        )
        with trio.fail_after(60):
            found_advertisements = []

            async with advertisement_aiter_ctx as advertisement_aiter:
                async for advertisement in advertisement_aiter:
                    found_advertisements.append(advertisement)
                    if len(found_advertisements) >= 2:
                        break

        assert len(found_advertisements) == 2
        for advertisement in found_advertisements:
            assert advertisement in advertisements


@pytest.mark.trio
async def test_alexandria_network_get_content(
    tester, alice,
):
    content = ContentFactory(4096)
    proof = compute_proof(content, sedes=content_sedes)
    hash_tree_root = proof.get_hash_tree_root()
    content_key = b"test-key"

    async with AsyncExitStack() as stack:
        networks = await stack.enter_async_context(tester.alexandria.network_group(4))

        for network in networks:
            advertisement = Advertisement.create(
                content_key=content_key,
                hash_tree_root=hash_tree_root,
                private_key=network.client.local_private_key,
            )
            network.advertisement_db.add(advertisement)
            network.pinned_content_storage.set_content(content_key, content)

        # give the the network some time to interconnect.
        with trio.fail_after(30):
            for _ in range(1000):
                await trio.lowlevel.checkpoint()

        bootnodes = tuple(network.enr_manager.enr for network in networks[:2])
        alice_alexandria_network = await stack.enter_async_context(
            alice.alexandria.network(bootnodes=bootnodes)
        )

        # give alice some time to interconnect too
        with trio.fail_after(30):
            for _ in range(1000):
                await trio.lowlevel.checkpoint()

        with trio.fail_after(60):
            result = await alice_alexandria_network.get_content(
                content_key, hash_tree_root
            )

        assert result == proof
        assert result.get_content() == content
