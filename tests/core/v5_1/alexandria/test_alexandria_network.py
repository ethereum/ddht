from contextlib import AsyncExitStack

from eth_enr.tools.factories import ENRFactory
import pytest
import trio

from ddht.constants import ROUTING_TABLE_BUCKET_SIZE
from ddht.kademlia import KademliaRoutingTable, at_log_distance, compute_log_distance
from ddht.v5_1.alexandria.content import compute_content_distance, content_key_to_content_id
from ddht.v5_1.alexandria.messages import FindNodesMessage, FindContentMessage


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
async def test_alexandria_network_find_content_api(
    alice, bob, alice_alexandria_network, bob_alexandria_client
):
    response_enr = ENRFactory()
    response_content = b'response-value'

    async with bob_alexandria_client.subscribe(FindContentMessage) as subscription:
        async with trio.open_nursery() as nursery:

            async def _respond():
                request = await subscription.receive()

                await bob_alexandria_client.send_found_content(
                    request.sender_node_id,
                    request.sender_endpoint,
                    enrs=(response_enr,),
                    content=None,
                    request_id=request.request_id,
                )

                request = await subscription.receive()

                await bob_alexandria_client.send_found_content(
                    request.sender_node_id,
                    request.sender_endpoint,
                    enrs=None,
                    content=response_content,
                    request_id=request.request_id,
                )

            nursery.start_soon(_respond)

            with trio.fail_after(2):
                response_a = await alice_alexandria_network.find_content(
                    bob.node_id,
                    content_key=b'test-key',
                )

            assert not response_a.is_content
            assert response_a.content == b''
            assert len(response_a.enrs) == 1
            assert response_a.enrs[0] == response_enr

            with trio.fail_after(2):
                response_b = await alice_alexandria_network.find_content(
                    bob.node_id,
                    content_key=b'test-key',
                )

            assert response_b.is_content
            assert response_b.enrs == ()
            assert response_b.content == response_content


@pytest.mark.trio
async def test_alexandria_network_retrieve_content_api(
    alice, tester,
):
    async with AsyncExitStack() as stack:
        networks = await stack.enter_async_context(tester.alexandria.network_group(8))

        all_node_ids = {alice.node_id} | {network.local_node_id for network in networks}

        # we need a content_key for which "alice" is the furthest from the content
        for i in range(4096):
            content_key = b'key-%d' % i
            content_id = content_key_to_content_id(content_key)
            furthest_node_id = max(
                all_node_ids,
                key=lambda node_id: compute_content_distance(node_id, content_id),
            )
            if furthest_node_id == alice.node_id:
                break

        # get the closest node and set the content key in their storage
        closest_network = min(
            networks,
            key=lambda network: compute_content_distance(network.local_node_id, content_id),
        )
        closest_network.storage.set_content(content_key, b'content-value')

        bootnodes = tuple(network.enr_manager.enr for network in networks)
        alice_network = await stack.enter_async_context(
            alice.alexandria.network(bootnodes=bootnodes)
        )

        # give alice a little time to connect to the network as well
        with trio.fail_after(20):
            for _ in range(1000):
                await trio.lowlevel.checkpoint()

        with trio.fail_after(10):
            content = await alice_network.retrieve_content(content_key)

        assert content == b'content-value'
