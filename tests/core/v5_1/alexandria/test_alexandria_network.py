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
