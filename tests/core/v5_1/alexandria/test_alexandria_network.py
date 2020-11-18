from eth_enr.tools.factories import ENRFactory
import pytest
import trio

from ddht.constants import ROUTING_TABLE_BUCKET_SIZE
from ddht.kademlia import KademliaRoutingTable, compute_log_distance
from ddht.v5_1.alexandria.advertisements import Advertisement
from ddht.v5_1.alexandria.messages import AdvertiseMessage, FindNodesMessage
from ddht.v5_1.alexandria.payloads import AckPayload


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
            bob.node_id, bob.endpoint
        )

    assert pong_message.payload.enr_seq == bob.enr.sequence_number


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
async def test_alexandria_network_advertise(
    alice, bob, bob_network, bob_alexandria_client, alice_alexandria_network
):
    advertisements = tuple(
        Advertisement.create(
            content_key=b"\x01testkey",
            hash_tree_root=b"\x12" * 32,
            private_key=alice.private_key,
        )
        for _ in range(10)
    )

    async with bob_alexandria_client.subscribe(AdvertiseMessage) as subscription:

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
                ack_payloads = await alice_alexandria_network.advertise(
                    bob.node_id, advertisements=advertisements,
                )
                assert len(ack_payloads) == 1
                ack_payload = ack_payloads[0]

                assert isinstance(ack_payload, AckPayload)
                assert ack_payload.advertisement_radius == 12345
