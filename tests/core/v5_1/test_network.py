import pytest
import trio

from ddht.kademlia import compute_log_distance
from ddht.tools.factories.enr import ENRFactory
from ddht.v5_1.messages import FoundNodesMessage


@pytest.mark.trio
async def test_network_responds_to_pings(alice, bob):
    async with alice.network() as alice_network:
        async with bob.network():
            with trio.fail_after(2):
                response = await alice_network.client.ping(bob.endpoint, bob.node_id,)

    assert response.message.enr_seq == bob.enr.sequence_number
    assert response.message.packet_ip == alice.endpoint.ip_address
    assert response.message.packet_port == alice.endpoint.port


@pytest.mark.trio
async def test_network_responds_to_find_node_requests(alice, bob):
    distances = (0, 255, 254)

    async with alice.network() as alice_network:
        async with bob.network() as bob_network:
            for _ in range(20):
                enr = ENRFactory()
                bob.node_db.set_enr(enr)
                bob_network.routing_table.update(enr.node_id)

            with trio.fail_after(2):
                responses = await alice_network.client.find_nodes(
                    bob.endpoint, bob.node_id, distances=distances,
                )

    assert all(
        isinstance(response.message, FoundNodesMessage) for response in responses
    )
    response_enrs = tuple(
        enr for response in responses for enr in response.message.enrs
    )
    response_distances = {
        compute_log_distance(enr.node_id, bob.node_id)
        if enr.node_id != bob.node_id
        else 0
        for enr in response_enrs
    }
    assert response_distances == set(distances)
