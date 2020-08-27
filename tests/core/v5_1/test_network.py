import collections
from contextlib import AsyncExitStack
import secrets

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
    distances = {0}

    async with alice.network() as alice_network:
        async with bob.network() as bob_network:
            for _ in range(200):
                enr = ENRFactory()
                bob.node_db.set_enr(enr)
                bob_network.routing_table.update(enr.node_id)
                distances.add(compute_log_distance(enr.node_id, bob.node_id))
                if distances.issuperset({0, 256, 255}):
                    break
            else:
                raise Exception("failed")

            with trio.fail_after(2):
                responses = await alice_network.client.find_nodes(
                    bob.endpoint, bob.node_id, distances=(0, 255, 256),
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
    assert response_distances.issuperset({0, 255, 256})


@pytest.mark.trio
async def test_network_ping_api(alice, bob):
    async with alice.network() as alice_network:
        async with bob.network():
            with trio.fail_after(2):
                pong = await alice_network.ping(bob.node_id)

    assert pong.enr_seq == bob.enr.sequence_number
    assert pong.packet_ip == alice.endpoint.ip_address
    assert pong.packet_port == alice.endpoint.port


@pytest.mark.trio
async def test_network_find_nodes_api(alice, bob):
    distances = {0}

    async with alice.network() as alice_network:
        async with bob.network() as bob_network:
            for _ in range(200):
                enr = ENRFactory()
                bob.node_db.set_enr(enr)
                bob_network.routing_table.update(enr.node_id)
                distances.add(compute_log_distance(enr.node_id, bob.node_id))
                if distances.issuperset({0, 256, 255}):
                    break
            else:
                raise Exception("failed")

            with trio.fail_after(2):
                enrs = await alice_network.find_nodes(bob.node_id, 0, 255, 256)

    assert any(enr.node_id == bob.node_id for enr in enrs)
    response_distances = {
        compute_log_distance(enr.node_id, bob.node_id)
        for enr in enrs
        if enr.node_id != bob.node_id
    }
    assert response_distances == {256, 255}


@pytest.mark.trio
async def test_network_recursive_find_nodes(tester, alice, bob):
    async with AsyncExitStack() as stack:
        await stack.enter_async_context(bob.network())
        bootnodes = collections.deque((bob.enr,), maxlen=4)
        nodes = [bob, alice]
        for _ in range(20):
            node = tester.node()
            nodes.append(node)
            await stack.enter_async_context(node.network(bootnodes=bootnodes))
            bootnodes.append(node.enr)

        # give the the network some time to interconnect.
        with trio.fail_after(5):
            for _ in range(1000):
                await trio.hazmat.checkpoint()

        alice_network = await stack.enter_async_context(
            alice.network(bootnodes=bootnodes)
        )

        # give alice a little time to connect to the network as well
        with trio.fail_after(5):
            for _ in range(1000):
                await trio.hazmat.checkpoint()

        target_node_id = secrets.token_bytes(32)
        node_ids_by_distance = tuple(
            sorted(
                tuple(node.enr.node_id for node in nodes),
                key=lambda node_id: compute_log_distance(target_node_id, node_id),
            )
        )
        best_node_ids_by_distance = set(node_ids_by_distance[:3])

        with trio.fail_after(10):
            found_enrs = await alice_network.recursive_find_nodes(target_node_id)
        found_node_ids = tuple(enr.node_id for enr in found_enrs)

        # Ensure that one of the three closest node ids was in the returned node ids
        assert best_node_ids_by_distance.intersection(found_node_ids)
