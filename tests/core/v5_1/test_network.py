import collections
from contextlib import AsyncExitStack
import secrets

from eth_enr.tools.factories import ENRFactory
import pytest
import trio

from ddht.kademlia import compute_log_distance
from ddht.v5_1.constants import ROUTING_TABLE_KEEP_ALIVE
from ddht.v5_1.messages import FoundNodesMessage, TalkRequestMessage


@pytest.mark.trio
async def test_network_responds_to_pings(alice, bob):
    async with alice.network() as alice_network:
        async with bob.network():
            with trio.fail_after(2):
                response = await alice_network.client.ping(bob.node_id, bob.endpoint)

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
                bob.enr_db.set_enr(enr)
                bob_network.routing_table.update(enr.node_id)
                distances.add(compute_log_distance(enr.node_id, bob.node_id))
                if distances.issuperset({0, 256, 255}):
                    break
            else:
                raise Exception("failed")

            with trio.fail_after(2):
                responses = await alice_network.client.find_nodes(
                    bob.node_id, bob.endpoint, distances=(0, 255, 256),
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
                bob.enr_db.set_enr(enr)
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
                await trio.lowlevel.checkpoint()

        alice_network = await stack.enter_async_context(
            alice.network(bootnodes=bootnodes)
        )

        # give alice a little time to connect to the network as well
        with trio.fail_after(5):
            for _ in range(1000):
                await trio.lowlevel.checkpoint()

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


@pytest.mark.trio
async def test_network_pings_oldest_routing_table(tester, alice, bob, autojump_clock):
    async with AsyncExitStack() as stack:
        carol = tester.node()
        dylan = tester.node()

        # startup a few peers to put in the routing table
        bob_network = await stack.enter_async_context(bob.network())
        carol_network = await stack.enter_async_context(carol.network())

        # populate the routing table and ENR database
        alice.enr_db.set_enr(bob.enr)
        alice.enr_db.set_enr(carol.enr)
        alice.enr_db.set_enr(dylan.enr)

        async with AsyncExitStack() as handshakes_done:
            await handshakes_done.enter_async_context(
                bob.events.session_handshake_complete.subscribe_and_wait()
            )
            await handshakes_done.enter_async_context(
                carol.events.session_handshake_complete.subscribe_and_wait()
            )

            alice_network = await stack.enter_async_context(
                alice.network(bootnodes=[bob.enr, carol.enr, dylan.enr]),
            )
        alice_network.routing_table.update(dylan.node_id)
        alice_network.routing_table.update(bob.node_id)
        alice_network.routing_table.update(carol.node_id)

        assert alice_network.routing_table._contains(bob.node_id, False)
        assert alice_network.routing_table._contains(carol.node_id, False)
        assert alice_network.routing_table._contains(dylan.node_id, False)

        # run through a few checkpoints which should remove dylan from the routing table
        await trio.sleep(ROUTING_TABLE_KEEP_ALIVE)
        await trio.sleep(ROUTING_TABLE_KEEP_ALIVE)

        assert alice_network.routing_table._contains(bob.node_id, False)
        assert alice_network.routing_table._contains(carol.node_id, False)
        assert not alice_network.routing_table._contains(dylan.node_id, False)

        # now take carol offline and let her be removed
        carol_network.get_manager().cancel()
        alice_network._last_pong_at[carol.node_id] = (
            trio.current_time() - ROUTING_TABLE_KEEP_ALIVE - 1
        )
        await trio.sleep(ROUTING_TABLE_KEEP_ALIVE)
        await trio.sleep(ROUTING_TABLE_KEEP_ALIVE)

        assert alice_network.routing_table._contains(bob.node_id, False)
        assert not alice_network.routing_table._contains(carol.node_id, False)
        assert not alice_network.routing_table._contains(dylan.node_id, False)

        # now take bob offline and let her be removed
        bob_network.get_manager().cancel()
        alice_network._last_pong_at[bob.node_id] = (
            trio.current_time() - ROUTING_TABLE_KEEP_ALIVE - 1
        )
        await trio.sleep(ROUTING_TABLE_KEEP_ALIVE)
        await trio.sleep(ROUTING_TABLE_KEEP_ALIVE)

        assert not alice_network.routing_table._contains(bob.node_id, False)
        assert not alice_network.routing_table._contains(carol.node_id, False)
        assert not alice_network.routing_table._contains(dylan.node_id, False)


@pytest.mark.trio
async def test_network_talk_api(alice, bob):
    async def _do_talk_response(network):
        async with network.dispatcher.subscribe(TalkRequestMessage) as subscription:
            request = await subscription.receive()
            await network.client.send_talk_response(
                request.sender_node_id,
                request.sender_endpoint,
                payload=b"test-response-payload",
                request_id=request.message.request_id,
            )

    async with alice.network() as alice_network:
        async with bob.network() as bob_network:
            async with trio.open_nursery() as nursery:
                nursery.start_soon(_do_talk_response, bob_network)

                with trio.fail_after(2):
                    response = await alice_network.talk(
                        bob.node_id, protocol=b"test", payload=b"test-payload",
                    )

    assert response == b"test-response-payload"
