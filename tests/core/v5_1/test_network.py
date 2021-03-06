from contextlib import AsyncExitStack

from eth_enr import ENRManager, OldSequenceNumber
from eth_enr.tools.factories import ENRFactory
from eth_utils import ValidationError
import pytest
import trio

from ddht.exceptions import DuplicateProtocol, EmptyFindNodesResponse
from ddht.kademlia import at_log_distance, compute_log_distance
from ddht.v5_1.abc import TalkProtocolAPI
from ddht.v5_1.constants import FOUND_NODES_MAX_PAYLOAD_SIZE, REQUEST_RESPONSE_TIMEOUT
from ddht.v5_1.exceptions import ProtocolNotSupported
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
                try:
                    bob.enr_db.set_enr(enr)
                except OldSequenceNumber:
                    pass
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
async def test_network_ping_api(alice, bob, alice_network, bob_network):
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
                try:
                    bob.enr_db.set_enr(enr)
                except OldSequenceNumber:
                    pass
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
@pytest.mark.parametrize("response_enr", ("own", "wrong"))
async def test_network_find_nodes_api_validates_response_distances(
    alice, bob, bob_client, alice_network, response_enr
):
    if response_enr == "own":
        enr_for_response = bob.enr
    elif response_enr == "wrong":
        for _ in range(200):
            enr_for_response = ENRFactory()
            if compute_log_distance(enr_for_response.node_id, bob.node_id) == 256:
                break
        else:
            raise Exception("failed")
    else:
        raise Exception(f"unsupported param: {response_enr}")

    async with bob.events.find_nodes_received.subscribe() as subscription:
        async with trio.open_nursery() as nursery:

            async def _respond():
                request = await subscription.receive()
                await bob_client.send_found_nodes(
                    alice.node_id,
                    alice.endpoint,
                    enrs=(enr_for_response,),
                    request_id=request.request_id,
                )

            nursery.start_soon(_respond)
            with trio.fail_after(2):
                with pytest.raises(
                    ValidationError, match="Invalid response: distance="
                ):
                    await alice_network.find_nodes(bob.node_id, 255)


@pytest.mark.trio
async def test_network_stream_find_nodes(alice, bob, alice_network, bob_client):
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
                async with alice_network.stream_find_nodes(
                    bob.node_id, bob.endpoint, distances=distances
                ) as resp_aiter:
                    actual_enrs = tuple([resp async for resp in resp_aiter])
            assert actual_enrs == enrs

            nursery.cancel_scope.cancel()


@pytest.mark.trio
@pytest.mark.parametrize("response_enr", ("own", "wrong"))
async def test_network_stream_find_nodes_api_validates_response_distances(
    alice, bob, bob_client, alice_network, response_enr
):
    if response_enr == "own":
        enr_for_response = bob.enr
    elif response_enr == "wrong":
        for _ in range(200):
            enr_for_response = ENRFactory()
            if compute_log_distance(enr_for_response.node_id, bob.node_id) == 256:
                break
        else:
            raise Exception("failed")
    else:
        raise Exception(f"unsupported param: {response_enr}")

    async with bob.events.find_nodes_received.subscribe() as subscription:
        async with trio.open_nursery() as nursery:

            async def _respond():
                request = await subscription.receive()
                await bob_client.send_found_nodes(
                    alice.node_id,
                    alice.endpoint,
                    enrs=(enr_for_response,),
                    request_id=request.request_id,
                )

            nursery.start_soon(_respond)
            with trio.fail_after(REQUEST_RESPONSE_TIMEOUT):
                with pytest.raises(
                    ValidationError, match="Invalid response: distance="
                ):
                    async with alice_network.stream_find_nodes(
                        bob.node_id, bob.endpoint, distances=[255]
                    ) as resp_aiter:
                        tuple([resp async for resp in resp_aiter])


@pytest.fixture
async def bob_client(bob):
    async with bob.client() as bob_client:
        yield bob_client


@pytest.fixture
async def alice_network(alice):
    async with alice.network() as alice_network:
        yield alice_network


@pytest.mark.trio
async def test_network_lookup_empty_response(bob, alice_network, alice, bob_client):
    async def return_empty_response():
        async with bob.events.find_nodes_received.subscribe() as subscription:
            find_nodes = await subscription.receive()

            await bob_client.send_found_nodes(
                alice.node_id,
                alice.endpoint,
                enrs=[],
                request_id=find_nodes.message.request_id,
            )

    with trio.fail_after(2):
        async with trio.open_nursery() as nursery:
            nursery.start_soon(return_empty_response)

            with pytest.raises(EmptyFindNodesResponse):
                await alice_network.lookup_enr(bob.node_id, enr_seq=101)


@pytest.mark.trio
async def test_network_lookup_normal_response(bob, alice_network, alice, bob_client):
    async def return_normal_response():
        async with bob.events.find_nodes_received.subscribe() as subscription:
            find_nodes = await subscription.receive()

            await bob_client.send_found_nodes(
                alice.node_id,
                alice.endpoint,
                enrs=[bob.enr],
                request_id=find_nodes.message.request_id,
            )

    with trio.fail_after(2):
        async with trio.open_nursery() as nursery:
            nursery.start_soon(return_normal_response)

            enr = await alice_network.lookup_enr(bob.node_id, enr_seq=101)
            assert enr == bob.enr


@pytest.mark.trio
async def test_network_lookup_many_enr_response(bob, alice_network, alice, bob_client):

    enr_manager = ENRManager(private_key=bob.private_key, enr_db=bob.enr_db,)
    enr_manager.update(
        (b"udp", bob.endpoint.port), (b"ip", bob.endpoint.ip_address),
    )

    first_enr = enr_manager.enr

    enr_manager.update((b"eth2", b"\x00\x00"))

    second_enr = enr_manager.enr

    # quick sanity check, if these are equal the next assertion would be testing nothing
    assert first_enr != second_enr

    async def return_duplicate_enr_response():
        async with bob.events.find_nodes_received.subscribe() as subscription:
            find_nodes = await subscription.receive()

            await bob_client.send_found_nodes(
                alice.node_id,
                alice.endpoint,
                enrs=[second_enr, first_enr],
                request_id=find_nodes.message.request_id,
            )

    with trio.fail_after(2):
        async with trio.open_nursery() as nursery:
            nursery.start_soon(return_duplicate_enr_response)

            enr = await alice_network.lookup_enr(bob.node_id, enr_seq=101)
            assert enr == second_enr  # the one with the highest sequence number


@pytest.mark.trio
async def test_network_lookup_fallback_to_recursive_find_nodes(
    tester, bob, alice_network, alice, bob_network
):
    carol = tester.node()

    with trio.fail_after(2):
        await alice_network.bond(bob.node_id)

    with pytest.raises(KeyError):
        await alice_network.lookup_enr(carol.node_id)

    async with carol.network():
        # now add carol to bob's routing table
        try:
            bob.enr_db.set_enr(carol.enr)
        except OldSequenceNumber:
            pass
        bob_network.routing_table.update(carol.node_id)

        with trio.fail_after(2):
            await alice_network.lookup_enr(carol.node_id)


@pytest.mark.trio
async def test_network_recursive_find_nodes(tester, alice):
    async with AsyncExitStack() as stack:
        networks = await stack.enter_async_context(tester.network_group(8))

        # give the the network some time to interconnect.
        with trio.fail_after(20):
            for _ in range(1000):
                await trio.lowlevel.checkpoint()

        bootnodes = tuple(network.enr_manager.enr for network in networks)
        alice_network = await stack.enter_async_context(
            alice.network(bootnodes=bootnodes)
        )

        # give alice a little time to connect to the network as well
        with trio.fail_after(20):
            for _ in range(1000):
                await trio.lowlevel.checkpoint()

        target_node_id = at_log_distance(alice.node_id, 256)
        node_ids_by_distance = tuple(
            sorted(
                tuple(network.local_node_id for network in networks),
                key=lambda node_id: compute_log_distance(target_node_id, node_id),
            )
        )
        best_node_ids_by_distance = set(node_ids_by_distance[:3])

        async with alice_network.recursive_find_nodes(target_node_id) as enr_aiter:
            with trio.fail_after(60):
                found_enrs = tuple([enr async for enr in enr_aiter])

        found_node_ids = set(enr.node_id for enr in found_enrs)
        assert len(found_node_ids) == len(found_enrs)

        # Ensure that one of the three closest node ids was in the returned node ids
        assert best_node_ids_by_distance.intersection(found_node_ids)


@pytest.mark.trio
async def test_network_explore(tester, alice):
    async with AsyncExitStack() as stack:
        networks = await stack.enter_async_context(tester.network_group(8))

        # give the the network some time to interconnect.
        with trio.fail_after(20):
            for _ in range(1000):
                await trio.lowlevel.checkpoint()

        bootnodes = tuple(network.enr_manager.enr for network in networks)
        alice_network = await stack.enter_async_context(
            alice.network(bootnodes=bootnodes)
        )

        # give alice a little time to connect to the network as well
        with trio.fail_after(20):
            for _ in range(1000):
                await trio.lowlevel.checkpoint()

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
async def test_network_talk_api(alice, bob):
    class ProtocolTest(TalkProtocolAPI):
        protocol_id = b"test"

    async def _do_talk_response(network):
        network.add_talk_protocol(ProtocolTest())

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


@pytest.mark.trio
async def test_network_talk_api_not_supported(alice, bob, alice_network, bob_network):
    with trio.fail_after(2):
        with pytest.raises(ProtocolNotSupported):
            await alice_network.talk(
                bob.node_id, protocol=b"test", payload=b"test-payload",
            )


class ProtocolA(TalkProtocolAPI):
    protocol_id = b"a"


class ProtocolB(TalkProtocolAPI):
    protocol_id = b"b"


@pytest.mark.trio
async def test_network_add_talk_protocol(alice):
    async with alice.network() as alice_network:
        alice_network.add_talk_protocol(ProtocolA())

        with pytest.raises(DuplicateProtocol):
            alice_network.add_talk_protocol(ProtocolA())

        alice_network.add_talk_protocol(ProtocolB())

        with pytest.raises(DuplicateProtocol):
            alice_network.add_talk_protocol(ProtocolB())


@pytest.mark.trio
async def test_network_responds_to_unhandled_protocol_messages(
    alice, bob, autojump_clock, bob_client
):
    try:
        bob.enr_db.set_enr(alice.enr)
    except OldSequenceNumber:
        pass

    async with alice.network() as alice_network:
        with trio.fail_after(1):
            response = await bob_client.talk(
                node_id=alice.node_id,
                protocol=ProtocolA.protocol_id,
                payload=b"payload-a",
                endpoint=alice.endpoint,
            )
        assert response.message.payload == b""

        alice_network.add_talk_protocol(ProtocolA())

        with pytest.raises(trio.TooSlowError):
            response = await bob_client.talk(
                node_id=alice.node_id,
                protocol=ProtocolA.protocol_id,
                payload=b"payload-a",
                endpoint=alice.endpoint,
            )


@pytest.mark.trio
async def test_network_bond_api(alice, bob, alice_network, bob_network):
    with trio.fail_after(2):
        result = await alice_network.bond(bob.node_id)
        assert result is True


@pytest.mark.trio
async def test_network_bond_api_fetches_enr(alice, bob, alice_network, bob_network):
    with trio.fail_after(2):
        # ensure alice has the *old* version of bob's ENR
        alice.enr_db.get_enr(bob.enr.node_id) == bob.enr

        # create the new version of bob's ENR
        bob_network.enr_manager.update((b"test", b"value"))
        assert bob_network.enr_manager.enr.sequence_number > bob.enr.sequence_number

        # do the bonding
        result = await alice_network.bond(bob.node_id)
        assert result is True

        latest_bob_enr = bob_network.enr_manager.enr.sequence_number
        assert alice.enr_db.get_enr(bob.node_id).sequence_number == latest_bob_enr


@pytest.mark.trio
async def test_network_bond_handles_ping_timeout(
    alice, bob, alice_network, autojump_clock
):
    with trio.fail_after(30):
        result = await alice_network.bond(bob.node_id)
        assert result is False


@pytest.mark.trio
async def test_network_bond_handles_timeout_retrieving_ENR(
    alice, bob, alice_network, bob_client, autojump_clock
):
    # ensure alice has the *old* version of bob's ENR
    alice.enr_db.get_enr(bob.enr.node_id) == bob.enr

    # create the new version of bob's ENR
    bob_client.enr_manager.update((b"test", b"value"))
    assert bob_client.enr_manager.enr.sequence_number > bob.enr.sequence_number

    async with bob.events.ping_received.subscribe() as ping_subscription:
        async with alice.events.pong_received.subscribe() as pong_subscription:
            async with trio.open_nursery() as nursery:

                async def _respond():
                    request = await ping_subscription.receive()
                    await bob_client.send_pong(
                        request.sender_node_id,
                        request.sender_endpoint,
                        request_id=request.request_id,
                    )

                nursery.start_soon(_respond)

                with trio.fail_after(30):
                    result = await alice_network.bond(bob.node_id)
                    assert result is False

                with trio.fail_after(1):
                    await pong_subscription.receive()
