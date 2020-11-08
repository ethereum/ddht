from eth_enr.tools.factories import ENRFactory
import pytest
import trio

from ddht.constants import ROUTING_TABLE_BUCKET_SIZE
from ddht.kademlia import KademliaRoutingTable, compute_log_distance
from ddht.tools.factories.content import ContentFactory
from ddht.v5_1.alexandria.messages import FindNodesMessage, GetContentMessage
from ddht.v5_1.alexandria.partials.proof import compute_proof, validate_proof
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
async def test_alexandria_network_get_content_proof_api(
    alice, bob, alice_alexandria_network, bob_alexandria_client,
):
    content = ContentFactory(length=16 * 32 * 4)
    proof = compute_proof(content, sedes=content_sedes)

    async with bob_alexandria_client.subscribe(GetContentMessage) as subscription:
        async with trio.open_nursery() as nursery:

            async def _serve():
                request = await subscription.receive()
                partial = proof.to_partial(
                    request.message.payload.start_chunk_index * 32,
                    request.message.payload.max_chunks * 32,
                )
                payload = partial.serialize()
                await bob_alexandria_client.send_content(
                    request.sender_node_id,
                    request.sender_endpoint,
                    is_proof=True,
                    payload=payload,
                    request_id=request.request_id,
                )

            nursery.start_soon(_serve)

            with trio.fail_after(2):
                partial = await alice_alexandria_network.get_content_proof(
                    bob.node_id,
                    bob.endpoint,
                    hash_tree_root=proof.hash_tree_root,
                    key=b"\x01",
                    start_chunk_index=0,
                    max_chunks=16,
                )
                validate_proof(partial)
                partial_data = partial.get_proven_data()
                assert partial_data[0 : 16 * 32] == content[0 : 16 * 32]


@pytest.mark.trio
async def test_alexandria_network_get_content_from_nodes_api_single_peer(
    alice, bob, alice_alexandria_network, bob_alexandria_client,
):
    content = ContentFactory(length=1024 * 10)
    proof = compute_proof(content, sedes=content_sedes)

    async with bob_alexandria_client.subscribe(GetContentMessage) as subscription:
        async with trio.open_nursery() as nursery:

            async def _serve():
                async for request in subscription:
                    start_at = request.message.payload.start_chunk_index * 32
                    end_at = min(
                        len(content), start_at + request.message.payload.max_chunks * 32
                    )
                    partial = proof.to_partial(start_at, end_at - start_at)
                    payload = partial.serialize()
                    await bob_alexandria_client.send_content(
                        request.sender_node_id,
                        request.sender_endpoint,
                        is_proof=True,
                        payload=payload,
                        request_id=request.request_id,
                    )

            nursery.start_soon(_serve)

            with trio.fail_after(2):
                result = await alice_alexandria_network.get_content_from_nodes(
                    nodes=((bob.node_id, bob.endpoint),),
                    hash_tree_root=proof.hash_tree_root,
                    key=b"\x01",
                    concurrency=1,
                )
                validate_proof(result)
                result_data = result.get_proven_data()
                assert result_data[0 : len(content)] == content

            nursery.cancel_scope.cancel()


@pytest.mark.trio
async def test_alexandria_network_get_content_from_nodes_api_impartial_chunks(
    alice, bob, alice_alexandria_network, bob_alexandria_client,
):
    content = ContentFactory(length=1024 * 10)
    proof = compute_proof(content, sedes=content_sedes)

    async with bob_alexandria_client.subscribe(GetContentMessage) as subscription:
        async with trio.open_nursery() as nursery:

            async def _serve():
                async for request in subscription:
                    start_at = request.message.payload.start_chunk_index * 32
                    # We only every return a proof for 1 chunk of data
                    end_at = min(len(content), start_at + 32)
                    partial = proof.to_partial(start_at, end_at - start_at)
                    payload = partial.serialize()
                    await bob_alexandria_client.send_content(
                        request.sender_node_id,
                        request.sender_endpoint,
                        is_proof=True,
                        payload=payload,
                        request_id=request.request_id,
                    )

            nursery.start_soon(_serve)

            with trio.fail_after(2):
                result = await alice_alexandria_network.get_content_from_nodes(
                    nodes=((bob.node_id, bob.endpoint),),
                    hash_tree_root=proof.hash_tree_root,
                    key=b"\x01",
                    concurrency=1,
                )
                validate_proof(result)
                result_data = result.get_proven_data()
                assert result_data[0 : len(content)] == content

            nursery.cancel_scope.cancel()
