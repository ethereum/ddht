from async_service import background_trio_service
import pytest
import trio

from ddht.tools.factories.content import ContentFactory
from ddht.v5_1.alexandria.content import ContentProvider, content_key_to_content_id
from ddht.v5_1.alexandria.messages import ContentMessage
from ddht.v5_1.alexandria.partials.proof import compute_proof, validate_proof
from ddht.v5_1.alexandria.sedes import content_sedes


@pytest.mark.trio
async def test_content_provider_serves_short_content(
    alice, bob, alice_alexandria_network, bob_alexandria_client,
):
    content = ContentFactory(length=1024)
    key = b"\x01"
    content_id = content_key_to_content_id(key)
    db = {content_id: content}
    proof = compute_proof(content, sedes=content_sedes)

    content_provider = ContentProvider(bob_alexandria_client, db)
    async with background_trio_service(content_provider):
        async with alice_alexandria_network.client.subscribe(
            ContentMessage
        ) as subscription:
            # this ensures that the subscription is in place.
            await content_provider.ready()

            with trio.fail_after(2):
                result = await alice_alexandria_network.get_content_from_nodes(
                    nodes=((bob.node_id, bob.endpoint),),
                    hash_tree_root=proof.hash_tree_root,
                    key=key,
                    concurrency=1,
                )
                validate_proof(result)
                result_data = result.get_proven_data()
                assert result_data[0 : len(content)] == content

            response = await subscription.receive()
            assert response.message.payload.is_proof is False


@pytest.mark.trio
async def test_content_provider_serves_large_content(
    alice, bob, alice_alexandria_network, bob_alexandria_client,
):
    content = ContentFactory(length=1024 * 10)
    key = b"\x01"
    content_id = content_key_to_content_id(key)
    db = {content_id: content}
    proof = compute_proof(content, sedes=content_sedes)

    content_provider = ContentProvider(bob_alexandria_client, db)
    async with background_trio_service(content_provider):
        async with alice_alexandria_network.client.subscribe(
            ContentMessage
        ) as subscription:
            # this ensures that the subscription is in place.
            await content_provider.ready()

            with trio.fail_after(2):
                result = await alice_alexandria_network.get_content_from_nodes(
                    nodes=((bob.node_id, bob.endpoint),),
                    hash_tree_root=proof.hash_tree_root,
                    key=key,
                    concurrency=1,
                )
                validate_proof(result)
                result_data = result.get_proven_data()
                assert result_data[0 : len(content)] == content

            response = await subscription.receive()
            assert response.message.payload.is_proof is True


@pytest.mark.trio
async def test_content_provider_restricts_max_chunks(
    alice, bob, alice_alexandria_network, bob_alexandria_client,
):
    content = ContentFactory(length=1024 * 10)
    key = b"\x01"
    content_id = content_key_to_content_id(key)
    db = {content_id: content}
    proof = compute_proof(content, sedes=content_sedes)

    content_provider = ContentProvider(
        bob_alexandria_client, db, max_chunks_per_request=16
    )
    async with background_trio_service(content_provider):
        # this ensures that the subscription is in place.
        await content_provider.ready()

        with trio.fail_after(2):
            proof = await alice_alexandria_network.get_content_proof(
                bob.node_id,
                hash_tree_root=proof.hash_tree_root,
                key=key,
                start_chunk_index=0,
                max_chunks=100,
                endpoint=bob.endpoint,
            )
            validate_proof(proof)
            num_leaf_chunks = len(
                tuple(
                    node
                    for node in proof.get_tree().walk()
                    if node.is_leaf and node.path != (True,)
                )
            )
            assert num_leaf_chunks == 16
