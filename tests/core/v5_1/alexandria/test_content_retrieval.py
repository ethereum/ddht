from async_service import LifecycleError, background_trio_service
import pytest
import trio

from ddht.tools.factories.content import ContentFactory
from ddht.v5_1.alexandria.content_retrieval import ContentRetrieval
from ddht.v5_1.alexandria.messages import GetContentMessage
from ddht.v5_1.alexandria.partials.proof import compute_proof, validate_proof
from ddht.v5_1.alexandria.sedes import content_sedes


@pytest.mark.trio
async def test_content_retrieval_cannot_await_before_running(alice_alexandria_network):
    content_retrieval = ContentRetrieval(
        alice_alexandria_network,
        content_key=b"test-key",
        hash_tree_root=b"unicornsrainbowscupcakessparkles",
    )

    with pytest.raises(LifecycleError):
        await content_retrieval.wait_content_proof()


@pytest.mark.trio
async def test_content_retrieval_from_single_peer(
    alice, bob, alice_alexandria_network, bob_alexandria_client,
):
    content = ContentFactory(length=1024 * 10)
    proof = compute_proof(content, sedes=content_sedes)

    content_retrieval = ContentRetrieval(
        alice_alexandria_network,
        content_key=b"test-key",
        hash_tree_root=proof.get_hash_tree_root(),
    )

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

            await content_retrieval.node_queue.add(bob.node_id)

            async with background_trio_service(content_retrieval):
                with trio.fail_after(10):
                    result = await content_retrieval.wait_content_proof()

                validate_proof(result)
                result_data = result.get_proven_data()
                assert result_data[0 : len(content)] == content

            nursery.cancel_scope.cancel()


@pytest.mark.trio
async def test_content_retrieval_with_griefing_peer_sending_tiny_chunks(
    alice, bob, alice_alexandria_network, bob_alexandria_client,
):
    content = ContentFactory(length=1024 * 10)
    proof = compute_proof(content, sedes=content_sedes)

    content_retrieval = ContentRetrieval(
        alice_alexandria_network,
        content_key=b"test-key",
        hash_tree_root=proof.get_hash_tree_root(),
    )

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

            await content_retrieval.node_queue.add(bob.node_id)

            with trio.fail_after(10):
                async with background_trio_service(content_retrieval):
                    with trio.fail_after(5):
                        result = await content_retrieval.wait_content_proof()

                validate_proof(result)
                result_data = result.get_proven_data()
                assert result_data[0 : len(content)] == content

            nursery.cancel_scope.cancel()
