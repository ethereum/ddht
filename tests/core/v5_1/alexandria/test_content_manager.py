from contextlib import AsyncExitStack

import pytest
import ssz
import trio

from ddht.tools.factories.content import ContentFactory
from ddht.v5_1.alexandria.advertisements import Advertisement
from ddht.v5_1.alexandria.messages import (
    AdvertiseMessage,
    FindNodesMessage,
    PingMessage,
)
from ddht.v5_1.alexandria.sedes import content_sedes


@pytest.mark.trio
async def test_content_manager_enumerates_and_broadcasts_content(
    alice, bob,
):
    #
    # Test Setup:
    #
    # 15 pieces of content
    # 5 that don't already have advertisements to test lazy creation
    contents = tuple(
        (b"\xfftest-content-" + bytes([idx]), ContentFactory(256 + 25)[idx : idx + 256])
        for idx in range(15)
    )

    advertisement_db = alice.alexandria.advertisement_db
    pinned_storage = alice.alexandria.pinned_content_storage

    for content_key, content in contents[0:16:3]:
        hash_tree_root = ssz.get_hash_tree_root(content, sedes=content_sedes)
        advertisement = Advertisement.create(
            content_key=content_key,
            hash_tree_root=hash_tree_root,
            private_key=alice.private_key,
        )
        advertisement_db.add(advertisement)

    content_keys, content_payloads = zip(*contents)

    for content_key, content in contents:
        pinned_storage.set_content(content_key, content)

    received_advertisements = []

    async with AsyncExitStack() as stack:
        bob_alexandria_client = await stack.enter_async_context(bob.alexandria.client())
        ad_subscription = await stack.enter_async_context(
            bob_alexandria_client.subscribe(AdvertiseMessage)
        )
        ping_subscription = await stack.enter_async_context(
            bob_alexandria_client.subscribe(PingMessage)
        )
        find_nodes_subscription = await stack.enter_async_context(
            bob_alexandria_client.subscribe(FindNodesMessage)
        )

        done = trio.Event()

        async def _do_found_nodes():
            async for request in find_nodes_subscription:
                await bob_alexandria_client.send_found_nodes(
                    request.sender_node_id,
                    request.sender_endpoint,
                    enrs=(),
                    request_id=request.request_id,
                )

        async def _do_pong():
            async for request in ping_subscription:
                await bob_alexandria_client.send_pong(
                    request.sender_node_id,
                    request.sender_endpoint,
                    advertisement_radius=2 ** 256 - 1,
                    enr_seq=bob.enr.sequence_number,
                    request_id=request.request_id,
                )

        async def _do_ack():
            async for request in ad_subscription:
                received_advertisements.extend(request.message.payload)
                await bob_alexandria_client.send_ack(
                    request.sender_node_id,
                    request.sender_endpoint,
                    advertisement_radius=2 ** 256 - 1,
                    request_id=request.request_id,
                )

                if len(received_advertisements) >= 15:
                    done.set()
                    break

        async with trio.open_nursery() as nursery:
            nursery.start_soon(_do_ack)
            nursery.start_soon(_do_pong)
            nursery.start_soon(_do_found_nodes)

            async with alice.alexandria.network() as alice_alexandria_network:
                await alice_alexandria_network.bond(bob.node_id)

                with trio.fail_after(60):
                    await done.wait()

            nursery.cancel_scope.cancel()

    # 1. All 15 pieces of content should have been advertised
    received_keys = {
        advertisement.content_key for advertisement in received_advertisements
    }
    assert len(received_keys) == 15
    assert received_keys == set(content_keys)

    # 2. The 10 contents that didn't have advertisements should have been lazily created.
    assert all(
        advertisement_db.exists(advertisement)
        for advertisement in received_advertisements
    )
