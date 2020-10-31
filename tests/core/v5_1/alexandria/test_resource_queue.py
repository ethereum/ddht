import random

import pytest
import trio

from ddht.v5_1.alexandria.resource_queue import ResourceQueue


async def _yield(num: int = 10):
    for _ in range(random.randint(0, num)):
        await trio.lowlevel.checkpoint()


@pytest.mark.trio
async def test_resource_queue():
    known_resources = {"a", "b", "c", "d"}
    queue = ResourceQueue(known_resources)

    resources_in_use = set()
    seen_resources = set()

    async def worker(seen):
        while True:
            async with queue.reserve() as resource:
                seen.add(resource)
                assert resource in queue.resources

                await _yield()

                assert resource not in resources_in_use
                resources_in_use.add(resource)

                await _yield()

                resources_in_use.remove(resource)

                await _yield()

                assert resource not in resources_in_use

    async with trio.open_nursery() as nursery:
        for _ in range(10):
            nursery.start_soon(worker, seen_resources)

        await _yield(200)

        assert seen_resources == queue.resources

        assert "e" not in queue.resources
        assert "f" not in queue.resources

        await queue.add("e")
        await queue.add("f")

        assert "e" in queue.resources
        assert "f" in queue.resources

        await _yield(200)

        seen_resources_after_add = set()

        for _ in range(20):
            nursery.start_soon(worker, seen_resources_after_add)

        await _yield(200)

        assert seen_resources_after_add == queue.resources

        nursery.cancel_scope.cancel()
