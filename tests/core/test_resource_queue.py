import random

import pytest
import trio

from ddht.resource_queue import ResourceQueue


async def _yield(num: int = 10, base: int = 0):
    for _ in range(random.randint(0, num) + base):
        await trio.lowlevel.checkpoint()


@pytest.mark.trio
async def test_resource_queue_fuzzy():
    known_resources = {"a", "b", "c", "d"}
    queue = ResourceQueue(known_resources)

    resources_in_use = set()
    seen_resources = set()

    async def worker(seen):
        """
        Worker process intended to try and hit as many edge cases as possible
        about what could happen within the context block of
        `ResourceQueue.reserve` by yielding to trio at as many stages as
        possible.
        """
        while True:
            async with queue.reserve() as resource:
                seen.add(resource)
                assert resource in queue

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

        await _yield(1, 200)

        assert seen_resources == queue.resources

        assert "e" not in queue
        assert "f" not in queue

        # Now add two more resources.  They should get picked up by the new
        # workers.
        await queue.add("e")
        await queue.add("f")

        assert "e" in queue
        assert "f" in queue

        await _yield(1, 200)

        seen_resources_after_add = set()

        for _ in range(10):
            nursery.start_soon(worker, seen_resources_after_add)

        await _yield(1, 200)

        assert seen_resources_after_add == queue.resources

        nursery.cancel_scope.cancel()


@pytest.mark.trio
async def test_resource_queue_add_idempotent():
    queue = ResourceQueue(("a", "b", "c"), max_resource_count=4)

    assert len(queue) == 3

    await queue.add("a")

    assert len(queue) == 3

    await queue.add("d")

    assert len(queue) == 4


@pytest.mark.trio
async def test_resource_queue_add_blocks_when_queue_full(autojump_clock):
    queue = ResourceQueue(("a", "b", "c"), max_resource_count=3)

    await queue.add("a")

    assert len(queue) == 3

    with pytest.raises(trio.TooSlowError):
        with trio.fail_after(1):
            await queue.add("d")


@pytest.mark.trio
async def test_resource_queue_remove():
    queue = ResourceQueue(("a", "b", "c"), max_resource_count=10)

    assert len(queue) == 3

    queue.remove("a")

    assert len(queue) == 2

    with pytest.raises(KeyError):
        queue.remove("a")
