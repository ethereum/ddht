import pytest
import trio

from ddht._utils import adaptive_timeout


@pytest.mark.trio
async def test_adaptive_timeout_all_complete(autojump_clock):
    did_complete = []

    async def do_sleep(task_id, seconds):
        await trio.sleep(seconds)
        did_complete.append(task_id)

    tasks = (
        (do_sleep, (1, 0.9)),
        (do_sleep, (1, 1.0)),
        (do_sleep, (1, 1.1)),
    )

    # with a threshold of 1 and variance of 2.0 all tasks should complete.
    await adaptive_timeout(*tasks, threshold=1, variance=2.0)

    assert len(did_complete) == 3


@pytest.mark.trio
async def test_adaptive_timeout_some_timeout(autojump_clock):
    did_complete = []

    async def do_sleep(task_id, seconds):
        await trio.sleep(seconds)
        did_complete.append(task_id)

    tasks = (
        (do_sleep, (1, 0.9)),
        (do_sleep, (1, 1.0)),
        (do_sleep, (1, 2.5)),  # this one should not complete
    )

    # with a threshold of 1 and variance of 2.0 all tasks should complete.
    await adaptive_timeout(*tasks, threshold=1, variance=2.0)

    assert len(did_complete) == 2


@pytest.mark.trio
async def test_adaptive_timeout_higher_threshold_all_complete(autojump_clock):
    did_complete = []

    async def do_sleep(task_id, seconds):
        await trio.sleep(seconds)
        did_complete.append(task_id)

    tasks = (
        (do_sleep, (1, 1.0)),
        (do_sleep, (1, 3.0)),
        (do_sleep, (1, 3.5)),
    )

    # with a threshold of 1 and variance of 2.0 all tasks should complete.
    await adaptive_timeout(*tasks, threshold=2, variance=2.0)

    assert len(did_complete) == 3


@pytest.mark.trio
async def test_adaptive_timeout_higher_threshold_some_timeout(autojump_clock):
    did_complete = []

    async def do_sleep(task_id, seconds):
        await trio.sleep(seconds)
        did_complete.append(task_id)

    tasks = (
        (do_sleep, (1, 1.0)),
        (do_sleep, (1, 3.0)),
        (do_sleep, (1, 4.5)),
    )

    # with a threshold of 1 and variance of 2.0 all tasks should complete.
    await adaptive_timeout(*tasks, threshold=2, variance=2.0)

    assert len(did_complete) == 2
