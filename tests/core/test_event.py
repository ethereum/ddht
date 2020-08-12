import pytest
import trio

from ddht.event import Event


@pytest.mark.trio
async def test_event_trigger_with_no_subscriptions():
    event = Event("test")

    await event.trigger(None)


@pytest.mark.trio
async def test_event_trigger_with_single_subscription():
    event = Event("test")

    async with event.subscribe() as subscription:
        with pytest.raises(trio.WouldBlock):
            subscription.receive_nowait()

        await event.trigger(1234)

        result = await subscription.receive()
        assert result == 1234

    with pytest.raises(trio.ClosedResourceError):
        subscription.receive_nowait()
    with pytest.raises(trio.ClosedResourceError):
        await subscription.receive()


@pytest.mark.trio
async def test_event_trigger_with_multiple_subscriptions():
    event = Event("test")

    async with event.subscribe() as subscription_a:
        await event.trigger(1234)

        async with event.subscribe() as subscription_b:
            await event.trigger(4321)

            result_a_1 = subscription_a.receive_nowait()
            result_a_2 = subscription_a.receive_nowait()
            result_b_1 = subscription_b.receive_nowait()

            assert result_a_1 == 1234
            assert result_a_2 == 4321
            assert result_b_1 == 4321

            with pytest.raises(trio.WouldBlock):
                subscription_a.receive_nowait()
            with pytest.raises(trio.WouldBlock):
                subscription_b.receive_nowait()


@pytest.mark.trio
async def test_event_wait_without_explicit_subscription():
    event = Event("test")

    async with trio.open_nursery() as nursery:
        got_it = trio.Event()

        async def wait_for_it():
            result = await event.wait()
            assert result == 1234
            got_it.set()

        nursery.start_soon(wait_for_it)
        # trigger a few times just in case the subscription isn't setup yet....
        await event.trigger(1234)
        await event.trigger(1234)
        await event.trigger(1234)
        await event.trigger(1234)

        with trio.fail_after(1):
            await got_it.wait()


@pytest.mark.trio
async def test_event_subscribe_and_wait():
    event = Event("test")

    with trio.fail_after(1):
        async with trio.open_nursery() as nursery:
            async with event.subscribe_and_wait():
                nursery.start_soon(event.trigger, 1234)
