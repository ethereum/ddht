from async_service import background_trio_service
import pytest
import trio

from ddht.tools.factories.alexandria import AdvertisementFactory
from ddht.v5_1.alexandria.messages import AdvertiseMessage, PingMessage
from ddht.v5_1.alexandria.radius_tracker import RadiusTracker


@pytest.mark.trio
async def test_radius_tracker_fetches_radius_when_unknown(
    alice, bob, alice_alexandria_network, bob_alexandria_client
):
    radius_tracker = RadiusTracker(alice_alexandria_network)

    async with bob_alexandria_client.subscribe(PingMessage) as subscription:
        async with trio.open_nursery() as nursery:
            did_respond = trio.Event()

            async def _respond():
                request = await subscription.receive()
                await bob_alexandria_client.send_pong(
                    request.sender_node_id,
                    request.sender_endpoint,
                    enr_seq=bob.enr.sequence_number,
                    advertisement_radius=1234,
                    request_id=request.request_id,
                )
                did_respond.set()

            nursery.start_soon(_respond)

            async with background_trio_service(radius_tracker):
                await radius_tracker.ready()

                with trio.fail_after(2):
                    advertisement_radius = await radius_tracker.get_advertisement_radius(
                        bob.node_id,
                    )

                    assert advertisement_radius == 1234

                    await did_respond.wait()


@pytest.mark.trio
async def test_radius_tracker_tracks_via_ping(
    alice, bob, alice_alexandria_network, bob_alexandria_client
):
    radius_tracker = RadiusTracker(alice_alexandria_network)

    bob.enr_db.set_enr(alice.enr)

    async with background_trio_service(radius_tracker):
        await radius_tracker.ready()

        await bob_alexandria_client.ping(
            alice.node_id, alice.endpoint, enr_seq=0, advertisement_radius=1234,
        )

        with trio.fail_after(2):
            advertisement_radius = await radius_tracker.get_advertisement_radius(
                bob.node_id,
            )

            assert advertisement_radius == 1234


@pytest.mark.trio
async def test_radius_tracker_tracks_via_pong(
    alice, bob, alice_alexandria_network, bob_alexandria_client
):
    radius_tracker = RadiusTracker(alice_alexandria_network)

    async with bob_alexandria_client.subscribe(PingMessage) as subscription:
        async with trio.open_nursery() as nursery:
            did_respond = trio.Event()

            async def _respond():
                request = await subscription.receive()
                await bob_alexandria_client.send_pong(
                    request.sender_node_id,
                    request.sender_endpoint,
                    enr_seq=bob.enr.sequence_number,
                    advertisement_radius=1234,
                    request_id=request.request_id,
                )
                did_respond.set()

            nursery.start_soon(_respond)

            async with background_trio_service(radius_tracker):
                await radius_tracker.ready()

                await alice_alexandria_network.ping(bob.node_id,)

                with trio.fail_after(2):
                    advertisement_radius = await radius_tracker.get_advertisement_radius(
                        bob.node_id,
                    )

                    assert advertisement_radius == 1234

                    await did_respond.wait()


@pytest.mark.trio
async def test_radius_tracker_tracks_via_ack(
    alice, bob, alice_alexandria_network, bob_alexandria_client
):
    radius_tracker = RadiusTracker(alice_alexandria_network)

    async with bob_alexandria_client.subscribe(AdvertiseMessage) as subscription:
        async with trio.open_nursery() as nursery:
            did_respond = trio.Event()

            async def _respond():
                request = await subscription.receive()
                await bob_alexandria_client.send_ack(
                    request.sender_node_id,
                    request.sender_endpoint,
                    advertisement_radius=1234,
                    acked=(True,),
                    request_id=request.request_id,
                )
                did_respond.set()

            nursery.start_soon(_respond)

            async with background_trio_service(radius_tracker):
                await radius_tracker.ready()

                await alice_alexandria_network.advertise(
                    bob.node_id, advertisements=(AdvertisementFactory(),),
                )

                with trio.fail_after(2):
                    advertisement_radius = await radius_tracker.get_advertisement_radius(
                        bob.node_id,
                    )

                    assert advertisement_radius == 1234

                    await did_respond.wait()
