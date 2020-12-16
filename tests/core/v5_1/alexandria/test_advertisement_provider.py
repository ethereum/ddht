import sqlite3

from async_service import background_trio_service
import pytest
import trio

from ddht.tools.factories.alexandria import AdvertisementFactory
from ddht.v5_1.alexandria.advertisement_db import AdvertisementDatabase
from ddht.v5_1.alexandria.advertisement_provider import AdvertisementProvider


@pytest.mark.trio
async def test_content_provider_serves_advertisements(
    alice, bob, alice_alexandria_network, bob_alexandria_client,
):
    content_key = b"test-content-key"
    advertisement_db = AdvertisementDatabase(sqlite3.connect(":memory:"))
    advertisements = tuple(
        AdvertisementFactory(content_key=content_key) for _ in range(5)
    )
    for advertisement in advertisements:
        advertisement_db.add(advertisement)

    advertisement_provider = AdvertisementProvider(
        bob_alexandria_client, (advertisement_db,),
    )
    async with background_trio_service(advertisement_provider):
        # this ensures that the subscription is in place.
        await advertisement_provider.ready()

        with trio.fail_after(2):
            result = await alice_alexandria_network.locate(
                bob.node_id, content_key=content_key,
            )
            assert set(result) == set(advertisements)


@pytest.mark.trio
async def test_content_provider_serves_unknown_content_key_request(
    alice, bob, alice_alexandria_network, bob_alexandria_client,
):
    advertisement_db = AdvertisementDatabase(sqlite3.connect(":memory:"))

    advertisement_provider = AdvertisementProvider(
        bob_alexandria_client, (advertisement_db,),
    )
    async with background_trio_service(advertisement_provider):
        # this ensures that the subscription is in place.
        await advertisement_provider.ready()

        with trio.fail_after(2):
            result = await alice_alexandria_network.locate(
                bob.node_id, content_key=b"test-content-key",
            )
            assert len(result) == 0
