import pytest

from ddht.tools.factories.alexandria import AdvertisementFactory
from ddht.v5_1.alexandria.constants import MAX_RADIUS
from ddht.v5_1.alexandria.content import compute_content_distance


#
# AdvertisementManagerAPI.check_interest
#
@pytest.mark.trio
async def test_local_advertisement_manager_advertisement_radius(
    bob, alice_alexandria_network,
):
    ad_manager = alice_alexandria_network.local_advertisement_manager
    assert ad_manager.max_advertisement_count is None
    assert ad_manager.advertisement_radius == MAX_RADIUS


@pytest.mark.trio
async def test_remote_advertisement_manager_advertisement_radius(
    alice, alice_alexandria_network, autojump_clock,
):
    ad_manager = alice_alexandria_network.remote_advertisement_manager
    assert ad_manager.max_advertisement_count <= 32
    assert ad_manager.advertisement_radius == MAX_RADIUS

    advertisements = tuple(
        sorted(
            (
                AdvertisementFactory()
                for _ in range(alice_alexandria_network.max_advertisement_count + 1)
            ),
            key=lambda ad: compute_content_distance(alice.node_id, ad.content_id),
        )
    )
    for advertisement in advertisements:
        ad_manager.advertisement_db.add(advertisement)

    await ad_manager.purge_distant_ads()

    expected_radius = compute_content_distance(
        advertisements[-2].content_id, alice.node_id
    )
    assert expected_radius < MAX_RADIUS
    assert ad_manager.advertisement_radius == expected_radius
