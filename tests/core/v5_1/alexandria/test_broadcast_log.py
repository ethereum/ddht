import pytest

from ddht.tools.factories.alexandria import AdvertisementFactory
from ddht.v5_1.alexandria.broadcast_log import BroadcastLog


@pytest.mark.trio
async def test_broadcast_log(alice, bob):
    ad_a = AdvertisementFactory(private_key=alice.private_key)
    ad_b = AdvertisementFactory(private_key=alice.private_key)
    ad_c = AdvertisementFactory(private_key=alice.private_key)
    ad_d = AdvertisementFactory(private_key=bob.private_key)
    ad_e = AdvertisementFactory(private_key=bob.private_key)

    all_ads = (ad_a, ad_b, ad_c, ad_d, ad_e)

    broadcast_log = BroadcastLog(max_records=8)

    # not logged when empty
    for ad in all_ads:
        assert not broadcast_log.was_logged(alice.node_id, ad)
        assert not broadcast_log.was_logged(bob.node_id, ad)

    broadcast_log.log(alice.node_id, ad_a)

    assert broadcast_log.was_logged(alice.node_id, ad_a)
    assert not broadcast_log.was_logged(bob.node_id, ad_a)

    # we insert 8 more, which should evict alice's entries for `ad_a`
    for ad in all_ads[1:]:
        broadcast_log.log(alice.node_id, ad)
        broadcast_log.log(bob.node_id, ad)

    assert not broadcast_log.was_logged(alice.node_id, ad_a)
