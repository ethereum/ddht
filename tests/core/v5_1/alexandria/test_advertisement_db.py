import sqlite3

import pytest

from ddht.kademlia import compute_distance
from ddht.tools.factories.alexandria import AdvertisementFactory
from ddht.tools.factories.keys import PrivateKeyFactory
from ddht.tools.factories.node_id import NodeIDFactory
from ddht.v5_1.alexandria.advertisement_db import (
    AdvertisementDatabase,
    AdvertisementNotFound,
    check_advertisement_exists,
    get_advertisement_by_signature,
    insert_advertisement,
)
from ddht.v5_1.alexandria.advertisements import Advertisement
from ddht.v5_1.alexandria.content import content_key_to_content_id


def test_sqlite_insert_and_retrieve_advertisement(conn):
    advertisement = AdvertisementFactory()
    with pytest.raises(AdvertisementNotFound):
        get_advertisement_by_signature(conn, advertisement.signature)

    insert_advertisement(conn, advertisement)

    result = get_advertisement_by_signature(conn, advertisement.signature)
    assert result == advertisement


def test_sqlite_check_advertisement_exists(conn):
    advertisement = AdvertisementFactory()
    assert check_advertisement_exists(conn, advertisement.signature) is False

    insert_advertisement(conn, advertisement)

    assert check_advertisement_exists(conn, advertisement.signature) is True


def test_sqlite_db_validates_field_lengths(conn):
    private_key = PrivateKeyFactory()
    base = AdvertisementFactory(private_key=private_key)

    advertisement_long_content_key = Advertisement.create(
        content_key=b"\x12" * 161,
        hash_tree_root=base.hash_tree_root,
        private_key=private_key,
    )

    with pytest.raises(sqlite3.IntegrityError):
        insert_advertisement(conn, advertisement_long_content_key)

    advertisement_long_hash_tree_root = Advertisement.create(
        content_key=base.content_key,
        hash_tree_root=b"\x12" * 33,
        private_key=private_key,
    )

    with pytest.raises(sqlite3.IntegrityError):
        insert_advertisement(conn, advertisement_long_hash_tree_root)

    advertisement_short_hash_tree_root = Advertisement.create(
        content_key=base.content_key,
        hash_tree_root=b"\x12" * 31,
        private_key=private_key,
    )

    with pytest.raises(sqlite3.IntegrityError):
        insert_advertisement(conn, advertisement_short_hash_tree_root)


@pytest.fixture
def advertisement_db(conn):
    return AdvertisementDatabase(conn)


def test_advertisement_db_add_exists_and_remove(advertisement_db):
    advertisement = AdvertisementFactory()

    assert not advertisement_db.exists(advertisement)

    advertisement_db.add(advertisement)

    assert advertisement_db.exists(advertisement)

    assert advertisement_db.remove(advertisement) is True

    assert not advertisement_db.exists(advertisement)

    assert advertisement_db.remove(advertisement) is False


def test_advertisement_db_query(advertisement_db):
    private_key = PrivateKeyFactory()
    ad_a = AdvertisementFactory(private_key=private_key)
    # content_key (and content_id)
    ad_b = AdvertisementFactory(content_key=ad_a.content_key)

    # hash_tree_root
    ad_c = AdvertisementFactory(hash_tree_root=ad_a.hash_tree_root)

    # node_id
    ad_d = AdvertisementFactory(private_key=private_key)

    # node_id & content_key
    ad_e = AdvertisementFactory(private_key=private_key, content_key=ad_a.content_key)

    # node_id & hash_tree_root
    ad_f = AdvertisementFactory(
        private_key=private_key, hash_tree_root=ad_a.hash_tree_root
    )

    # content_key & hash_tree_root
    ad_g = AdvertisementFactory(
        hash_tree_root=ad_a.hash_tree_root, content_key=ad_a.content_key,
    )

    advertisement_db.add(ad_a)
    advertisement_db.add(ad_b)
    advertisement_db.add(ad_c)
    advertisement_db.add(ad_d)
    advertisement_db.add(ad_e)
    advertisement_db.add(ad_f)
    advertisement_db.add(ad_g)

    # should match all records
    result_a = set(advertisement_db.query())
    assert result_a == {ad_a, ad_b, ad_c, ad_d, ad_e, ad_f, ad_g}

    # should match all records with the node_id
    result_b = set(advertisement_db.query(node_id=ad_a.node_id))
    assert result_b == {ad_a, ad_d, ad_e, ad_f}

    # should match all records with the content_id or content_key
    result_c = set(advertisement_db.query(content_id=ad_a.content_id))
    assert result_c == {ad_a, ad_b, ad_e, ad_g}

    result_d = set(advertisement_db.query(content_key=ad_a.content_key))
    assert result_d == {ad_a, ad_b, ad_e, ad_g}

    # should match all records with the hash_tree_root
    result_e = set(advertisement_db.query(hash_tree_root=ad_a.hash_tree_root))
    assert result_e == {ad_a, ad_c, ad_f, ad_g}

    # should match only ad_b
    result_f = set(advertisement_db.query(node_id=ad_b.node_id))
    assert result_f == {ad_b}

    # should match only ad_c
    result_g = set(advertisement_db.query(node_id=ad_c.node_id))
    assert result_g == {ad_c}


def test_advertisement_db_get_closest_and_furthest(advertisement_db):
    node_id = NodeIDFactory()
    ads = AdvertisementFactory.create_batch(20)

    for ad in ads:
        advertisement_db.add(ad)

    expected_closest = tuple(
        ad
        for ad in sorted(ads, key=lambda ad: compute_distance(ad.content_id, node_id))
    )

    actual_closest = tuple(advertisement_db.closest(node_id))

    assert expected_closest == actual_closest

    actual_furthest = tuple(advertisement_db.furthest(node_id))

    expected_furthest = tuple(reversed(expected_closest))

    assert expected_furthest == actual_furthest


def test_advertisement_db_get_hash_tree_roots_for_content_id(advertisement_db):
    content_key = b"\x01testkey"
    content_id = content_key_to_content_id(content_key)

    assert set(advertisement_db.get_hash_tree_roots_for_content_id(content_id)) == set()

    hash_tree_root_a = b"\x02" * 32
    hash_tree_root_b = b"\x03" * 32
    hash_tree_root_c = b"\x04" * 32

    advertisement_db.add(
        AdvertisementFactory(content_key=content_key, hash_tree_root=hash_tree_root_a)
    )
    advertisement_db.add(
        AdvertisementFactory(content_key=content_key, hash_tree_root=hash_tree_root_a)
    )
    advertisement_db.add(
        AdvertisementFactory(content_key=content_key, hash_tree_root=hash_tree_root_b)
    )
    advertisement_db.add(
        AdvertisementFactory(content_key=content_key, hash_tree_root=hash_tree_root_b)
    )
    advertisement_db.add(
        AdvertisementFactory(content_key=content_key, hash_tree_root=hash_tree_root_c)
    )
    advertisement_db.add(
        AdvertisementFactory(content_key=content_key, hash_tree_root=hash_tree_root_c)
    )

    expected = {hash_tree_root_a, hash_tree_root_b, hash_tree_root_c}

    actual = set(advertisement_db.get_hash_tree_roots_for_content_id(content_id))

    assert actual == expected


def test_advertisement_db_count_api(advertisement_db):
    assert advertisement_db.count() == 0

    ad_a = AdvertisementFactory()
    advertisement_db.add(ad_a)

    assert advertisement_db.count() == 1

    for _ in range(3):
        advertisement_db.add(AdvertisementFactory())

    assert advertisement_db.count() == 4

    advertisement_db.remove(ad_a)

    assert advertisement_db.count() == 3


def test_advertisement_db_expired_api(advertisement_db):
    assert advertisement_db.count() == 0

    ad_a = AdvertisementFactory()
    advertisement_db.add(ad_a)

    ad_b = AdvertisementFactory.expired()
    advertisement_db.add(ad_b)

    ad_c = AdvertisementFactory()
    advertisement_db.add(ad_c)

    ad_d = AdvertisementFactory.expired()
    advertisement_db.add(ad_d)

    assert advertisement_db.count() == 4

    expired_ads = tuple(advertisement_db.expired())
    assert len(expired_ads) == 2
    assert ad_b in expired_ads
    assert ad_d in expired_ads

    for ad in expired_ads:
        advertisement_db.remove(ad)

    assert advertisement_db.count() == 2

    assert not tuple(advertisement_db.expired())
