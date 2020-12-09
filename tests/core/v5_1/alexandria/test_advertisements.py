from eth_keys import keys
from eth_keys.exceptions import BadSignature
from eth_utils import keccak
from eth_utils.toolz import sliding_window
from hypothesis import given, settings
from hypothesis import strategies as st
import pytest
import ssz

from ddht.tools.factories.alexandria import AdvertisementFactory
from ddht.tools.factories.content import ContentFactory
from ddht.tools.factories.keys import PrivateKeyFactory
from ddht.v5_1.alexandria.advertisements import (
    ADVERTISEMENT_FIXED_SIZE,
    Advertisement,
    partition_advertisements,
)
from ddht.v5_1.alexandria.content import content_key_to_content_id
from ddht.v5_1.alexandria.sedes import AdvertisementSedes, AdvertiseSedes


def test_advertisement_creation():
    private_key = PrivateKeyFactory()
    content_key = b"\x01testkey"
    hash_tree_root = b"\x12" * 32
    ad = Advertisement.create(content_key, hash_tree_root, private_key)

    assert ad.content_id == content_key_to_content_id(content_key)

    signature = ad.signature

    assert signature.v == ad.signature_v
    assert signature.r == ad.signature_r
    assert signature.s == ad.signature_s

    assert ad.public_key == private_key.public_key

    assert ad.is_valid
    ad.verify()

    expected_node_id = keccak(private_key.public_key.to_bytes())
    assert ad.node_id == expected_node_id


def test_advertisement_with_invalid_signature():
    ad = AdvertisementFactory.invalid()

    assert not ad.is_valid

    with pytest.raises(BadSignature):
        ad.node_id
    with pytest.raises(BadSignature):
        ad.public_key
    with pytest.raises(BadSignature):
        ad.verify()


def test_advertisement_to_and_from_sedes_payload():
    advertisement = AdvertisementFactory()

    sedes_payload = advertisement.to_sedes_payload()
    result = Advertisement.from_sedes_payload(sedes_payload)

    assert result == advertisement


content_key_st = st.integers(min_value=33, max_value=128).map(ContentFactory)


@given(content_key=content_key_st,)
def test_advertisement_encoded_size(content_key):
    advertisement = AdvertisementFactory(content_key=content_key)
    encoded = ssz.encode(advertisement.to_sedes_payload(), sedes=AdvertisementSedes)

    assert len(encoded) == ADVERTISEMENT_FIXED_SIZE + len(content_key) - 4


@given(content_keys=st.lists(content_key_st),)
def test_advertise_message_encoded_size(content_keys):
    advertisements = tuple(
        AdvertisementFactory(content_key=content_key) for content_key in content_keys
    )
    ssz_payload = tuple(ad.to_sedes_payload() for ad in advertisements)
    encoded = ssz.encode(ssz_payload, sedes=AdvertiseSedes)

    num_advertisements = len(advertisements)

    assert len(encoded) == (
        num_advertisements * ADVERTISEMENT_FIXED_SIZE
        + sum(len(content_key) for content_key in content_keys)
    )


@settings(deadline=500)
@given(content_keys=st.lists(content_key_st))
def test_advertisement_partitioning(content_keys):
    advertisements = tuple(
        AdvertisementFactory(
            content_key=content_key, signature=keys.Signature(b"\x00" * 65)
        )
        for content_key in content_keys
    )
    batches = partition_advertisements(advertisements, 512)

    for batch in batches:
        ssz_payload = tuple(ad.to_sedes_payload() for ad in batch)
        encoded = ssz.encode(ssz_payload, sedes=AdvertiseSedes)
        assert len(encoded) <= 512

    for left, right in sliding_window(2, batches):
        ssz_payload = tuple(ad.to_sedes_payload() for ad in left + (right[0],))
        encoded = ssz.encode(ssz_payload, sedes=AdvertiseSedes)
        assert len(encoded) > 512
