from eth_utils import ValidationError
import pytest
import trio

from ddht.tools.factories.alexandria import AdvertisementFactory
from ddht.tools.factories.content import ContentFactory
from ddht.v5_1.alexandria.content import compute_content_distance
from ddht.v5_1.alexandria.messages import GetContentMessage
from ddht.v5_1.alexandria.partials.proof import compute_proof
from ddht.v5_1.alexandria.sedes import content_sedes


#
# AdvertisementManagerAPI.check_interest
#
@pytest.mark.trio
async def test_advertisement_collector_check_interest_already_in_local_db(
    alice, alice_alexandria_network,
):
    ad_collector = alice_alexandria_network.advertisement_collector
    advertisement = AdvertisementFactory(private_key=alice.private_key)

    assert ad_collector.check_interest(advertisement) is True

    alice_alexandria_network.local_advertisement_db.add(advertisement)

    assert ad_collector.check_interest(advertisement) is False


@pytest.mark.trio
async def test_advertisement_collector_check_interest_already_in_remote_db(
    bob, alice_alexandria_network,
):
    ad_collector = alice_alexandria_network.advertisement_collector
    advertisement = AdvertisementFactory(private_key=bob.private_key)

    assert ad_collector.check_interest(advertisement) is True

    alice_alexandria_network.remote_advertisement_db.add(advertisement)

    assert ad_collector.check_interest(advertisement) is False


@pytest.mark.trio
async def test_advertisement_collector_check_interest_outside_radius(
    alice, alice_alexandria_network,
):
    ad_collector = alice_alexandria_network.advertisement_collector
    advertisement_db = alice_alexandria_network.remote_advertisement_db
    assert alice_alexandria_network.max_advertisement_count <= 32

    advertisements = tuple(
        sorted(
            (
                AdvertisementFactory()
                for _ in range(alice_alexandria_network.max_advertisement_count + 1)
            ),
            key=lambda ad: compute_content_distance(ad.content_id, alice.node_id),
        )
    )
    furthest = advertisements[-1]

    assert alice_alexandria_network.local_advertisement_radius == 2 ** 256 - 1
    assert ad_collector.check_interest(furthest) is True

    for ad in advertisements[: alice_alexandria_network.max_advertisement_count]:
        advertisement_db.add(ad)

    expected_radius = compute_content_distance(
        advertisements[-2].content_id, alice.node_id
    )
    assert expected_radius < 2 ** 256 - 1

    assert alice_alexandria_network.local_advertisement_radius == expected_radius
    assert ad_collector.check_interest(furthest) is False


#
# Generic validation checks
#
@pytest.mark.trio
async def test_advertisement_collector_validate_already_in_local_database(
    alice, alice_alexandria_network,
):
    ad_collector = alice_alexandria_network.advertisement_collector
    advertisement = AdvertisementFactory(private_key=alice.private_key)
    alice_alexandria_network.local_advertisement_db.add(advertisement)

    with pytest.raises(ValidationError, match="known"):
        await ad_collector.validate_advertisement(advertisement)


@pytest.mark.trio
async def test_advertisement_collector_validate_already_in_remote_database(
    bob, alice_alexandria_network,
):
    ad_collector = alice_alexandria_network.advertisement_collector
    advertisement = AdvertisementFactory(private_key=bob.private_key)
    alice_alexandria_network.remote_advertisement_db.add(advertisement)

    with pytest.raises(ValidationError, match="known"):
        await ad_collector.validate_advertisement(advertisement)


@pytest.mark.trio
async def test_advertisement_collector_validate_invalid_signature(
    alice_alexandria_network,
):
    ad_collector = alice_alexandria_network.advertisement_collector
    advertisement = AdvertisementFactory(
        signature_v=1, signature_r=1357924680, signature_s=2468013579,
    )

    with pytest.raises(ValidationError, match="signature"):
        await ad_collector.validate_advertisement(advertisement)


@pytest.mark.trio
async def test_advertisement_collector_validate_unknown_node_id(
    alice_alexandria_network,
):
    ad_collector = alice_alexandria_network.advertisement_collector

    with pytest.raises(ValidationError, match="liveliness"):
        await ad_collector.validate_advertisement(AdvertisementFactory())


@pytest.mark.trio
async def test_advertisement_collector_validate_mismatched_hash_tree_roots(
    bob, alice_alexandria_network,
):
    ad_collector = alice_alexandria_network.advertisement_collector

    advertisement_a = AdvertisementFactory(private_key=bob.private_key)
    advertisement_b = AdvertisementFactory(
        private_key=bob.private_key, content_key=advertisement_a.content_key,
    )
    assert advertisement_a.hash_tree_root != advertisement_b.hash_tree_root

    advertisement_db = alice_alexandria_network.remote_advertisement_db

    advertisement_db.add(advertisement_a)

    with pytest.raises(NotImplementedError):
        await ad_collector.validate_advertisement(advertisement_b)


@pytest.mark.trio
async def test_advertisement_collector_validate_multiple_mismatched_hash_tree_roots(
    bob, alice_alexandria_network,
):
    ad_collector = alice_alexandria_network.advertisement_collector

    advertisement_a = AdvertisementFactory(private_key=bob.private_key)
    advertisement_b = AdvertisementFactory(
        private_key=bob.private_key, content_key=advertisement_a.content_key,
    )
    advertisement_c = AdvertisementFactory(
        private_key=bob.private_key, content_key=advertisement_a.content_key,
    )

    assert advertisement_a.hash_tree_root != advertisement_b.hash_tree_root
    assert advertisement_b.hash_tree_root != advertisement_c.hash_tree_root

    advertisement_db = alice_alexandria_network.remote_advertisement_db

    advertisement_db.add(advertisement_a)
    advertisement_db.add(advertisement_b)

    with pytest.raises(NotImplementedError):
        await ad_collector.validate_advertisement(advertisement_c)


#
# Validation checks against *remote* ads
#
@pytest.mark.trio
async def test_advertisement_collector_validate_remote_expired(
    bob, alice_alexandria_network,
):
    ad_collector = alice_alexandria_network.advertisement_collector
    advertisement = AdvertisementFactory.expired(private_key=bob.private_key)

    with pytest.raises(ValidationError, match="expired"):
        await ad_collector.validate_advertisement(advertisement)


@pytest.mark.trio
async def test_advertisement_collector_validate_remote_node_unreachable(
    bob, alice_alexandria_network, autojump_clock
):
    ad_collector = alice_alexandria_network.advertisement_collector
    advertisement = AdvertisementFactory(private_key=bob.private_key)

    with pytest.raises(ValidationError, match="liveliness"):
        await ad_collector.validate_advertisement(advertisement)


@pytest.mark.trio
async def test_advertisement_collector_validate_remote_fail_initial_proof_check(
    bob, alice_alexandria_network, bob_alexandria_network, autojump_clock,
):
    ad_collector = alice_alexandria_network.advertisement_collector
    advertisement = AdvertisementFactory(private_key=bob.private_key)

    with pytest.raises(ValidationError, match="proof retrieval"):
        await ad_collector.validate_advertisement(advertisement)


@pytest.mark.trio
async def test_advertisement_collector_validate_remote_fail_custody_proof_check(
    bob, alice_alexandria_network, bob_alexandria_network, autojump_clock,
):
    content = ContentFactory(2048)
    proof = compute_proof(content, sedes=content_sedes)
    ad_collector = alice_alexandria_network.advertisement_collector
    advertisement = AdvertisementFactory(
        private_key=bob.private_key, hash_tree_root=proof.get_hash_tree_root(),
    )

    async with bob_alexandria_network.client.subscribe(
        GetContentMessage
    ) as subscription:
        did_serve_initial_proof = False

        async with trio.open_nursery() as nursery:
            did_serve_initial_proof = True

            async def _respond():
                request = await subscription.receive()
                partial = proof.to_partial(0, 64)
                await bob_alexandria_network.client.send_content(
                    request.sender_node_id,
                    request.sender_endpoint,
                    is_proof=True,
                    payload=partial.serialize(),
                    request_id=request.request_id,
                )

            nursery.start_soon(_respond)

            with pytest.raises(ValidationError, match="Proof of custody check failed"):
                await ad_collector.validate_advertisement(advertisement)

            assert did_serve_initial_proof is True


#
# Validation checks against *local* ads
#
@pytest.mark.trio
async def test_advertisement_collector_validate_local_expired(
    alice, alice_alexandria_network,
):
    ad_collector = alice_alexandria_network.advertisement_collector
    advertisement = AdvertisementFactory.expired(private_key=alice.private_key)

    with pytest.raises(ValidationError, match="expired"):
        await ad_collector.validate_advertisement(advertisement)


@pytest.mark.trio
async def test_advertisement_collector_validate_local_content_not_found(
    alice, alice_alexandria_network,
):
    ad_collector = alice_alexandria_network.advertisement_collector
    advertisement = AdvertisementFactory(private_key=alice.private_key)

    content_storage = alice_alexandria_network.commons_content_storage
    assert not content_storage.has_content(advertisement.content_key)

    with pytest.raises(ValidationError, match="not found"):
        await ad_collector.validate_advertisement(advertisement)


@pytest.mark.trio
async def test_advertisement_collector_validate_local_hash_tree_root_mismatch(
    alice, alice_alexandria_network,
):
    ad_collector = alice_alexandria_network.advertisement_collector
    advertisement = AdvertisementFactory(private_key=alice.private_key)

    content_storage = alice_alexandria_network.commons_content_storage
    content_storage.set_content(advertisement.content_key, ContentFactory())

    with pytest.raises(ValidationError, match="Mismatched roots"):
        await ad_collector.validate_advertisement(advertisement)


#
# Tests against actual handling
#
@pytest.mark.trio
async def test_advertisement_collector_handle_new_valid_local_advertisement(
    alice, alice_alexandria_network, bob_alexandria_network,
):
    content = ContentFactory(2048)
    proof = compute_proof(content, sedes=content_sedes)
    ad_collector = alice_alexandria_network.advertisement_collector
    advertisement = AdvertisementFactory(
        private_key=alice.private_key, hash_tree_root=proof.get_hash_tree_root(),
    )
    alice_alexandria_network.commons_content_storage.set_content(
        advertisement.content_key, content,
    )

    assert not alice_alexandria_network.local_advertisement_db.exists(advertisement)

    with trio.fail_after(5):
        await ad_collector.handle_advertisement(advertisement)

    assert alice_alexandria_network.local_advertisement_db.exists(advertisement)


@pytest.mark.trio
async def test_advertisement_collector_handle_new_valid_remote_advertisement(
    alice, bob, alice_alexandria_network, bob_alexandria_network,
):
    content = ContentFactory(2048)
    proof = compute_proof(content, sedes=content_sedes)
    ad_collector = alice_alexandria_network.advertisement_collector
    advertisement = AdvertisementFactory(
        private_key=bob.private_key, hash_tree_root=proof.get_hash_tree_root(),
    )
    bob_alexandria_network.commons_content_storage.set_content(
        advertisement.content_key, content,
    )

    assert not alice_alexandria_network.remote_advertisement_db.exists(advertisement)

    with trio.fail_after(5):
        async with ad_collector.new_advertisement.subscribe_and_wait():
            await ad_collector.handle_advertisement(advertisement)

    assert alice_alexandria_network.remote_advertisement_db.exists(advertisement)


@pytest.mark.trio
async def test_advertisement_collector_handle_existing_valid_local_advertisement(
    alice, alice_alexandria_network, bob_alexandria_network,
):
    content = ContentFactory(2048)
    proof = compute_proof(content, sedes=content_sedes)
    ad_collector = alice_alexandria_network.advertisement_collector
    advertisement = AdvertisementFactory(
        private_key=alice.private_key, hash_tree_root=proof.get_hash_tree_root(),
    )
    alice_alexandria_network.commons_content_storage.set_content(
        advertisement.content_key, content,
    )
    alice_alexandria_network.local_advertisement_db.add(advertisement)

    assert alice_alexandria_network.local_advertisement_db.exists(advertisement)

    async with ad_collector.new_advertisement.subscribe() as subscription:
        with trio.fail_after(5):
            await ad_collector.handle_advertisement(advertisement)
        with pytest.raises(trio.TooSlowError):
            with trio.fail_after(5):
                await subscription.receive()

    assert alice_alexandria_network.local_advertisement_db.exists(advertisement)


@pytest.mark.trio
async def test_advertisement_collector_handle_existing_valid_remote_advertisement(
    alice, bob, alice_alexandria_network, bob_alexandria_network, autojump_clock,
):
    content = ContentFactory(2048)
    proof = compute_proof(content, sedes=content_sedes)
    ad_collector = alice_alexandria_network.advertisement_collector
    advertisement = AdvertisementFactory(
        private_key=bob.private_key, hash_tree_root=proof.get_hash_tree_root(),
    )
    bob_alexandria_network.commons_content_storage.set_content(
        advertisement.content_key, content,
    )

    alice_alexandria_network.remote_advertisement_db.add(advertisement)

    assert alice_alexandria_network.remote_advertisement_db.exists(advertisement)

    async with ad_collector.new_advertisement.subscribe() as subscription:
        with trio.fail_after(5):
            await ad_collector.handle_advertisement(advertisement)
        with pytest.raises(trio.TooSlowError):
            with trio.fail_after(5):
                await subscription.receive()

    assert alice_alexandria_network.remote_advertisement_db.exists(advertisement)


#
# Actual request handling
#
@pytest.mark.trio
async def test_advertisement_collector_acks_false_if_advertisements_already_known(
    alice, bob, alice_alexandria_client, bob_alexandria_network, autojump_clock,
):
    content = ContentFactory(2048)
    proof = compute_proof(content, sedes=content_sedes)
    advertisement = AdvertisementFactory(
        private_key=bob.private_key, hash_tree_root=proof.get_hash_tree_root(),
    )
    bob_alexandria_network.commons_content_storage.set_content(
        advertisement.content_key, content,
    )
    bob_alexandria_network.local_advertisement_db.add(advertisement)

    with pytest.raises(trio.TooSlowError):
        with trio.fail_after(10):
            async with bob_alexandria_network.advertisement_collector.new_advertisement.subscribe_and_wait():  # noqa: E501
                ack_message = await alice_alexandria_client.advertise(
                    bob.node_id, bob.endpoint, advertisements=(advertisement,),
                )

    assert len(ack_message.payload.acked) == 1
    assert ack_message.payload.acked[0] is False
