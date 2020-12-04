from eth_enr import OldSequenceNumber
import pytest
import trio

from ddht.v5_1.constants import SESSION_IDLE_TIMEOUT


@pytest.mark.trio
async def test_dispatcher_detects_handshake_timeout_as_initiator(
    alice, bob, alice_client, bob_client, autojump_clock
):
    await bob_client.get_manager().stop()
    alice_client.pool.initiate_session(bob.endpoint, bob.node_id)

    with trio.fail_after(SESSION_IDLE_TIMEOUT + 1):
        async with alice.events.session_timeout.subscribe() as subscription:
            session = await subscription.receive()
            assert session.remote_node_id == bob.node_id


@pytest.mark.trio
async def test_dispatcher_detects_handshake_timeout_as_recipient(
    alice, bob, alice_client, bob_client, autojump_clock
):
    await bob_client.get_manager().stop()
    alice_client.pool.receive_session(bob.endpoint)

    with trio.fail_after(SESSION_IDLE_TIMEOUT + 1):
        async with alice.events.session_timeout.subscribe() as subscription:
            session = await subscription.receive()
            assert session.remote_node_id == bob.node_id


@pytest.mark.trio
async def test_dispatcher_initiates_new_session_when_packet_cannot_be_decoded(
    alice, bob, autojump_clock
):
    try:
        alice.enr_db.set_enr(bob.enr)
    except OldSequenceNumber:
        pass
    try:
        bob.enr_db.set_enr(alice.enr)
    except OldSequenceNumber:
        pass

    async with alice.client():
        async with bob.client() as bob_client:
            # initiate a fully completed session and then remove it from bob's side.
            async with alice.events.session_created.subscribe_and_wait():
                async with bob.events.session_handshake_complete.subscribe() as subscription:
                    await bob_client.send_ping(
                        endpoint=alice.endpoint, node_id=alice.node_id
                    )
                    session = await subscription.receive()
                    bob_client.pool.remove_session(session.id)

            # initiate another session but abort the handshake partway through
            with trio.fail_after(2):
                async with alice.events.session_created.subscribe_and_wait():
                    async with bob.events.session_created.subscribe_and_wait():
                        await bob_client.send_ping(
                            endpoint=alice.endpoint, node_id=alice.node_id
                        )


@pytest.mark.trio
async def test_dispatcher_detects_duplicate_handshakes(alice, bob, autojump_clock):
    try:
        alice.enr_db.set_enr(bob.enr)
    except OldSequenceNumber:
        pass
    try:
        bob.enr_db.set_enr(alice.enr)
    except OldSequenceNumber:
        pass

    async with alice.client() as alice_client:
        async with bob.client() as bob_client:
            # initiate a fully completed session and then remove it from bob's side.
            async with alice.events.session_created.subscribe_and_wait():
                async with bob.events.session_handshake_complete.subscribe() as subscription:
                    await bob_client.send_ping(
                        endpoint=alice.endpoint, node_id=alice.node_id
                    )
                    session = await subscription.receive()
                    bob_client.pool.remove_session(session.id)

            # initiate another session but abort the handshake partway through
            async with bob.events.session_created.subscribe() as subscription:
                async with alice.events.session_created.subscribe_and_wait():
                    await bob_client.send_ping(
                        endpoint=alice.endpoint, node_id=alice.node_id
                    )
                    session = await subscription.receive()
                bob_client.pool.remove_session(session.id)

            # now we can verify alice has one fully handshaked session and one partial session
            alice_sessions_with_bob = alice_client.pool.get_sessions_for_endpoint(
                bob.endpoint
            )
            assert len(alice_sessions_with_bob) == 2
            session_a, session_b = alice_sessions_with_bob
            if session_a.is_after_handshake:
                assert not session_b.is_after_handshake
                full_session, partial_session = session_a, session_b
            else:
                assert session_b.is_after_handshake
                full_session, partial_session = session_b, session_a

            # now bob initiates yet another handshake with alice, which upon
            # success, alice should remove the full handshake
            async with alice.events.session_handshake_complete.subscribe() as subscription:
                await bob_client.send_ping(
                    endpoint=alice.endpoint, node_id=alice.node_id
                )
                new_full_session = await subscription.receive()

            for _ in range(10):
                await trio.lowlevel.checkpoint()

            alice_sessions_with_bob = alice_client.pool.get_sessions_for_endpoint(
                bob.endpoint
            )
            assert len(alice_sessions_with_bob) == 2

            assert full_session not in alice_sessions_with_bob
            assert partial_session in alice_sessions_with_bob
            assert new_full_session in alice_sessions_with_bob
