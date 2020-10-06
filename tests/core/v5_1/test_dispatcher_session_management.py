import pytest
import trio

from ddht.v5_1.constants import SESSION_IDLE_TIMEOUT


@pytest.mark.trio
async def test_dispatcher_detects_handshake_timeout_as_initiator(
    alice, bob, autojump_clock
):
    alice.enr_db.set_enr(bob.enr)
    bob.enr_db.set_enr(alice.enr)

    async with alice.client() as alice_client:
        async with bob.client() as bob_client:
            async with alice.events.session_created.subscribe_and_wait():
                async with bob.events.packet_sent.subscribe() as subscription:
                    await alice_client.send_ping(
                        node_id=bob.node_id, endpoint=bob.endpoint,
                    )
                    _, envelope = await subscription.receive()
                    assert envelope.packet.is_who_are_you
                    bob_client.get_manager().cancel()
        # Here we know that the session with bob was created and that bob has terminated
        #
        # Now we wait for the timeout to be triggered
        with trio.fail_after(SESSION_IDLE_TIMEOUT + 1):
            async with alice.events.session_timeout.subscribe() as subscription:
                session = await subscription.receive()
                assert session.remote_node_id == bob.node_id


@pytest.mark.trio
async def test_dispatcher_detects_handshake_timeout_as_recipient(
    alice, bob, autojump_clock
):
    alice.enr_db.set_enr(bob.enr)
    bob.enr_db.set_enr(alice.enr)

    async with bob.client():
        async with alice.client() as alice_client:
            async with bob.events.session_created.subscribe_and_wait():
                async with alice.events.packet_received.subscribe() as subscription:
                    await alice_client.send_ping(
                        node_id=bob.node_id, endpoint=bob.endpoint,
                    )
                    async for _, envelope in subscription:
                        if envelope.packet.is_who_are_you:
                            alice_client.get_manager().cancel()
                            break
        # Here we know that the session with bob was created and that alice has
        # terminated, failing to send the final HandshakePacket
        #
        # Now we wait for the timeout to be triggered
        with trio.fail_after(SESSION_IDLE_TIMEOUT + 1):
            async with bob.events.session_timeout.subscribe() as subscription:
                session = await subscription.receive()
                assert session.remote_node_id == alice.node_id


@pytest.mark.trio
async def test_dispatcher_initiates_new_session_when_packet_cannot_be_decoded(
    alice, bob, autojump_clock
):
    alice.enr_db.set_enr(bob.enr)
    bob.enr_db.set_enr(alice.enr)

    async with alice.client():
        async with bob.client() as bob_client:
            # initiate a fully completed session and then remove it from bob's side.
            async with alice.events.session_created.subscribe_and_wait():
                async with bob.events.session_handshake_complete.subscribe() as subscription:
                    await bob_client.send_ping(
                        node_id=alice.node_id, endpoint=alice.endpoint,
                    )
                    session = await subscription.receive()
                    bob_client.pool.remove_session(session.id)

            # initiate another session but abort the handshake partway through
            with trio.fail_after(2):
                async with alice.events.session_created.subscribe_and_wait():
                    async with bob.events.session_created.subscribe_and_wait():
                        await bob_client.send_ping(
                            node_id=alice.node_id, endpoint=alice.endpoint,
                        )


@pytest.mark.trio
async def test_dispatcher_detects_duplicate_handshakes(alice, bob, autojump_clock):
    alice.enr_db.set_enr(bob.enr)
    bob.enr_db.set_enr(alice.enr)

    async with alice.client() as alice_client:
        async with bob.client() as bob_client:
            # initiate a fully completed session and then remove it from bob's side.
            async with alice.events.session_created.subscribe_and_wait():
                async with bob.events.session_handshake_complete.subscribe() as subscription:
                    await bob_client.send_ping(
                        node_id=alice.node_id, endpoint=alice.endpoint,
                    )
                    session = await subscription.receive()
                    bob_client.pool.remove_session(session.id)

            # initiate another session but abort the handshake partway through
            async with bob.events.session_created.subscribe() as subscription:
                async with alice.events.session_created.subscribe_and_wait():
                    await bob_client.send_ping(
                        node_id=alice.node_id, endpoint=alice.endpoint,
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
                    node_id=alice.node_id, endpoint=alice.endpoint,
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
