import pytest
import trio

from ddht.v5_1.alexandria.messages import PingMessage, PongMessage, decode_message
from ddht.v5_1.messages import TalkRequestMessage, TalkResponseMessage


@pytest.mark.trio
async def test_alexandria_client_send_ping(bob, bob_network, alice_alexandria_client):
    async with bob_network.dispatcher.subscribe(TalkRequestMessage) as subscription:
        await alice_alexandria_client.send_ping(bob.node_id, bob.endpoint, enr_seq=100)
        with trio.fail_after(1):
            talk_response = await subscription.receive()
        message = decode_message(talk_response.message.payload)
        assert isinstance(message, PingMessage)
        assert message.payload.enr_seq == 100


@pytest.mark.trio
async def test_alexandria_client_send_pong(bob, bob_network, alice_alexandria_client):
    async with bob_network.dispatcher.subscribe(TalkResponseMessage) as subscription:
        await alice_alexandria_client.send_pong(
            bob.node_id, bob.endpoint, enr_seq=100, request_id=b"\x01\x02"
        )
        with trio.fail_after(1):
            talk_response = await subscription.receive()
        message = decode_message(talk_response.message.payload)
        assert isinstance(message, PongMessage)
        assert message.payload.enr_seq == 100


@pytest.mark.trio
async def test_alexandria_client_ping_request_response(
    alice, bob, bob_network, alice_alexandria_client, bob_alexandria_client,
):
    async def _respond():
        request = await subscription.receive()
        await bob_alexandria_client.send_pong(
            alice.node_id,
            alice.endpoint,
            enr_seq=bob.enr.sequence_number,
            request_id=request.request_id,
        )

    async with bob_network.dispatcher.subscribe(TalkRequestMessage) as subscription:
        async with trio.open_nursery() as nursery:
            nursery.start_soon(_respond)

            with trio.fail_after(1):
                pong_message = await alice_alexandria_client.ping(
                    bob.node_id, bob.endpoint
                )
                assert isinstance(pong_message, PongMessage)
                assert pong_message.payload.enr_seq == bob.enr.sequence_number


@pytest.mark.trio
async def test_alexandria_api_ping_request_response_request_id_mismatch(
    alice_network,
    alice,
    bob,
    bob_network,
    autojump_clock,
    bob_alexandria_client,
    alice_alexandria_client,
):
    async def _respond_wrong_request_id():
        await subscription.receive()
        await bob_alexandria_client.send_pong(
            alice.node_id,
            alice.endpoint,
            enr_seq=bob.enr.sequence_number,
            request_id=alice_network.client.request_tracker.get_free_request_id(
                bob.node_id
            ),
        )

    async with bob_network.dispatcher.subscribe(TalkRequestMessage) as subscription:
        async with trio.open_nursery() as nursery:
            nursery.start_soon(_respond_wrong_request_id)

            with pytest.raises(trio.TooSlowError):
                with trio.fail_after(1):
                    await alice_alexandria_client.ping(bob.node_id, bob.endpoint)
