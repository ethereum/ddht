import pytest
import trio

from ddht.v5_1.alexandria.messages import PingMessage, PongMessage
from ddht.v5_1.alexandria.payloads import PingPayload, PongPayload


@pytest.mark.trio
async def test_alexandria_subscription_via_talk_request(
    alice_alexandria, alice, bob, bob_alexandria
):
    async with bob_alexandria.subscribe(PingMessage) as subscription:
        await alice_alexandria.ping(bob.node_id)

        with trio.fail_after(1):
            message = await subscription.receive()

        assert isinstance(message.message, PingMessage)
        assert message.message.payload.enr_seq == alice.enr.sequence_number


@pytest.mark.trio
async def test_alexandria_subscription_via_talk_response(
    alice_alexandria, alice, bob_alexandria
):
    async with bob_alexandria.subscribe(PongMessage) as subscription:
        with trio.fail_after(1):
            await bob_alexandria.ping(alice.node_id)

            message = await subscription.receive()

        assert isinstance(message.message, PongMessage)
        assert message.message.payload.enr_seq == alice.enr.sequence_number


@pytest.mark.trio
async def test_alexandria_subscription_via_talk_request_protocol_mismatch(
    alice_network, alice, bob, bob_alexandria, autojump_clock
):
    async with bob_alexandria.subscribe(PingMessage) as subscription:
        message = PingMessage(PingPayload(alice.enr.sequence_number))
        data_payload = message.to_wire_bytes()
        await alice_network.client.send_talk_request(
            bob.node_id,
            bob.endpoint,
            protocol=b"wrong-protocol-id",
            payload=data_payload,
        )
        with pytest.raises(trio.TooSlowError):
            with trio.fail_after(1):
                message = await subscription.receive()


@pytest.mark.trio
async def test_alexandria_subscription_via_talk_response_unknown_request_id(
    alice_network, alice, bob, bob_alexandria, autojump_clock
):
    async with bob_alexandria.subscribe(PongMessage) as subscription:
        message = PongMessage(PongPayload(alice.enr.sequence_number))
        data_payload = message.to_wire_bytes()
        await alice_network.client.send_talk_response(
            bob.node_id,
            bob.endpoint,
            payload=data_payload,
            request_id=b"\x01\x02\x03",  # unknown/unexpected request_id
        )
        with pytest.raises(trio.TooSlowError):
            with trio.fail_after(1):
                message = await subscription.receive()
