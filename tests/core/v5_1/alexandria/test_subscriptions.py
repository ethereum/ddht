import pytest
import trio

from ddht.v5_1.alexandria.messages import PingMessage, PongMessage
from ddht.v5_1.alexandria.payloads import PingPayload, PongPayload


@pytest.mark.trio
async def test_alexandria_client_subscription_via_talk_request(
    alice, bob, alice_alexandria_client, bob_alexandria_client,
):
    async with bob_alexandria_client.subscribe(PingMessage) as subscription:
        await alice_alexandria_client.send_ping(
            bob.node_id,
            bob.endpoint,
            enr_seq=alice.enr.sequence_number,
            advertisement_radius=1234,
        )

        with trio.fail_after(1):
            message = await subscription.receive()

        assert isinstance(message.message, PingMessage)
        assert message.message.payload.enr_seq == alice.enr.sequence_number
        assert message.message.payload.advertisement_radius == 1234


@pytest.mark.trio
async def test_alexandria_client_subscription_via_talk_response(
    alice, bob, alice_alexandria_client, bob_alexandria_client,
):
    async with bob_alexandria_client.subscribe(PongMessage) as subscription:
        with bob_alexandria_client.request_tracker.reserve_request_id(
            alice.node_id, b"\x01\x02"
        ):
            with trio.fail_after(1):
                await alice_alexandria_client.send_pong(
                    bob.node_id,
                    bob.endpoint,
                    enr_seq=alice.enr.sequence_number,
                    advertisement_radius=1234,
                    request_id=b"\x01\x02",
                )

                message = await subscription.receive()

            assert isinstance(message.message, PongMessage)
            assert message.message.payload.enr_seq == alice.enr.sequence_number
            assert message.message.payload.advertisement_radius == 1234


@pytest.mark.trio
async def test_alexandria_client_subscription_via_talk_request_protocol_mismatch(
    alice_network, alice, bob, bob_alexandria_client, autojump_clock
):
    async with bob_alexandria_client.subscribe(PingMessage) as subscription:
        message = PingMessage(PingPayload(alice.enr.sequence_number, 1234))
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
async def test_alexandria_client_subscription_via_talk_response_unknown_request_id(
    alice_network, alice, bob, bob_alexandria_client, autojump_clock
):
    async with bob_alexandria_client.subscribe(PongMessage) as subscription:
        message = PongMessage(PongPayload(alice.enr.sequence_number, 1234))
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
