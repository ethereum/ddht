from contextlib import AsyncExitStack

import pytest
import trio

from ddht.base_message import InboundMessage
from ddht.subscription_manager import SubscriptionManager
from ddht.tools.factories.endpoint import EndpointFactory
from ddht.tools.factories.node_id import NodeIDFactory


class MessageForTesting:
    request_id = b"\x01"

    def __init__(self, id):
        self.id = id


class OtherMessageForTesting:
    request_id = b"\x01"

    def __init__(self, id):
        self.id = id


@pytest.mark.trio
async def test_subscribe_delivers_messages():
    node_id = NodeIDFactory()
    endpoint = EndpointFactory()
    manager = SubscriptionManager()

    async with manager.subscribe(MessageForTesting) as subscription:
        with pytest.raises(trio.WouldBlock):
            subscription.receive_nowait()

        manager.feed_subscriptions(
            InboundMessage(
                message=OtherMessageForTesting(1234),
                sender_node_id=node_id,
                sender_endpoint=endpoint,
            )
        )
        manager.feed_subscriptions(
            InboundMessage(
                message=MessageForTesting(1234),
                sender_node_id=node_id,
                sender_endpoint=endpoint,
            )
        )

        with trio.fail_after(1):
            message = await subscription.receive()

        assert isinstance(message.message, MessageForTesting)
        assert message.message.id == 1234

        with pytest.raises(trio.WouldBlock):
            subscription.receive_nowait()

    manager.feed_subscriptions(
        InboundMessage(
            message=MessageForTesting(1234),
            sender_node_id=node_id,
            sender_endpoint=endpoint,
        )
    )
    with pytest.raises(trio.ClosedResourceError):
        subscription.receive_nowait()


@pytest.mark.trio
async def test_subscribe_filters_by_node_id_and_endpoint():
    node_id = NodeIDFactory()
    endpoint = EndpointFactory()
    manager = SubscriptionManager()

    async with AsyncExitStack() as stack:

        subscription_a = await stack.enter_async_context(
            manager.subscribe(MessageForTesting)
        )
        subscription_b = await stack.enter_async_context(
            manager.subscribe(MessageForTesting, node_id=node_id)
        )
        subscription_c = await stack.enter_async_context(
            manager.subscribe(MessageForTesting, node_id=node_id, endpoint=endpoint)
        )

        with pytest.raises(trio.WouldBlock):
            subscription_a.receive_nowait()
        with pytest.raises(trio.WouldBlock):
            subscription_b.receive_nowait()
        with pytest.raises(trio.WouldBlock):
            subscription_c.receive_nowait()

        # One Message that doesn't match the message type
        manager.feed_subscriptions(
            InboundMessage(
                message=OtherMessageForTesting(1234),
                sender_node_id=node_id,
                sender_endpoint=endpoint,
            )
        )

        # One Message that only matches the message type
        manager.feed_subscriptions(
            InboundMessage(
                message=MessageForTesting(1),
                sender_node_id=NodeIDFactory(),
                sender_endpoint=EndpointFactory(),
            )
        )

        # One Message that matches the message type AND node_id
        manager.feed_subscriptions(
            InboundMessage(
                message=MessageForTesting(2),
                sender_node_id=node_id,
                sender_endpoint=EndpointFactory(),
            )
        )

        # One Message that matches the message type AND node_id AND endpoint
        manager.feed_subscriptions(
            InboundMessage(
                message=MessageForTesting(3),
                sender_node_id=node_id,
                sender_endpoint=endpoint,
            )
        )

        # now grab all the messages
        with trio.fail_after(1):
            message_a_0 = await subscription_a.receive()
            message_a_1 = await subscription_a.receive()
            message_a_2 = await subscription_a.receive()

            message_b_0 = await subscription_b.receive()
            message_b_1 = await subscription_b.receive()

            message_c_0 = await subscription_c.receive()

        # all of the subscriptions should now be empty
        with pytest.raises(trio.WouldBlock):
            subscription_a.receive_nowait()
        with pytest.raises(trio.WouldBlock):
            subscription_b.receive_nowait()
        with pytest.raises(trio.WouldBlock):
            subscription_c.receive_nowait()

        assert message_a_0.message.id == 1
        assert message_a_1.message.id == 2
        assert message_a_2.message.id == 3

        assert message_b_0.message.id == 2
        assert message_b_1.message.id == 3

        assert message_c_0.message.id == 3
