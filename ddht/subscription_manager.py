import collections
from typing import Any, AsyncIterator, DefaultDict, NamedTuple, Optional, Set, Type

from async_generator import asynccontextmanager
from eth_typing import NodeID
from eth_utils import get_extended_debug_logger
import trio

from ddht.abc import SubscriptionManagerAPI
from ddht.base_message import AnyInboundMessage, InboundMessage, TMessage
from ddht.endpoint import Endpoint


class _Subcription(NamedTuple):
    send_channel: trio.abc.SendChannel[AnyInboundMessage]
    filter_by_endpoint: Optional[Endpoint]
    filter_by_node_id: Optional[NodeID]


class SubscriptionManager(SubscriptionManagerAPI[TMessage]):
    _subscriptions: DefaultDict[Type[Any], Set[_Subcription]]

    def __init__(self) -> None:
        self.logger = get_extended_debug_logger("ddht.SubscriptionManager")

        self._subscriptions = collections.defaultdict(set)

    def feed_subscriptions(self, message: AnyInboundMessage) -> None:
        message_type = type(message.message)

        subscriptions = tuple(self._subscriptions[message_type])
        self.logger.debug2(
            "Handling %d subscriptions for message: %s", len(subscriptions), message,
        )
        for subscription in subscriptions:
            if subscription.filter_by_endpoint is not None:
                if message.sender_endpoint != subscription.filter_by_endpoint:
                    continue
            if subscription.filter_by_node_id is not None:
                if message.sender_node_id != subscription.filter_by_node_id:
                    continue

            try:
                subscription.send_channel.send_nowait(message)  # type: ignore
            except trio.WouldBlock:
                self.logger.debug(
                    "Discarding message for subscription %s due to full channel: %s",
                    subscription,
                    message,
                )
            except trio.BrokenResourceError:
                pass

    @asynccontextmanager
    async def subscribe(
        self,
        message_type: Type[TMessage],
        endpoint: Optional[Endpoint] = None,
        node_id: Optional[NodeID] = None,
    ) -> AsyncIterator[trio.abc.ReceiveChannel[InboundMessage[TMessage]]]:
        send_channel, receive_channel = trio.open_memory_channel[
            InboundMessage[TMessage]
        ](256)
        subscription = _Subcription(send_channel, endpoint, node_id)
        self._subscriptions[message_type].add(subscription)

        self.logger.debug2("Subscription setup for: %s", message_type)

        try:
            async with receive_channel:
                yield receive_channel
        finally:
            self._subscriptions[message_type].remove(subscription)
