import logging
from typing import AsyncContextManager, Optional, Type

from async_service import Service
from eth_enr import ENRManagerAPI
from eth_typing import NodeID
import trio

from ddht.base_message import InboundMessage
from ddht.endpoint import Endpoint
from ddht.exceptions import DecodingError
from ddht.subscription_manager import SubscriptionManager
from ddht.v5_1.abc import NetworkAPI
from ddht.v5_1.alexandria.abc import AlexandriaClientAPI, AlexandriaNetworkAPI
from ddht.v5_1.alexandria.client import AlexandriaClient
from ddht.v5_1.alexandria.messages import (
    PingMessage,
    TAlexandriaMessage,
    decode_message,
)
from ddht.v5_1.alexandria.payloads import PongPayload
from ddht.v5_1.messages import TalkRequestMessage, TalkResponseMessage


class AlexandriaNetwork(Service, AlexandriaNetworkAPI):
    logger = logging.getLogger("ddht.Alexandria")

    # Delegate to the AlexandriaClient for determining `protocol_id`
    protocol_id = AlexandriaClient.protocol_id

    _alexandria_client: AlexandriaClientAPI

    def __init__(self, client: AlexandriaClientAPI) -> None:
        self._alexandria_client = client

        self.subscription_manager = SubscriptionManager()

    @property
    def network(self) -> NetworkAPI:
        return self._alexandria_client.network

    @property
    def enr_manager(self) -> ENRManagerAPI:
        return self._alexandria_client.network.enr_manager

    def subscribe(
        self,
        message_type: Type[TAlexandriaMessage],
        endpoint: Optional[Endpoint] = None,
        node_id: Optional[NodeID] = None,
    ) -> AsyncContextManager[
        trio.abc.ReceiveChannel[InboundMessage[TAlexandriaMessage]]
    ]:
        return self.subscription_manager.subscribe(message_type, endpoint, node_id)  # type: ignore

    async def run(self) -> None:
        self.manager.run_daemon_task(self._feed_talk_requests)
        self.manager.run_daemon_task(self._feed_talk_responses)
        self.manager.run_daemon_task(self._pong_when_pinged)

        await self.manager.wait_finished()

    #
    # High Level API
    #
    async def ping(
        self, node_id: NodeID, *, endpoint: Optional[Endpoint] = None,
    ) -> PongPayload:
        response = await self._alexandria_client.ping(node_id, endpoint=endpoint)
        return response.payload

    #
    # Long Running Processes
    #
    async def _pong_when_pinged(self) -> None:
        async with self.subscribe(PingMessage) as subscription:
            async for request in subscription:
                await self._alexandria_client.send_pong(
                    request.sender_node_id,
                    request.sender_endpoint,
                    enr_seq=self.enr_manager.enr.sequence_number,
                    request_id=request.request_id,
                )

    async def _feed_talk_requests(self) -> None:
        async with self.network.client.dispatcher.subscribe(
            TalkRequestMessage
        ) as subscription:
            async for request in subscription:
                if request.message.protocol != self.protocol_id:
                    continue

                try:
                    message = decode_message(request.message.payload)
                except DecodingError:
                    pass
                else:
                    self.subscription_manager.feed_subscriptions(
                        InboundMessage(
                            message=message,
                            sender_node_id=request.sender_node_id,
                            sender_endpoint=request.sender_endpoint,
                            explicit_request_id=request.message.request_id,
                        )
                    )

    async def _feed_talk_responses(self) -> None:
        async with self.network.client.dispatcher.subscribe(
            TalkResponseMessage
        ) as subscription:
            async for response in subscription:
                is_known_request_id = self._alexandria_client.request_tracker.is_request_id_active(
                    response.sender_node_id, response.request_id,
                )
                if not is_known_request_id:
                    continue
                elif not response.message.payload:
                    continue

                try:
                    message = decode_message(response.message.payload)
                except DecodingError:
                    pass
                else:
                    self.subscription_manager.feed_subscriptions(
                        InboundMessage(
                            message=message,
                            sender_node_id=response.sender_node_id,
                            sender_endpoint=response.sender_endpoint,
                        )
                    )
