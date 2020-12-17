import functools
from typing import (
    Any,
    AsyncContextManager,
    AsyncIterator,
    Collection,
    List,
    Optional,
    Sequence,
    Set,
    Tuple,
    Type,
)

from async_generator import asynccontextmanager
from async_service import Service
from eth_enr import ENRAPI
from eth_keys import keys
from eth_typing import NodeID
from eth_utils import ValidationError, get_extended_debug_logger
import trio

from ddht.base_message import InboundMessage
from ddht.endpoint import Endpoint
from ddht.enr import partition_enrs
from ddht.exceptions import DecodingError
from ddht.kademlia import compute_log_distance
from ddht.request_tracker import RequestTracker
from ddht.subscription_manager import SubscriptionManager
from ddht.v5_1.abc import NetworkAPI
from ddht.v5_1.alexandria.abc import AlexandriaClientAPI
from ddht.v5_1.alexandria.advertisements import Advertisement, partition_advertisements
from ddht.v5_1.alexandria.constants import ALEXANDRIA_PROTOCOL_ID, MAX_PAYLOAD_SIZE
from ddht.v5_1.alexandria.messages import (
    AckMessage,
    AdvertiseMessage,
    AlexandriaMessage,
    AlexandriaMessageType,
    ContentMessage,
    FindNodesMessage,
    FoundNodesMessage,
    GetContentMessage,
    LocateMessage,
    LocationsMessage,
    PingMessage,
    PongMessage,
    TAlexandriaMessage,
    decode_message,
)
from ddht.v5_1.alexandria.payloads import (
    AckPayload,
    ContentPayload,
    FindNodesPayload,
    FoundNodesPayload,
    GetContentPayload,
    LocatePayload,
    LocationsPayload,
    PingPayload,
    PongPayload,
)
from ddht.v5_1.alexandria.typing import ContentKey
from ddht.v5_1.constants import REQUEST_RESPONSE_TIMEOUT
from ddht.v5_1.messages import TalkRequestMessage, TalkResponseMessage


class AlexandriaClient(Service, AlexandriaClientAPI):
    protocol_id = ALEXANDRIA_PROTOCOL_ID

    _active_request_ids: Set[bytes]

    def __init__(self, network: NetworkAPI) -> None:
        self.logger = get_extended_debug_logger("ddht.AlexandriaClient")

        self.network = network
        self.request_tracker = RequestTracker()
        self.subscription_manager = SubscriptionManager()

        network.add_talk_protocol(self)

        self._active_request_ids = set()

    @property
    def local_private_key(self) -> keys.PrivateKey:
        return self.network.client.local_private_key

    #
    # Service API
    #
    async def run(self) -> None:
        self.manager.run_daemon_task(self._feed_talk_requests)
        self.manager.run_daemon_task(self._feed_talk_responses)

        await self.manager.wait_finished()

    #
    # Request/Response message sending primatives
    #
    async def _send_request(
        self,
        node_id: NodeID,
        endpoint: Endpoint,
        message: AlexandriaMessage[Any],
        *,
        request_id: Optional[bytes] = None,
    ) -> bytes:
        data_payload = message.to_wire_bytes()
        if len(data_payload) > MAX_PAYLOAD_SIZE:
            raise Exception(
                "Payload too large:  payload=%d  max_size=%d",
                len(data_payload),
                MAX_PAYLOAD_SIZE,
            )
        request_id = await self.network.client.send_talk_request(
            node_id,
            endpoint,
            protocol=ALEXANDRIA_PROTOCOL_ID,
            payload=data_payload,
            request_id=request_id,
        )
        return request_id

    async def _send_response(
        self,
        node_id: NodeID,
        endpoint: Endpoint,
        message: AlexandriaMessage[Any],
        *,
        request_id: bytes,
    ) -> None:
        if message.type != AlexandriaMessageType.RESPONSE:
            raise TypeError("%s is not of type RESPONSE", message)
        data_payload = message.to_wire_bytes()
        if len(data_payload) > MAX_PAYLOAD_SIZE:
            raise Exception(
                f"Payload too large:  payload={len(data_payload)}  max_size={MAX_PAYLOAD_SIZE}"
            )
        await self.network.client.send_talk_response(
            node_id, endpoint, payload=data_payload, request_id=request_id,
        )

    async def _request(
        self,
        node_id: NodeID,
        endpoint: Endpoint,
        request: AlexandriaMessage[Any],
        response_class: Type[TAlexandriaMessage],
        request_id: Optional[bytes] = None,
    ) -> TAlexandriaMessage:
        #
        # Request ID Shenanigans
        #
        # We need a subscription API for alexandria messages in order to be
        # able to cleanly implement functionality like responding to ping
        # messages with pong messages.
        #
        # We accomplish this by monitoring all TALKREQUEST and TALKRESPONSE
        # messages that occur on the alexandria protocol. For TALKREQUEST based
        # messages we can naively monitor incoming messages and match them
        # against the protocol_id. For the TALKRESPONSE however, the only way
        # for us to know that an incoming message belongs to this protocol is
        # to match it against an in-flight request (which we implicitely know
        # is part of the alexandria protocol).
        #
        # In order to do this, we need to know which request ids are active for
        # Alexandria protocol messages. We do this by using a separate
        # RequestTrackerAPI. The `request_id` for messages is still acquired
        # from the core tracker located on the base protocol `ClientAPI`.  We
        # then feed this into our local tracker, which allows us to query it
        # upon receiving an incoming TALKRESPONSE to see if the response is to
        # a message from this protocol.
        if request.type != AlexandriaMessageType.REQUEST:
            raise TypeError("%s is not of type REQUEST", request)
        if request_id is None:
            request_id = self.network.client.request_tracker.get_free_request_id(
                node_id
            )
        request_data = request.to_wire_bytes()
        if len(request_data) > MAX_PAYLOAD_SIZE:
            raise Exception(
                "Payload too large:  payload=%d  max_size=%d",
                len(request_data),
                MAX_PAYLOAD_SIZE,
            )
        with self.request_tracker.reserve_request_id(node_id, request_id):
            response_data = await self.network.talk(
                node_id,
                protocol=ALEXANDRIA_PROTOCOL_ID,
                payload=request_data,
                endpoint=endpoint,
                request_id=request_id,
            )

        response = decode_message(response_data)
        if type(response) is not response_class:
            raise DecodingError(
                f"Invalid response. expected={response_class}  got={type(response)}"
            )
        return response  # type: ignore

    def subscribe(
        self,
        message_type: Type[TAlexandriaMessage],
        endpoint: Optional[Endpoint] = None,
        node_id: Optional[NodeID] = None,
    ) -> AsyncContextManager[
        trio.abc.ReceiveChannel[InboundMessage[TAlexandriaMessage]]
    ]:
        return self.subscription_manager.subscribe(message_type, endpoint, node_id,)  # type: ignore

    @asynccontextmanager
    async def subscribe_request(
        self,
        node_id: NodeID,
        endpoint: Endpoint,
        request: AlexandriaMessage[Any],
        response_message_type: Type[TAlexandriaMessage],
        request_id: Optional[bytes],
    ) -> AsyncIterator[trio.abc.ReceiveChannel[InboundMessage[TAlexandriaMessage]]]:
        send_channel, receive_channel = trio.open_memory_channel[
            InboundMessage[TAlexandriaMessage]
        ](256)

        #
        # START
        #
        # There cannot be any `await/async` calls between here and the `END`
        # marker, otherwise we will be subject to a race condition where
        # another request could collide with this request id.
        #
        if request_id is None:
            request_id = self.network.client.request_tracker.get_free_request_id(
                node_id
            )

        self.logger.debug2(
            "Sending request: %s with request id %s", request, request_id.hex(),
        )

        with self.request_tracker.reserve_request_id(node_id, request_id):
            #
            # END
            #
            async with trio.open_nursery() as nursery:
                # The use of `functools.partial` below is due to an inadequacy
                # in the type hinting of `trio.Nursery.start_soon` which
                # doesn't support more than 4 positional argumeents.
                nursery.start_soon(
                    functools.partial(
                        self._manage_request_response,
                        node_id,
                        endpoint,
                        request,
                        response_message_type,
                        send_channel,
                        request_id,
                    )
                )
                try:
                    async with receive_channel:
                        try:
                            yield receive_channel
                        # Wrap EOC error with TSE to make the timeouts obvious
                        except trio.EndOfChannel as err:
                            raise trio.TooSlowError(
                                f"Timeout: request={request}  request_id={request_id.hex()}"
                            ) from err
                finally:
                    nursery.cancel_scope.cancel()

    async def _manage_request_response(
        self,
        node_id: NodeID,
        endpoint: Endpoint,
        request: AlexandriaMessage[Any],
        response_message_type: Type[AlexandriaMessage[Any]],
        send_channel: trio.abc.SendChannel[InboundMessage[AlexandriaMessage[Any]]],
        request_id: bytes,
    ) -> None:
        with trio.move_on_after(REQUEST_RESPONSE_TIMEOUT) as scope:
            subscription_ctx = self.subscription_manager.subscribe(
                response_message_type, endpoint, node_id,
            )
            async with subscription_ctx as subscription:
                self.logger.debug2(
                    "Sending request with request id %s", request_id.hex(),
                )
                # Send the request
                await self._send_request(
                    node_id, endpoint, request, request_id=request_id
                )

                # Wait for the response
                async with send_channel:
                    async for response in subscription:
                        if response.request_id != request_id:
                            continue
                        else:
                            await send_channel.send(response)
        if scope.cancelled_caught:
            self.logger.debug(
                "Abandoned request response monitor: request=%s message_type=%s",
                request,
                response_message_type,
            )

    #
    # Low Level Message Sending
    #
    async def send_ping(
        self,
        node_id: NodeID,
        endpoint: Endpoint,
        *,
        enr_seq: int,
        advertisement_radius: int,
        request_id: Optional[bytes] = None,
    ) -> bytes:
        message = PingMessage(PingPayload(enr_seq, advertisement_radius))
        return await self._send_request(
            node_id, endpoint, message, request_id=request_id
        )

    async def send_pong(
        self,
        node_id: NodeID,
        endpoint: Endpoint,
        *,
        enr_seq: int,
        advertisement_radius: int,
        request_id: bytes,
    ) -> None:
        message = PongMessage(PongPayload(enr_seq, advertisement_radius))
        await self._send_response(node_id, endpoint, message, request_id=request_id)

    async def send_find_nodes(
        self,
        node_id: NodeID,
        endpoint: Endpoint,
        *,
        distances: Collection[int],
        request_id: Optional[bytes] = None,
    ) -> bytes:
        message = FindNodesMessage(FindNodesPayload(tuple(distances)))
        return await self._send_request(
            node_id, endpoint, message, request_id=request_id
        )

    async def send_found_nodes(
        self,
        node_id: NodeID,
        endpoint: Endpoint,
        *,
        enrs: Sequence[ENRAPI],
        request_id: bytes,
    ) -> int:

        enr_batches = partition_enrs(enrs, max_payload_size=MAX_PAYLOAD_SIZE,)
        num_batches = len(enr_batches)
        for batch in enr_batches:
            message = FoundNodesMessage(FoundNodesPayload.from_enrs(num_batches, batch))
            await self._send_response(node_id, endpoint, message, request_id=request_id)

        return num_batches

    async def send_get_content(
        self,
        node_id: NodeID,
        endpoint: Endpoint,
        *,
        content_key: ContentKey,
        start_chunk_index: int,
        max_chunks: int,
        request_id: Optional[bytes] = None,
    ) -> bytes:
        message = GetContentMessage(
            GetContentPayload(content_key, start_chunk_index, max_chunks)
        )
        return await self._send_request(
            node_id, endpoint, message, request_id=request_id
        )

    async def send_content(
        self,
        node_id: NodeID,
        endpoint: Endpoint,
        *,
        is_proof: bool,
        payload: bytes,
        request_id: bytes,
    ) -> None:
        message = ContentMessage(ContentPayload(is_proof, payload))
        return await self._send_response(
            node_id, endpoint, message, request_id=request_id
        )

    async def send_advertisements(
        self,
        node_id: NodeID,
        endpoint: Endpoint,
        *,
        advertisements: Sequence[Advertisement],
        request_id: Optional[bytes] = None,
    ) -> bytes:
        message = AdvertiseMessage(tuple(advertisements))
        return await self._send_request(
            node_id, endpoint, message, request_id=request_id
        )

    async def send_ack(
        self,
        node_id: NodeID,
        endpoint: Endpoint,
        *,
        advertisement_radius: int,
        acked: Tuple[bool, ...],
        request_id: bytes,
    ) -> None:
        message = AckMessage(AckPayload(advertisement_radius, acked))
        return await self._send_response(
            node_id, endpoint, message, request_id=request_id
        )

    async def send_locate(
        self,
        node_id: NodeID,
        endpoint: Endpoint,
        *,
        content_key: ContentKey,
        request_id: bytes,
    ) -> bytes:
        message = LocateMessage(LocatePayload(content_key))
        return await self._send_request(
            node_id, endpoint, message, request_id=request_id
        )

    async def send_locations(
        self,
        node_id: NodeID,
        endpoint: Endpoint,
        *,
        advertisements: Sequence[Advertisement],
        request_id: bytes,
    ) -> int:
        # Here we use a slightly smaller MAX_PAYLOAD_SIZE to account for the
        # other fields in the message.
        advertisement_batches = partition_advertisements(
            advertisements, max_payload_size=MAX_PAYLOAD_SIZE - 8,
        )
        num_batches = len(advertisement_batches)
        for batch in advertisement_batches:
            message = LocationsMessage(LocationsPayload(num_batches, batch))
            await self._send_response(node_id, endpoint, message, request_id=request_id)

        return num_batches

    #
    # High Level Request/Response
    #
    async def ping(
        self,
        node_id: NodeID,
        endpoint: Endpoint,
        enr_seq: int,
        advertisement_radius: int,
        request_id: Optional[bytes] = None,
    ) -> PongMessage:
        request = PingMessage(PingPayload(enr_seq, advertisement_radius))
        response = await self._request(
            node_id, endpoint, request, PongMessage, request_id
        )
        return response

    async def find_nodes(
        self,
        node_id: NodeID,
        endpoint: Endpoint,
        distances: Collection[int],
        *,
        request_id: Optional[bytes] = None,
    ) -> Tuple[InboundMessage[FoundNodesMessage], ...]:
        request = FindNodesMessage(FindNodesPayload(tuple(distances)))

        subscription: trio.abc.ReceiveChannel[InboundMessage[FoundNodesMessage]]
        # unclear why `subscribe_request` isn't properly carrying the type information
        async with self.subscribe_request(  # type: ignore
            node_id, endpoint, request, FoundNodesMessage, request_id=request_id,
        ) as subscription:
            head_response = await subscription.receive()
            total = head_response.message.payload.total
            responses: Tuple[InboundMessage[FoundNodesMessage], ...]
            if total == 1:
                responses = (head_response,)
            elif total > 1:
                tail_responses: List[InboundMessage[FoundNodesMessage]] = []
                for _ in range(total - 1):
                    tail_responses.append(await subscription.receive())
                responses = (head_response,) + tuple(tail_responses)
            else:
                # TODO: this code path needs to be excercised and
                # probably replaced with some sort of
                # `SessionTerminated` exception.
                raise Exception("Invalid `total` counter in response")

            # Validate that all responses are indeed at one of the
            # specified distances.
            for response in responses:
                for enr in response.message.payload.enrs:
                    if enr.node_id == node_id:
                        if 0 not in distances:
                            raise ValidationError(
                                f"Invalid response: distance=0  expected={distances}"
                            )
                    else:
                        distance = compute_log_distance(enr.node_id, node_id)
                        if distance not in distances:
                            raise ValidationError(
                                f"Invalid response: distance={distance}  expected={distances}"
                            )

            return responses

    async def get_content(
        self,
        node_id: NodeID,
        endpoint: Endpoint,
        *,
        content_key: ContentKey,
        start_chunk_index: int,
        max_chunks: int,
        request_id: Optional[bytes] = None,
    ) -> ContentMessage:
        request = GetContentMessage(
            GetContentPayload(content_key, start_chunk_index, max_chunks)
        )
        response = await self._request(
            node_id, endpoint, request, ContentMessage, request_id
        )
        return response

    async def advertise(
        self,
        node_id: NodeID,
        endpoint: Endpoint,
        *,
        advertisements: Collection[Advertisement],
    ) -> AckMessage:
        if not advertisements:
            raise Exception("Must send at least one advertisement")
        message = AdvertiseMessage(tuple(advertisements))
        return await self._request(node_id, endpoint, message, AckMessage)

    async def locate(
        self,
        node_id: NodeID,
        endpoint: Endpoint,
        *,
        content_key: ContentKey,
        request_id: Optional[bytes] = None,
    ) -> Tuple[InboundMessage[LocationsMessage], ...]:
        stream_locate_ctx = self.stream_locate(
            node_id, endpoint, content_key=content_key, request_id=request_id,
        )
        async with stream_locate_ctx as response_aiter:
            return tuple([response async for response in response_aiter])

    @asynccontextmanager
    async def stream_locate(
        self,
        node_id: NodeID,
        endpoint: Endpoint,
        *,
        content_key: ContentKey,
        request_id: Optional[bytes] = None,
    ) -> AsyncIterator[trio.abc.ReceiveChannel[InboundMessage[LocationsMessage]]]:
        request = LocateMessage(LocatePayload(content_key))

        async def _feed_responses(
            send_channel: trio.abc.SendChannel[InboundMessage[LocationsMessage]],
        ) -> None:
            subscription: trio.abc.ReceiveChannel[InboundMessage[LocationsMessage]]
            # unclear why `subscribe_request` isn't properly carrying the type information
            async with self.subscribe_request(  # type: ignore
                node_id, endpoint, request, LocationsMessage, request_id=request_id,
            ) as subscription:
                async with send_channel:
                    head_response = await subscription.receive()
                    await send_channel.send(head_response)
                    total = head_response.message.payload.total
                    for _ in range(total - 1):
                        response = await subscription.receive()
                        await send_channel.send(response)

        send_channel, receive_channel = trio.open_memory_channel[
            InboundMessage[LocationsMessage]
        ](4)
        async with trio.open_nursery() as nursery:
            nursery.start_soon(_feed_responses, send_channel)

            async with receive_channel:
                yield receive_channel

            nursery.cancel_scope.cancel()

    #
    # Long Running Processes to manage subscriptions
    #
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
                    if message.type != AlexandriaMessageType.REQUEST:
                        self.logger.debug(
                            "Received non-REQUEST msg via TALKREQ: %s", message
                        )
                        continue

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
                is_known_request_id = self.request_tracker.is_request_id_active(
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
                    if message.type != AlexandriaMessageType.RESPONSE:
                        self.logger.debug(
                            "Received non-RESPONSE msg via TALKRESP: %s", message
                        )
                        continue

                    self.subscription_manager.feed_subscriptions(
                        InboundMessage(
                            message=message,
                            sender_node_id=response.sender_node_id,
                            sender_endpoint=response.sender_endpoint,
                            explicit_request_id=response.request_id,
                        )
                    )
