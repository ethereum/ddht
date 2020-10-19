from typing import Any, Optional, Set, Type

from eth_typing import NodeID

from ddht.endpoint import Endpoint
from ddht.exceptions import DecodingError
from ddht.request_tracker import RequestTracker
from ddht.v5_1.abc import NetworkAPI
from ddht.v5_1.alexandria.abc import AlexandriaClientAPI
from ddht.v5_1.alexandria.constants import ALEXANDRIA_PROTOCOL_ID
from ddht.v5_1.alexandria.messages import (
    AlexandriaMessage,
    PingMessage,
    PongMessage,
    TAlexandriaMessage,
    decode_message,
)
from ddht.v5_1.alexandria.payloads import PingPayload, PongPayload


class AlexandriaClient(AlexandriaClientAPI):
    protocol_id = ALEXANDRIA_PROTOCOL_ID

    _active_request_ids: Set[bytes]

    def __init__(self, network: NetworkAPI) -> None:
        self.network = network
        self.request_tracker = RequestTracker()

        self._active_request_ids = set()

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
        data_payload = message.to_wire_bytes()
        await self.network.client.send_talk_response(
            node_id, endpoint, payload=data_payload, request_id=request_id,
        )

    async def _request(
        self,
        node_id: NodeID,
        endpoint: Optional[Endpoint],
        request: AlexandriaMessage[Any],
        response_class: Type[TAlexandriaMessage],
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
        request_id = self.network.client.request_tracker.get_free_request_id(node_id)
        request_data = request.to_wire_bytes()
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

    #
    # Low Level Message Sending
    #
    async def send_ping(
        self,
        node_id: NodeID,
        endpoint: Endpoint,
        *,
        enr_seq: int,
        request_id: Optional[bytes] = None,
    ) -> bytes:
        message = PingMessage(PingPayload(enr_seq))
        return await self._send_request(
            node_id, endpoint, message, request_id=request_id
        )

    async def send_pong(
        self, node_id: NodeID, endpoint: Endpoint, *, enr_seq: int, request_id: bytes,
    ) -> None:
        message = PongMessage(PongPayload(enr_seq))
        await self._send_response(node_id, endpoint, message, request_id=request_id)

    #
    # High Level Request/Response
    #
    async def ping(
        self, node_id: NodeID, endpoint: Optional[Endpoint] = None,
    ) -> PongMessage:
        request = PingMessage(PingPayload(self.network.enr_manager.enr.sequence_number))
        response = await self._request(node_id, endpoint, request, PongMessage)
        return response
