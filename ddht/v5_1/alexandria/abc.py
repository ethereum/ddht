from abc import abstractmethod
from typing import Optional

from eth_typing import NodeID

from ddht.abc import RequestTrackerAPI
from ddht.endpoint import Endpoint
from ddht.v5_1.abc import NetworkAPI, TalkProtocolAPI
from ddht.v5_1.alexandria.messages import PongMessage


class AlexandriaClientAPI(TalkProtocolAPI):
    network: NetworkAPI
    request_tracker: RequestTrackerAPI

    #
    # Low Level Message Sending
    #
    @abstractmethod
    async def send_ping(
        self,
        node_id: NodeID,
        endpoint: Endpoint,
        *,
        enr_seq: int,
        request_id: Optional[bytes] = None,
    ) -> bytes:
        ...

    @abstractmethod
    async def send_pong(
        self, node_id: NodeID, endpoint: Endpoint, *, enr_seq: int, request_id: bytes,
    ) -> None:
        ...

    #
    # High Level Request/Response
    #
    @abstractmethod
    async def ping(
        self, node_id: NodeID, endpoint: Optional[Endpoint] = None,
    ) -> PongMessage:
        ...
