from abc import ABC, abstractmethod
from collections import UserDict
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncContextManager,
    Collection,
    Deque,
    Generic,
    Iterator,
    List,
    Optional,
    Sequence,
    Tuple,
    Type,
    TypedDict,
    TypeVar,
)

from async_service import ServiceAPI
from eth_enr.abc import ENRAPI, IdentitySchemeAPI
from eth_typing import NodeID
import trio

from ddht.base_message import BaseMessage
from ddht.boot_info import BootInfo
from ddht.endpoint import Endpoint
from ddht.typing import JSON, SessionKeys

TAddress = TypeVar("TAddress", bound="AddressAPI")


class AddressAPI(ABC):
    udp_port: int
    tcp_port: int

    @abstractmethod
    def __init__(self, ip: str, udp_port: int, tcp_port: int) -> None:
        ...

    @property
    @abstractmethod
    def is_loopback(self) -> bool:
        ...

    @property
    @abstractmethod
    def is_unspecified(self) -> bool:
        ...

    @property
    @abstractmethod
    def is_reserved(self) -> bool:
        ...

    @property
    @abstractmethod
    def is_private(self) -> bool:
        ...

    @property
    @abstractmethod
    def ip(self) -> str:
        ...

    @property
    @abstractmethod
    def ip_packed(self) -> bytes:
        ...

    @abstractmethod
    def __eq__(self, other: Any) -> bool:
        ...

    @abstractmethod
    def to_endpoint(self) -> List[bytes]:
        ...

    @classmethod
    @abstractmethod
    def from_endpoint(
        cls: Type[TAddress], ip: str, udp_port: bytes, tcp_port: bytes = b"\x00\x00"
    ) -> TAddress:
        ...


TEventPayload = TypeVar("TEventPayload")


class EventAPI(Generic[TEventPayload]):
    name: str

    @abstractmethod
    async def trigger(self, payload: TEventPayload) -> None:
        ...

    @abstractmethod
    def trigger_nowait(self, payload: TEventPayload) -> None:
        ...

    @abstractmethod
    def subscribe(self) -> AsyncContextManager[trio.abc.ReceiveChannel[TEventPayload]]:
        ...

    @abstractmethod
    def subscribe_and_wait(self) -> AsyncContextManager[None]:
        ...

    @abstractmethod
    async def wait(self) -> TEventPayload:
        ...


# https://github.com/python/mypy/issues/5264#issuecomment-399407428
if TYPE_CHECKING:
    MessageTypeRegistryBaseType = UserDict[int, Type[BaseMessage]]
else:
    MessageTypeRegistryBaseType = UserDict


class MessageTypeRegistryAPI(MessageTypeRegistryBaseType):
    @abstractmethod
    def register(self, message_data_class: Type[BaseMessage]) -> Type[BaseMessage]:
        ...

    @abstractmethod
    def get_message_id(self, message_data_class: Type[BaseMessage]) -> int:
        ...


class RoutingTableAPI(ABC):
    center_node_id: NodeID
    bucket_size: int

    buckets: Tuple[Deque[NodeID], ...]
    replacement_caches: Tuple[Deque[NodeID], ...]

    @property
    @abstractmethod
    def num_buckets(self) -> int:
        ...

    @abstractmethod
    def get_index_bucket_and_replacement_cache(
        self, node_id: NodeID
    ) -> Tuple[int, Deque[NodeID], Deque[NodeID]]:
        ...

    @abstractmethod
    def update(self, node_id: NodeID) -> Optional[NodeID]:
        ...

    @abstractmethod
    def update_bucket_unchecked(self, node_id: NodeID) -> None:
        ...

    @abstractmethod
    def remove(self, node_id: NodeID) -> None:
        ...

    @abstractmethod
    def get_nodes_at_log_distance(self, log_distance: int) -> Tuple[NodeID, ...]:
        ...

    @property
    @abstractmethod
    def is_empty(self) -> bool:
        ...

    @abstractmethod
    def get_least_recently_updated_log_distance(self) -> int:
        ...

    @abstractmethod
    def iter_nodes_around(self, reference_node_id: NodeID) -> Iterator[NodeID]:
        ...

    @abstractmethod
    def iter_all_random(self) -> Iterator[NodeID]:
        ...


TSignatureInputs = TypeVar("TSignatureInputs")


class HandshakeSchemeAPI(ABC, Generic[TSignatureInputs]):
    identity_scheme: Type[IdentitySchemeAPI]
    signature_inputs_cls: Type[TSignatureInputs]

    #
    # Handshake
    #
    @classmethod
    @abstractmethod
    def create_handshake_key_pair(cls) -> Tuple[bytes, bytes]:
        """Create a random private/public key pair used for performing a handshake."""
        ...

    @classmethod
    @abstractmethod
    def validate_handshake_public_key(cls, public_key: bytes) -> None:
        """Validate that a public key received during handshake is valid."""
        ...

    @classmethod
    @abstractmethod
    def compute_session_keys(
        cls,
        *,
        local_private_key: bytes,
        remote_public_key: bytes,
        local_node_id: NodeID,
        remote_node_id: NodeID,
        salt: bytes,
        is_locally_initiated: bool,
    ) -> SessionKeys:
        """Compute the symmetric session keys."""
        ...

    @classmethod
    @abstractmethod
    def create_id_nonce_signature(
        cls, *, signature_inputs: TSignatureInputs, private_key: bytes,
    ) -> bytes:
        """Sign an id nonce received during handshake."""
        ...

    @classmethod
    @abstractmethod
    def validate_id_nonce_signature(
        cls, *, signature_inputs: TSignatureInputs, signature: bytes, public_key: bytes,
    ) -> None:
        """Validate the id nonce signature received from a peer."""
        ...


# https://github.com/python/mypy/issues/5264#issuecomment-399407428
if TYPE_CHECKING:
    HandshakeSchemeRegistryBaseType = UserDict[
        Type[IdentitySchemeAPI], Type[HandshakeSchemeAPI[Any]]
    ]
else:
    HandshakeSchemeRegistryBaseType = UserDict


class HandshakeSchemeRegistryAPI(HandshakeSchemeRegistryBaseType):
    @abstractmethod
    def register(
        self, handshake_scheme_class: Type[HandshakeSchemeAPI[TSignatureInputs]]
    ) -> Type[HandshakeSchemeAPI[TSignatureInputs]]:
        ...


#
# JSON-RPC Interfaces
#
class RPCRequest(TypedDict, total=False):
    jsonrpc: str
    method: str
    params: List[Any]
    id: int


class RPCResponse(TypedDict, total=False):
    id: int
    jsonrpc: str
    result: JSON
    error: str


class RPCHandlerAPI(ABC):
    @abstractmethod
    async def __call__(self, request: RPCRequest) -> RPCResponse:
        ...


class ApplicationAPI(ServiceAPI):
    @abstractmethod
    def __init__(self, boot_info: BootInfo) -> None:
        ...


#
# Client Interfaces
#
TPongMessage = TypeVar("TPongMessage")


class PingPongAPI(ABC, Generic[TPongMessage]):
    @abstractmethod
    async def send_ping(
        self,
        node_id: NodeID,
        endpoint: Endpoint,
        *,
        enr_seq: Optional[int] = None,
        request_id: Optional[bytes] = None,
    ) -> bytes:
        ...

    @abstractmethod
    async def send_pong(
        self, node_id: NodeID, endpoint: Endpoint, *, request_id: bytes,
    ) -> None:
        ...

    @abstractmethod
    async def ping(self, node_id: NodeID, endpoint: Endpoint) -> TPongMessage:
        ...


TFoundNodesMessage = TypeVar("TFoundNodesMessage")


class FindNodesAPI(ABC, Generic[TFoundNodesMessage]):
    @abstractmethod
    async def send_find_nodes(
        self,
        node_id: NodeID,
        endpoint: Endpoint,
        *,
        distances: Collection[int],
        request_id: Optional[bytes] = None,
    ) -> bytes:
        ...

    @abstractmethod
    async def send_found_nodes(
        self,
        node_id: NodeID,
        endpoint: Endpoint,
        *,
        enrs: Sequence[ENRAPI],
        request_id: bytes,
    ) -> int:
        ...

    @abstractmethod
    async def find_nodes(
        self, node_id: NodeID, endpoint: Endpoint, distances: Collection[int]
    ) -> Tuple[TFoundNodesMessage, ...]:
        ...
