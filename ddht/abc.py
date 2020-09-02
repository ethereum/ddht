from abc import ABC, abstractmethod
from collections import UserDict
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncContextManager,
    Deque,
    Generic,
    Iterator,
    List,
    Optional,
    Tuple,
    Type,
    TypeVar,
)

from eth_enr.abc import IdentitySchemeAPI
from eth_typing import NodeID
import trio

from ddht.base_message import BaseMessage
from ddht.typing import IDNonce, SessionKeys

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


class HandshakeSchemeAPI(ABC):
    identity_scheme: Type[IdentitySchemeAPI]

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
        id_nonce: IDNonce,
        is_locally_initiated: bool,
    ) -> SessionKeys:
        """Compute the symmetric session keys."""
        ...

    @classmethod
    @abstractmethod
    def create_id_nonce_signature(
        cls, *, id_nonce: IDNonce, ephemeral_public_key: bytes, private_key: bytes
    ) -> bytes:
        """Sign an id nonce received during handshake."""
        ...

    @classmethod
    @abstractmethod
    def validate_id_nonce_signature(
        cls,
        *,
        id_nonce: IDNonce,
        ephemeral_public_key: bytes,
        signature: bytes,
        public_key: bytes,
    ) -> None:
        """Validate the id nonce signature received from a peer."""
        ...


# https://github.com/python/mypy/issues/5264#issuecomment-399407428
if TYPE_CHECKING:
    HandshakeSchemeRegistryBaseType = UserDict[
        Type[IdentitySchemeAPI], Type[HandshakeSchemeAPI]
    ]
else:
    HandshakeSchemeRegistryBaseType = UserDict


class HandshakeSchemeRegistryAPI(HandshakeSchemeRegistryBaseType):
    @abstractmethod
    def register(
        self, handshake_scheme_class: Type[HandshakeSchemeAPI]
    ) -> Type[HandshakeSchemeAPI]:
        ...
