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
    MutableMapping,
    Optional,
    Tuple,
    Type,
    TypeVar,
)

import trio

from ddht.base_message import BaseMessage
from ddht.enr import ENR
from ddht.identity_schemes import IdentitySchemeRegistry
from ddht.typing import ENR_KV, NodeID

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


class DatabaseAPI(MutableMapping[bytes, bytes], ABC):
    @abstractmethod
    def set(self, key: bytes, value: bytes) -> None:
        ...

    @abstractmethod
    def exists(self, key: bytes) -> bool:
        ...

    @abstractmethod
    def delete(self, key: bytes) -> None:
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


class NodeDBAPI(ABC):
    @abstractmethod
    def __init__(
        self, identity_scheme_registry: IdentitySchemeRegistry, db: DatabaseAPI
    ) -> None:
        ...

    @abstractmethod
    def set_enr(self, enr: ENR) -> None:
        ...

    @abstractmethod
    def get_enr(self, node_id: NodeID) -> ENR:
        ...

    @abstractmethod
    def delete_enr(self, node_id: NodeID) -> None:
        ...

    @abstractmethod
    def set_last_pong_time(self, node_id: NodeID, last_pong: int) -> None:
        ...

    @abstractmethod
    def get_last_pong_time(self, node_id: NodeID) -> int:
        ...

    @abstractmethod
    def delete_last_pong_time(self, node_id: NodeID) -> None:
        ...


class ENRManagerAPI(ABC):
    @property
    @abstractmethod
    def enr(self) -> ENR:
        ...

    @abstractmethod
    def update(self, *kv_pairs: ENR_KV) -> ENR:
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
