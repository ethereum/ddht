from abc import ABC, abstractmethod
from typing import (
    Any,
    AsyncContextManager,
    Generic,
    List,
    MutableMapping,
    Type,
    TypeVar,
)

import trio

from ddht.enr import ENR
from ddht.identity_schemes import IdentitySchemeRegistry
from ddht.typing import NodeID

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
    def subscribe(self) -> AsyncContextManager[trio.abc.ReceiveChannel[TEventPayload]]:
        ...

    @abstractmethod
    def subscribe_and_wait(self) -> AsyncContextManager[None]:
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
