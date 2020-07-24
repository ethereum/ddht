from abc import ABC, abstractmethod
from typing import (
    MutableMapping,
)

from ddht.enr import ENR
from ddht.typing import NodeID
from ddht.identity_schemes import IdentitySchemeRegistry


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


class NodeDBAPI(ABC):

    @abstractmethod
    def __init__(self, identity_scheme_registry: IdentitySchemeRegistry, db: DatabaseAPI) -> None:
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
