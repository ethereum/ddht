from abc import ABC, abstractmethod
from typing import AsyncContextManager, Any

import trio

from ddht.v5_1.abc import TalkProtocolAPI

from async_service import ServiceAPI


class ExtensionAPI(ABC):
    id: int
    data: bytes

    @property
    @abstractmethod
    def length(self) -> int:
        ...

    @property
    @abstractmethod
    def __eq__(self, other: Any) -> bool:
        ...


class UTPAPI(ServiceAPI, TalkProtocolAPI):
    @abstractmethod
    def open_connection(self,
                        connection_id: int,
                        ) -> AsyncContextManager[trio.abc.HalfCloseableStream]:
        ...

    @abstractmethod
    def receive_connection(self,
                           connection_id: int,
                           ) -> AsyncContextManager[trio.abc.HalfCloseableStream]:
        ...
