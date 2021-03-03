from abc import abstractmethod
from typing import AsyncContextManager

import trio

from ddht.v5_1.abc import TalkProtocolAPI

from async_service import ServiceAPI


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
