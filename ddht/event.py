import logging
from typing import AsyncIterator, Set

from async_generator import asynccontextmanager
import trio

from ddht.abc import EventAPI, TEventPayload


class Event(EventAPI[TEventPayload]):
    logger = logging.getLogger("ddht.events.Event")

    _channels: Set[trio.abc.SendChannel[TEventPayload]]

    def __init__(self, name: str, buffer_size: int = 256) -> None:
        self.name = name
        self._buffer_size = buffer_size
        self._lock = trio.Lock()
        self._channels = set()

    async def trigger(self, payload: TEventPayload) -> None:
        self.logger.debug("%s: triggered: %s", self.name, payload)
        if not self._channels:
            return
        async with self._lock:
            for send_channel in self._channels:
                try:
                    await send_channel.send(payload)
                except trio.BrokenResourceError:
                    pass

    def trigger_nowait(self, payload: TEventPayload) -> None:
        self.logger.debug("%s: triggered: %s", self.name, payload)
        for send_channel in self._channels:
            try:
                send_channel.send_nowait(payload)  # type: ignore
            except trio.BrokenResourceError:
                pass

    @asynccontextmanager
    async def subscribe(self) -> AsyncIterator[trio.abc.ReceiveChannel[TEventPayload]]:
        send_channel, receive_channel = trio.open_memory_channel[TEventPayload](
            self._buffer_size
        )

        async with self._lock:
            self._channels.add(send_channel)

        try:
            async with receive_channel:
                yield receive_channel
        finally:
            async with self._lock:
                self._channels.remove(send_channel)

    @asynccontextmanager
    async def subscribe_and_wait(self) -> AsyncIterator[None]:
        async with self.subscribe() as subscription:
            yield
            await subscription.receive()

    async def wait(self) -> TEventPayload:
        async with self.subscribe() as subscription:
            result = await subscription.receive()
        return result
