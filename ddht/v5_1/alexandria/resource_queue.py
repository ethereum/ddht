from typing import AsyncIterator, Collection, Generic, Hashable, Set, TypeVar

from async_generator import asynccontextmanager
import trio

TResource = TypeVar("TResource", bound=Hashable)


class RemoveResource(Exception):
    pass


class ResourceQueue(Generic[TResource]):
    resources: Set[TResource]

    def __init__(
        self, resources: Collection[TResource], max_resource_count: int = 256
    ) -> None:
        if len(resources) > max_resource_count:
            raise ValueError(
                f"Number of resources exceeds maximum: {len(resources)} > {max_resource_count}"
            )
        self.resources = set(resources)
        self._max_resource_count = max_resource_count
        self._send, self._receive = trio.open_memory_channel[TResource](
            max_resource_count
        )
        for resource in resources:
            self._send.send_nowait(resource)

    async def add(self, resource: TResource) -> None:
        if resource in self.resources:
            raise ValueError("Resource already tracked")
        self.resources.add(resource)
        await self._send.send(resource)

    def remove(self, resource: TResource) -> None:
        self.resources.remove(resource)

    @asynccontextmanager
    async def reserve(self) -> AsyncIterator[TResource]:
        while True:
            resource = await self._receive.receive()
            if resource in self.resources:
                break
            else:
                continue

        try:
            yield resource
        finally:
            if resource in self.resources:
                await self._send.send(resource)
