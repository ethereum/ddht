from typing import Any, AsyncIterator, Collection, Set

from async_generator import asynccontextmanager
import trio

from ddht.abc import ResourceQueueAPI, TResource


class RemoveResource(Exception):
    pass


class ResourceQueue(ResourceQueueAPI[TResource]):
    """
    Allow some set of "worker" processes to share the underlying resources,
    ensuring that any given resource is only in use by a single worker at any
    given time.

    .. code-block:: python

        things = (thing_1, thing_2, ...)
        queue = ResourceQueue(things)

        async def worker():
            while True:
                # within the following async context block the `resource` is
                # reserved for this worker.
                async with queue.reserve() as resource:

                    # We typically want to do something with the resource.
                    do_work(resource)  # do

                    # We can remove the resource from the queue
                    if done_with(resource):
                        queue.remove()

                    # We can add new resources to the queue
                    if has_new_resources(resource):
                        new_resource = ...  #
                        queue.add(new_resource)

                # Upon exiting the context block the resource is automatically
                # added back into the queue (unless it was explicitely
                # removed).
    """

    resources: Set[TResource]

    def __init__(
        self, resources: Collection[TResource], max_resource_count: int = None,
    ) -> None:
        if max_resource_count is None:
            max_resource_count = len(resources)

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
        if resource in self:
            return
        self.resources.add(resource)
        await self._send.send(resource)

    def __contains__(self, value: Any) -> bool:
        return value in self.resources

    def __len__(self) -> int:
        return len(self.resources)

    def remove(self, resource: TResource) -> None:
        self.resources.remove(resource)

    @asynccontextmanager
    async def reserve(self) -> AsyncIterator[TResource]:
        # Fetch a new resource from the queue.  If the resource is no longer
        # part of the tracked resources discard it and move onto the next
        # resource in the queue.
        while True:
            resource = await self._receive.receive()
            if resource in self:
                break
            else:
                continue

        try:
            yield resource
        finally:
            # The resource could have been removed during the context block so
            # only add it back to the queue if it is still part of the tracked
            # resources.
            if resource in self:
                await self._send.send(resource)
