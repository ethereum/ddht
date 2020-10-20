import functools
from typing import Any, AsyncIterator, Callable, Optional, TypeVar, cast

from async_generator import asynccontextmanager
import trio

HANG_TIMEOUT = 10


TCallable = TypeVar("TCallable", bound=Callable[..., Any])


def no_hang(fn: TCallable) -> TCallable:
    @functools.wraps(fn)
    async def wrapper(*args: Any, **kwargs: Any) -> Any:
        with trio.fail_after(HANG_TIMEOUT):
            return await fn(*args, **kwargs)

    return cast(TCallable, wrapper)


class NamedLock:
    """
    A thin wrapper around a lock that also attributes a *name* to the current
    owner.  Used to make debugging tests easier when someone accidentally tries
    to use two parts of the `NodeAPI` which cannot be used concurrently
    (such as when both would try to open a socket on the same port).
    """

    _listen_lock: trio.Lock
    _lock_owner: Optional[str]

    def __init__(self) -> None:
        self._listen_lock = trio.Lock()
        self._lock_owner = None

    @asynccontextmanager
    async def acquire(self, owner: str) -> AsyncIterator[None]:
        try:
            self._listen_lock.acquire_nowait()
        except trio.WouldBlock:
            raise Exception(f"Locked: owner={self._lock_owner}  acquiring-for={owner}")
        else:
            self._lock_owner = owner
            try:
                yield
            finally:
                self._lock_owner = None
                self._listen_lock.release()
