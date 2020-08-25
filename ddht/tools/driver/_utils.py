import functools
from typing import Any, Callable, TypeVar, cast

import trio

HANG_TIMEOUT = 10


TCallable = TypeVar("TCallable", bound=Callable[..., Any])


def no_hang(fn: TCallable) -> TCallable:
    @functools.wraps(fn)
    async def wrapper(*args: Any, **kwargs: Any) -> Any:
        with trio.fail_after(HANG_TIMEOUT):
            return await fn(*args, **kwargs)

    return cast(TCallable, wrapper)
