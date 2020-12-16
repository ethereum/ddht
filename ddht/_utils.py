import asyncio
from contextlib import contextmanager
import itertools
import logging
import math
import operator
import pathlib
import secrets
import socket
import time
from typing import (
    Any,
    AsyncGenerator,
    AsyncIterator,
    Awaitable,
    Callable,
    Collection,
    Iterable,
    Iterator,
    List,
    Optional,
    Sequence,
    Tuple,
    TypeVar,
    Union,
)

from async_generator import asynccontextmanager
from eth_enr import ENRAPI
from eth_keys import keys
from eth_typing import NodeID
from eth_utils import humanize_hash, to_tuple
from eth_utils.toolz import groupby
import trio


def humanize_node_id(node_id: NodeID) -> str:
    return humanize_hash(node_id)  # type: ignore


def sxor(s1: bytes, s2: bytes) -> bytes:
    """
    Perform a bitwise xor operation on two equal length byte strings
    """
    if len(s1) != len(s2):
        raise ValueError("Cannot sxor strings of different length")
    return bytes(x ^ y for x, y in zip(s1, s2))


AsyncFnsAndArgsType = Union[Callable[..., Awaitable[Any]], Tuple[Any, ...]]


async def gather(*async_fns_and_args: AsyncFnsAndArgsType) -> Tuple[Any, ...]:
    """
    Run a collection of async functions in parallel and collect their results.

    The results will be in the same order as the corresponding async functions.
    """
    indices_and_results = []

    async def get_result(index: int) -> None:
        async_fn_and_args = async_fns_and_args[index]
        if isinstance(async_fn_and_args, Iterable):
            async_fn, *args = async_fn_and_args
        elif asyncio.iscoroutinefunction(async_fn_and_args):
            async_fn = async_fn_and_args
            args = []
        else:
            raise TypeError(
                "Each argument must be either an async function or a tuple consisting of an "
                "async function followed by its arguments"
            )

        result = await async_fn(*args)
        indices_and_results.append((index, result))

    async with trio.open_nursery() as nursery:
        for index in range(len(async_fns_and_args)):
            nursery.start_soon(get_result, index)

    indices_and_results_sorted = sorted(indices_and_results, key=operator.itemgetter(0))
    return tuple(result for _, result in indices_and_results_sorted)


async def every(
    interval: float, initial_delay: float = 0
) -> AsyncGenerator[float, Optional[float]]:
    """
    Generator used to perform a task in regular intervals.

    The generator will attempt to yield at a sequence of target times, defined as
    `start_time + initial_delay + N * interval` seconds where `start_time` is trio's current time
    at instantiation of the generator and `N` starts at `0`. The target time is also the value that
    is yielded.

    If at a certain iteration the target time has already passed, the generator will yield
    immediately (with a checkpoint in between). The yield value is still the target time.

    The generator accepts an optional send value which will delay the next and all future
    iterations of the generator by that amount.
    """
    start_time = trio.current_time()
    undelayed_yield_times = (
        start_time + interval * iteration for iteration in itertools.count()
    )
    delay = initial_delay

    for undelayed_yield_time in undelayed_yield_times:
        yield_time = undelayed_yield_time + delay
        await trio.sleep_until(yield_time)

        additional_delay = yield yield_time
        if additional_delay is not None:
            delay += additional_delay


def generate_node_key_file(path: pathlib.Path) -> None:
    if path.exists():
        raise FileExistsError(f"Keyfile path already exists {path}")
    key = secrets.token_bytes(32)
    path.write_bytes(key)


def read_node_key_file(path: pathlib.Path) -> keys.PrivateKey:
    return keys.PrivateKey(path.read_bytes())


def get_open_port() -> int:
    port: int = 0
    while port < 1024:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind(("", 0))
        s.listen(1)
        port = s.getsockname()[1]
        s.close()
    return port


TEnterValue = TypeVar("TEnterValue")


@asynccontextmanager
async def asyncnullcontext(enter_value: TEnterValue) -> AsyncIterator[TEnterValue]:
    yield enter_value


@to_tuple
def reduce_enrs(enrs: Collection[ENRAPI]) -> Iterable[ENRAPI]:
    enrs_by_node_id = groupby(operator.attrgetter("node_id"), enrs)
    for _, enr_group in enrs_by_node_id.items():
        if len(enr_group) == 1:
            yield enr_group[0]
        else:
            yield max(enr_group, key=operator.attrgetter("sequence_number"))


@contextmanager
def timer(name: str) -> Iterator[None]:
    start_at = time.monotonic()
    try:
        yield
    finally:
        end_at = time.monotonic()
        elapsed = end_at - start_at
        logging.getLogger("ddht.Timer").info("TIMER[%s]: %f", name, elapsed)


TValue = TypeVar("TValue")


def weighted_choice(values: Sequence[TValue]) -> TValue:
    """
    A simple weighted choice which favors the items later in the list.

    Weighting is linear with the first item having a weight of 1, the second
    having a weight of 2 and so on.

    values = (a, b, c, d)
    bound = 10  # 1 + 2 + 3 + 4

    VALUES: [a][ b  ][   c   ][    d      ]
    BOUND :  1  2  3  4  5  6  7  8  9  10

    We pick at random from BOUND, and calculate in reverse which index in
    VALUES the chose position corresponds to.
    """
    num_values = len(values)
    # The `bound` is the cumulative sum of `sum(1, 2, 3, ..., n)` where `n` is
    # the number of items we are choosing from.
    bound = (num_values * (num_values + 1)) // 2

    # Now we pick a value that is within the bound above, and then solve
    # backwards for which value corresponds to the chosen position within the
    # bounds.
    scaled_index = secrets.randbelow(bound)
    # This is a simplified quadratic formula to solve the following for the
    # `index` variable.
    #
    # (index * (index + 1)) / 2 = scaled_index
    #
    index = int(math.floor(0.5 + math.sqrt(0.25 + 2 * scaled_index))) - 1

    return values[index]


async def adaptive_timeout(
    *tasks: Tuple[Callable[..., Awaitable[None]], Sequence[Any]],
    threshold: int = 1,
    variance: float = 2,
) -> None:
    """
    Given a set of tasks this function will run them concurrently.  Once at
    least `threshold` have completed, the average completion time is measured.
    The remaining tasks are then given `avg_task_time * variance` to complete after
    which they will be cancelled
    """
    if threshold >= len(tasks):
        raise ValueError("The `threshold` value must be less than the number of tasks")
    elif threshold < 1:
        raise ValueError("The `threshold` value must be 1 or greater")

    # a mutable list to track the average task time
    task_times: List[float] = []
    condition = trio.Condition()

    # a thin wrapper around the provided tasks which measures their execution
    # time.
    async def task_wrapper(
        task_fn: Callable[..., Awaitable[None]], args: Sequence[Any]
    ) -> None:
        nonlocal task_times

        start_at = trio.current_time()
        await task_fn(*args)
        elapsed = trio.current_time() - start_at
        async with condition:
            task_times.append(elapsed)
            condition.notify_all()

    async with trio.open_nursery() as nursery:
        for task_fn, task_args in tasks:
            nursery.start_soon(task_wrapper, task_fn, task_args)

        start_at = trio.current_time()

        # wait for `threshold` tasks to complete
        while len(task_times) < threshold:
            async with condition:
                await condition.wait()

        # measure the average task completion time and calculate the remaining
        # timeout for the remaining tasks.
        avg_task_time = sum(task_times) / len(task_times)
        timeout_at = start_at + (avg_task_time * variance)
        timeout_remaining = timeout_at - trio.current_time()

        # apply the calculated timeout on the remaining tasks
        if timeout_remaining > 0:
            with trio.move_on_after(timeout_remaining):
                while len(task_times) < len(tasks):
                    async with condition:
                        await condition.wait()

        # cancel any remaining tasks.
        nursery.cancel_scope.cancel()


TElement = TypeVar("TElement")
TItem = TypeVar("TItem")


def caboose(seq: Iterable[TItem], el: TElement) -> Iterable[Union[TElement, TItem]]:
    """
    Return an iterable of `seq` with `el` added to the end
    """
    yield from seq
    yield el


BYTE = 1
KILOBYTE = 1024
MEGABYTE = KILOBYTE * 1024
GIGABYTE = MEGABYTE * 1024
TERABYTE = GIGABYTE * 1024
PETABYTE = TERABYTE * 1024

BYTES_UNITS = (
    (PETABYTE, "PB"),
    (TERABYTE, "TB"),
    (GIGABYTE, "GB"),
    (MEGABYTE, "MB"),
    (KILOBYTE, "KB"),
    (BYTE, "B"),
)


def humanize_bytes(value: int) -> str:
    if value == 0:
        return "0B"

    for bytes_per_unit, unit_display in BYTES_UNITS:
        if value >= bytes_per_unit:
            scaled_value = value / bytes_per_unit
            value_display = f"{scaled_value:.2f}".rstrip("0").rstrip(".")
            return f"{value_display}{unit_display}"
    else:
        raise Exception("Should be unreachable")
