import argparse
from dataclasses import dataclass
import pathlib
from typing import Literal, Optional, Sequence, Tuple, TypedDict, Union

from eth_enr import ENR
from eth_enr.abc import ENRAPI

from ddht.v5_1.alexandria.constants import (
    DEFAULT_BOOTNODES,
    DEFAULT_COMMONS_STORAGE_SIZE,
    DEFAULT_MAX_ADVERTISEMENTS,
)


class AlexandriaBootInfoKwargs(TypedDict, total=False):
    bootnodes: Tuple[ENRAPI, ...]

    max_advertisement_count: int

    commons_storage_size: int
    commons_storage: Optional[Union[Literal[":memory:"], pathlib.Path]]

    pinned_storage: Optional[Union[Literal[":memory:"], pathlib.Path]]


def _cli_args_to_boot_info_kwargs(args: argparse.Namespace) -> AlexandriaBootInfoKwargs:
    if args.alexandria_bootnodes is None:
        bootnodes = tuple(ENR.from_repr(enr_repr) for enr_repr in DEFAULT_BOOTNODES)
    else:
        bootnodes = args.alexandria_bootnodes

    max_advertisement_count: int

    if args.alexandria_max_advertisement_count is None:
        max_advertisement_count = DEFAULT_MAX_ADVERTISEMENTS
    else:
        max_advertisement_count = args.alexandria_max_advertisement_count

    commons_storage_size: int

    if args.alexandria_commons_storage_size is None:
        commons_storage_size = DEFAULT_COMMONS_STORAGE_SIZE
    else:
        commons_storage_size = args.alexandria_commons_storage_size

    commons_storage: Optional[Union[Literal[":memory:"], pathlib.Path]]

    if args.alexandria_commons_storage == ":memory:":
        commons_storage = ":memory:"
    elif args.alexandria_commons_storage is not None:
        commons_storage = (
            pathlib.Path(args.alexandria_commons_storage).expanduser().resolve()
        )
    else:
        commons_storage = None

    pinned_storage: Optional[Union[Literal[":memory:"], pathlib.Path]]

    if args.alexandria_pinned_storage == ":memory:":
        pinned_storage = ":memory:"
    elif args.alexandria_pinned_storage is not None:
        pinned_storage = (
            pathlib.Path(args.alexandria_pinned_storage).expanduser().resolve()
        )
    else:
        pinned_storage = None

    return AlexandriaBootInfoKwargs(
        bootnodes=bootnodes,
        max_advertisement_count=max_advertisement_count,
        commons_storage_size=commons_storage_size,
        commons_storage=commons_storage,
        pinned_storage=pinned_storage,
    )


@dataclass(frozen=True)
class AlexandriaBootInfo:
    bootnodes: Tuple[ENRAPI, ...]

    max_advertisement_count: int

    commons_storage_size: int
    commons_storage: Optional[Union[Literal[":memory:"], pathlib.Path]]

    pinned_storage: Optional[Union[Literal[":memory:"], pathlib.Path]]

    @classmethod
    def from_cli_args(cls, args: Sequence[str]) -> "AlexandriaBootInfo":
        # Import here to prevent circular imports
        from ddht.cli_parser import parser

        namespace = parser.parse_args(args)
        return cls.from_namespace(namespace)

    @classmethod
    def from_namespace(cls, args: argparse.Namespace) -> "AlexandriaBootInfo":
        kwargs = _cli_args_to_boot_info_kwargs(args)
        return cls(**kwargs)
