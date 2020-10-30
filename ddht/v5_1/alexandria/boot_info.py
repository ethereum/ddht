import argparse
from dataclasses import dataclass
from typing import Sequence, Tuple, TypedDict

from eth_enr import ENR
from eth_enr.abc import ENRAPI

from ddht.v5_1.alexandria.constants import DEFAULT_BOOTNODES


class AlexandriaBootInfoKwargs(TypedDict, total=False):
    bootnodes: Tuple[ENRAPI, ...]


def _cli_args_to_boot_info_kwargs(args: argparse.Namespace) -> AlexandriaBootInfoKwargs:
    if args.alexandria_bootnodes is None:
        bootnodes = tuple(ENR.from_repr(enr_repr) for enr_repr in DEFAULT_BOOTNODES)
    else:
        bootnodes = args.alexandria_bootnodes

    return AlexandriaBootInfoKwargs(bootnodes=bootnodes,)


@dataclass(frozen=True)
class AlexandriaBootInfo:
    bootnodes: Tuple[ENRAPI, ...]

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
