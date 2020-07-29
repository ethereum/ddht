import argparse
from dataclasses import dataclass
import ipaddress
import pathlib
import tempfile
from typing import Optional, Sequence, Tuple, TypedDict, Union

from eth_keys import keys
from eth_utils import decode_hex

from ddht._utils import get_open_port
from ddht.constants import DEFAULT_BOOTNODES, DEFAULT_LISTEN, DEFAULT_PORT
from ddht.enr import ENR
from ddht.xdg import get_xdg_ddht_root

AnyIPAddress = Union[ipaddress.IPv4Address, ipaddress.IPv6Address]


class BootInfoKwargs(TypedDict, total=False):
    base_dir: pathlib.Path
    port: int
    listen_on: AnyIPAddress
    bootnodes: Tuple[ENR, ...]
    private_key: Optional[keys.PrivateKey]
    is_ephemeral: bool


def _cli_args_to_boot_info_kwargs(args: argparse.Namespace) -> BootInfoKwargs:
    is_ephemeral = args.ephemeral is True

    if args.base_dir is not None:
        base_dir = args.base_dir.expanduser().resolve()
    elif is_ephemeral:
        base_dir = pathlib.Path(tempfile.TemporaryDirectory().name)
    else:
        base_dir = get_xdg_ddht_root()

    if args.port is not None:
        port = args.port
    elif is_ephemeral:
        port = get_open_port()
    else:
        port = DEFAULT_PORT

    if args.listen_address is None:
        listen_on = DEFAULT_LISTEN
    else:
        listen_on = args.listen_address

    if args.bootnodes is None:
        bootnodes = tuple(ENR.from_repr(enr_repr) for enr_repr in DEFAULT_BOOTNODES)
    else:
        bootnodes = args.bootnodes

    private_key: Optional[keys.PrivateKey]

    if args.private_key is not None:
        private_key = keys.PrivateKey(decode_hex(args.private_key))
    else:
        private_key = None

    return BootInfoKwargs(
        base_dir=base_dir,
        port=port,
        listen_on=listen_on,
        bootnodes=bootnodes,
        private_key=private_key,
        is_ephemeral=is_ephemeral,
    )


@dataclass(frozen=True)
class BootInfo:
    base_dir: pathlib.Path
    port: int
    listen_on: AnyIPAddress
    bootnodes: Tuple[ENR, ...]
    private_key: Optional[keys.PrivateKey]
    is_ephemeral: bool

    @classmethod
    def from_cli_args(cls, args: Sequence[str]) -> "BootInfo":
        # Import here to prevent circular imports
        from ddht.cli_parser import parser

        namespace = parser.parse_args(args)
        return cls.from_namespace(namespace)

    @classmethod
    def from_namespace(cls, args: argparse.Namespace) -> "BootInfo":
        kwargs = _cli_args_to_boot_info_kwargs(args)
        return cls(**kwargs)
