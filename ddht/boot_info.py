import argparse
from dataclasses import dataclass
import pathlib
import tempfile
from typing import Optional, Sequence, Tuple, TypedDict

from eth_enr import ENR
from eth_enr.abc import ENRAPI
from eth_keys import keys
from eth_utils import decode_hex

from ddht._utils import get_open_port
from ddht.constants import DEFAULT_PORT, ProtocolVersion
from ddht.typing import AnyIPAddress
from ddht.v5.constants import DEFAULT_BOOTNODES as DEFAULT_V5_BOOTNODES
from ddht.v5_1.constants import DEFAULT_BOOTNODES as DEFAULT_V51_BOOTNODES
from ddht.xdg import get_xdg_ddht_root


class BootInfoKwargs(TypedDict, total=False):
    protocol_version: ProtocolVersion
    base_dir: pathlib.Path
    port: int
    listen_on: Optional[AnyIPAddress]
    bootnodes: Tuple[ENRAPI, ...]
    private_key: Optional[keys.PrivateKey]
    is_ephemeral: bool
    is_upnp_enabled: bool


def _cli_args_to_boot_info_kwargs(args: argparse.Namespace) -> BootInfoKwargs:
    protocol_version = args.protocol_version

    is_ephemeral = args.ephemeral is True
    is_upnp_enabled = not args.disable_upnp

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

    listen_on: Optional[AnyIPAddress]

    if args.listen_address is None:
        listen_on = None
    else:
        listen_on = args.listen_address

    if args.bootnodes is None:
        if protocol_version is ProtocolVersion.v5:
            bootnodes = tuple(
                ENR.from_repr(enr_repr) for enr_repr in DEFAULT_V5_BOOTNODES
            )
        elif protocol_version is ProtocolVersion.v5_1:
            bootnodes = tuple(
                ENR.from_repr(enr_repr) for enr_repr in DEFAULT_V51_BOOTNODES
            )
        else:
            raise Exception(f"Unsupported protocol version: {protocol_version}")
    else:
        bootnodes = args.bootnodes

    private_key: Optional[keys.PrivateKey]

    if args.private_key is not None:
        private_key = keys.PrivateKey(decode_hex(args.private_key))
    else:
        private_key = None

    return BootInfoKwargs(
        protocol_version=protocol_version,
        base_dir=base_dir,
        port=port,
        listen_on=listen_on,
        bootnodes=bootnodes,
        private_key=private_key,
        is_ephemeral=is_ephemeral,
        is_upnp_enabled=is_upnp_enabled,
    )


@dataclass(frozen=True)
class BootInfo:
    protocol_version: ProtocolVersion
    base_dir: pathlib.Path
    port: int
    listen_on: Optional[AnyIPAddress]
    bootnodes: Tuple[ENRAPI, ...]
    private_key: Optional[keys.PrivateKey]
    is_ephemeral: bool
    is_upnp_enabled: bool

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
