import argparse
import ipaddress
import pathlib
from typing import Any

from eth_enr import ENR

from ddht import __version__
from ddht.cli_commands import do_main
from ddht.constants import ProtocolVersion

parser = argparse.ArgumentParser(description="Discovery V5 DHT")
parser.set_defaults(func=do_main)

parser.add_argument("--version", action="version", version=__version__)

#
# subparser for sub commands
#
subparser = parser.add_subparsers(dest="subcommand")

#
# Argument Groups
#
ddht_parser = parser.add_argument_group("core")
logging_parser = parser.add_argument_group("logging")
network_parser = parser.add_argument_group("network")


#
# Core
#
ddht_parser.add_argument("--private-key", help="Hex encoded 32 byte private key")
ddht_parser.add_argument(
    "--protocol-version",
    default=ProtocolVersion.v5,
    type=ProtocolVersion,
    choices=ProtocolVersion,
    help="Protocol version which should be used",
)

base_dir_parser = ddht_parser.add_mutually_exclusive_group()

base_dir_parser.add_argument("--base-dir", type=pathlib.Path)
base_dir_parser.add_argument(
    "--ephemeral", action="store_true",
)

#
# Logging
#
logging_parser.add_argument(
    "--log-file",
    type=pathlib.Path,
    dest="log_file",
    help=("Manually override the logging destination."),
)
logging_parser.add_argument(
    "--log-level-file",
    type=int,
    dest="log_level_file",
    help=("Configure the file logging level"),
)
logging_parser.add_argument(
    "--log-level-stderr",
    type=int,
    dest="log_level_stderr",
    help=("Configure the stderr logging level"),
)


#
# Network
#
class NormalizeAndAppendENR(argparse.Action):
    def __call__(
        self,
        parser: argparse.ArgumentParser,
        namespace: argparse.Namespace,
        value: Any,
        option_string: str = None,
    ) -> None:
        if value is None:
            return

        enr = ENR.from_repr(value)

        if getattr(namespace, self.dest) is None:
            setattr(namespace, self.dest, ())

        enr_list = getattr(namespace, self.dest)
        enr_list += (enr,)

        setattr(namespace, self.dest, enr_list)


network_parser.add_argument(
    "--disable-upnp", action="store_true", help="Disable UPnP port forwarding",
)
network_parser.add_argument(
    "--port", type=int, help="Port number for the discovery service"
)
network_parser.add_argument(
    "--listen-address", type=ipaddress.ip_address, help="IP address to listen on"
)
network_parser.add_argument(
    "--bootnode",
    action=NormalizeAndAppendENR,
    help="IP address to listen on",
    dest="bootnodes",
)
