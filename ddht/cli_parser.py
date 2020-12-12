import argparse
import ipaddress
import pathlib
from typing import Any

from eth_enr import ENR

from ddht import __version__
from ddht.cli_commands import do_alexandria, do_crawl, do_main
from ddht.constants import ProtocolVersion

parser = argparse.ArgumentParser(description="Discovery V5 DHT")
parser.set_defaults(func=do_main)

parser.add_argument("--version", action="version", version=__version__)

#
# Argument Groups
#
ddht_parser = parser.add_argument_group("core")
logging_parser = parser.add_argument_group("logging")
network_parser = parser.add_argument_group("network")
jsonrpc_parser = parser.add_argument_group("jsonrpc")


#
# Core
#
ddht_parser.add_argument("--private-key", help="Hex encoded 32 byte private key")
ddht_parser.add_argument(
    "--protocol-version",
    default=ProtocolVersion.v5_1,
    type=ProtocolVersion,
    choices=ProtocolVersion,
    help="Protocol version which should be used",
)

base_dir_parser = ddht_parser.add_mutually_exclusive_group()

base_dir_parser.add_argument("--base-dir", type=pathlib.Path)
base_dir_parser.add_argument(
    "--ephemeral",
    action="store_true",
    help=(
        "Run the application in *ephemeral* mode which generates a random "
        "single use private key and uses a temporary directory which will be "
        "removed when the application shuts down."
    ),
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
    "--session-cache-size", default=1024, type=int, help="Max number of active sessions"
)

bootnodes_parser_group = network_parser.add_mutually_exclusive_group()
bootnodes_parser_group.add_argument(
    "--bootnode",
    action=NormalizeAndAppendENR,
    help="ENR for custom bootnode",
    dest="bootnodes",
)
bootnodes_parser_group.add_argument(
    "--no-bootstrap",
    help="Start without any bootnodes",
    action="store_const",
    const=(),
    dest="bootnodes",
)


#
# JSON-RPC
#
jsonrpc_parser.add_argument(
    "--disable-jsonrpc", help="Disable the JSON-RPC server",
)
jsonrpc_parser.add_argument(
    "--ipc-path",
    type=pathlib.Path,
    help="Path where the IPC socket will be opened for serving JSON-RPC",
)


#############################
#                           #
# Subcommands go BELOW here #
#                           #
#############################

subparser = parser.add_subparsers(dest="subcommand")


#
# Crawl Subcommand
#
crawl_parser = subparser.add_parser(
    "crawl",
    help="Attempts to bond with as many nodes as possible and dumps all found ENRs",
)
crawl_parser.set_defaults(func=do_crawl)


class ValidateConcurrency(argparse.Action):
    def __call__(
        self,
        parser: argparse.ArgumentParser,
        namespace: argparse.Namespace,
        value: Any,
        option_string: str = None,
    ) -> None:
        if value < 1:
            parser.error(
                "The value for --concurrency must be a a non-zero positive integer"
            )
            return

        setattr(namespace, self.dest, value)


crawl_parser.add_argument(
    "--concurrency",
    type=int,
    action=ValidateConcurrency,
    help="The concurrency level for crawling the network",
    default=32,
    dest="crawl_concurrency",
)


#
# Alexandria Subcommand
#
alexandria_parser = subparser.add_parser(
    "alexandria", help="Run the client as a node on the Alexandria network",
)


alexandria_parser.set_defaults(func=do_alexandria)

#
# Alexandria[Bootnodes]
#
alexandria_bootnodes_parser_group = alexandria_parser.add_mutually_exclusive_group()
alexandria_bootnodes_parser_group.add_argument(
    "--bootnode",
    action=NormalizeAndAppendENR,
    help="ENR for custom bootnode",
    dest="alexandria_bootnodes",
)
alexandria_bootnodes_parser_group.add_argument(
    "--no-bootstrap",
    help="Start without any bootnodes",
    action="store_const",
    const=(),
    dest="alexandria_bootnodes",
)

#
# Alexandria[CommonsStorage]
#
alexandria_commons_storage_parser_group = (
    alexandria_parser.add_mutually_exclusive_group()
)
alexandria_commons_storage_parser_group.add_argument(
    "--commons-storage-dir",
    help=(
        "The filesystem path where the 'commons' storage should be located."
        "Use the string `:memory:` to use an in-memory ephemeral data store"
    ),
    dest="alexandria_commons_storage",
)
alexandria_commons_storage_parser_group.add_argument(
    "--commons-storage-ephemeral",
    help=("Use an in-memory ephemeral data store for 'commons' storage"),
    action="store_const",
    const=":memory:",
    dest="alexandria_commons_storage",
)

#
# Alexandria[PinnedStorage]
#
alexandria_pinned_storage_parser_group = (
    alexandria_parser.add_mutually_exclusive_group()
)
alexandria_pinned_storage_parser_group.add_argument(
    "--pinned-storage-dir",
    help=(
        "The filesystem path where the 'pinned' storage should be located."
        "Use the string `:memory:` to use an in-memory ephemeral data store"
    ),
    dest="alexandria_pinned_storage",
)
alexandria_pinned_storage_parser_group.add_argument(
    "--pinned-storage-ephemeral",
    help=("Use an in-memory ephemeral data store for 'pinned' storage"),
    action="store_const",
    const=":memory:",
    dest="alexandria_pinned_storage",
)
