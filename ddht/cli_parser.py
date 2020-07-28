import argparse
import ipaddress
import pathlib

from ddht import __version__
from ddht.cli_commands import do_main

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
ddht_parser.add_argument("--base-dir", type=pathlib.Path)
ddht_parser.add_argument("--private-key", help="Hex encoded 32 byte private key")

#
# Logging
#
logging_parser.add_argument(
    "-l",
    "--log-level",
    type=int,
    dest="log_level",
    help=("Configure the logging level. "),
)

#
# Network
#
network_parser.add_argument(
    "--port", type=int, help="Port number for the discovery service"
)
network_parser.add_argument(
    "--listen-address", type=ipaddress.ip_address, help="IP address to listen on"
)
network_parser.add_argument("--bootnodes", help="IP address to listen on")
