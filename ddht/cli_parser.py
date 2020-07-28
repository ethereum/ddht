import argparse

from ddht import __version__
from ddht.cli_commands import do_main

parser = argparse.ArgumentParser(description="Discovery V5 DHT")
parser.set_defaults(func=do_main)

#
# subparser for sub commands
#
subparser = parser.add_subparsers(dest="subcommand")

#
# Argument Groups
#
ddht_parser = parser.add_argument_group("core")
logging_parser = parser.add_argument_group("logging")


#
# Globals
#
ddht_parser.add_argument("--version", action="version", version=__version__)

#
# Logging configuration
#
logging_parser.add_argument(
    "-l",
    "--log-level",
    type=int,
    dest="log_level",
    help=("Configure the logging level. "),
)
