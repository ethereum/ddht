import argparse
import logging

from async_service import Service

from ddht.abc import ApplicationAPI
from ddht.boot_info import BootInfo


class BaseApplication(Service, ApplicationAPI):
    logger = logging.getLogger("ddht.DDHT")

    _args: argparse.Namespace
    _boot_info: BootInfo

    def __init__(self, args: argparse.Namespace, boot_info: BootInfo) -> None:
        self._args = args
        self._boot_info = boot_info
