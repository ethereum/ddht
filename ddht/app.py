import logging

from async_service import Service

from ddht.abc import ApplicationAPI
from ddht.boot_info import BootInfo


class BaseApplication(Service, ApplicationAPI):
    logger = logging.getLogger("ddht.DDHT")

    _boot_info: BootInfo

    def __init__(self, boot_info: BootInfo) -> None:
        self._boot_info = boot_info
