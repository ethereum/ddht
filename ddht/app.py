import logging

from async_service import Service


class Application(Service):
    logger = logging.getLogger("ddht.DDHT")

    async def run(self) -> None:
        self.logger.info("STUB")
        await self.manager.wait_finished()
