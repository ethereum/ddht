import logging
import sys


class DDHTFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        record.shortname = record.name.split(".")[-1]  # type: ignore

        return super().format(record)


LOG_FORMATTER = DDHTFormatter(
    fmt="%(levelname)8s  %(asctime)s  %(shortname)20s  %(message)s"
)


def setup_logging(level: int = None) -> None:
    setup_stderr_logging(level)


def setup_stderr_logging(level: int = None) -> logging.StreamHandler:
    if level is None:
        level = logging.INFO
    logger = logging.getLogger()
    logger.setLevel(level)

    handler_stream = logging.StreamHandler(sys.stderr)

    if level is not None:
        handler_stream.setLevel(level)
    handler_stream.setFormatter(LOG_FORMATTER)

    logger.addHandler(handler_stream)

    logger.debug("Logging initialized for stderr")

    return handler_stream
