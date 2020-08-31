import logging
import logging.handlers
import os
import pathlib
import sys
from typing import Dict, Optional, Tuple


class DDHTFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        record.shortname = record.name.split(".")[-1]  # type: ignore

        return super().format(record)


LOG_FORMATTER = DDHTFormatter(
    fmt="%(levelname)8s  %(asctime)s  %(name)20s  %(message)s"
)


LOG_LEVEL_CHOICES = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARN": logging.WARNING,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
    "CRITICAL": logging.CRITICAL,
}


def parse_raw_log_level(raw_level: str) -> int:
    if raw_level.upper() in LOG_LEVEL_CHOICES:
        return LOG_LEVEL_CHOICES[raw_level.upper()]

    raise Exception(f"Invalid log level: {raw_level}")


def parse_log_level_spec(log_level_spec: str) -> Tuple[Optional[str], int]:
    raw_level, _, raw_path = log_level_spec.partition(":")
    level = parse_raw_log_level(raw_level)

    # if there is no colon then this is a spec for the root logger and path will be ''
    path = raw_path if raw_path != "" else None
    return path, level


def environment_to_log_levels(raw_levels: Optional[str]) -> Dict[Optional[str], int]:
    if raw_levels is None:
        return dict()

    levels = dict()
    for raw_level in raw_levels.split(","):
        path, level = parse_log_level_spec(raw_level)
        levels[path] = level

    return levels


def setup_logging(
    logfile: pathlib.Path, file_log_level: int, stderr_level: int = None
) -> None:
    stderr_level = stderr_level or logging.INFO
    file_log_level = file_log_level or logging.DEBUG

    raw_environ_log_levels = os.environ.get("LOGLEVEL", None)
    environ_log_levels = environment_to_log_levels(raw_environ_log_levels)

    for path, level in environ_log_levels.items():
        logging.getLogger(path).setLevel(level)

    logger = logging.getLogger()
    logger.setLevel(min(stderr_level, file_log_level))

    setup_stderr_logging(stderr_level)
    setup_file_logging(logfile, file_log_level)


def setup_stderr_logging(level: int) -> logging.StreamHandler:
    handler_stream = logging.StreamHandler(sys.stderr)
    handler_stream.setLevel(level)
    handler_stream.setFormatter(LOG_FORMATTER)

    logger = logging.getLogger()
    logger.addHandler(handler_stream)

    return handler_stream


def setup_file_logging(
    logfile: pathlib.Path, level: int
) -> logging.handlers.RotatingFileHandler:

    ten_MB = 10 * 1024 * 1024

    handler_file = logging.handlers.RotatingFileHandler(
        logfile,
        maxBytes=ten_MB,
        backupCount=5,
        delay=True,  # don't open the file until we try to write a logline
    )
    handler_file.setLevel(level)
    handler_file.setFormatter(LOG_FORMATTER)

    if logfile.exists():
        # begin a new file every time we launch, this makes debugging much easier
        handler_file.doRollover()

    logger = logging.getLogger()
    logger.addHandler(handler_file)

    return handler_file
