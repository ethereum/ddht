import pexpect
import logging

import pytest

from ddht import __version__
from ddht import logging as ddht_logging


def test_ddht_version():
    child = pexpect.spawn("ddht --version")
    child.expect(__version__)


def test_ddht_help():
    child = pexpect.spawn("ddht --help")
    child.expect("Discovery V5 DHT")
    child.expect("core:")
    child.expect("logging:")
    child.expect("network:")


@pytest.mark.parametrize(
    "env,expected",
    (
        (None, dict()),
        ('debug', {None: logging.DEBUG}),
        ('DEBUG,INFO:root', {None: logging.DEBUG, 'root': logging.INFO}),
    )
)
def test_loglevel_parsing(env, expected):
    parsed = ddht_logging.environment_to_log_levels(env)
    assert parsed == expected


def test_bad_log_level():
    with pytest.raises(Exception):
        ddht_logging.environment_to_log_levels('debu')
