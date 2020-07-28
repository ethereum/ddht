import pexpect

from ddht import __version__


def test_ddht_version():
    child = pexpect.spawn("ddht --version")
    child.expect(__version__)


def test_ddht_help():
    child = pexpect.spawn("ddht --help")
    child.expect("Discovery V5 DHT")
    child.expect("core:")
    child.expect("logging:")
    child.expect("network:")
