import ipaddress
import pathlib

from eth_keys import keys
import pytest

from ddht.boot_info import BootInfo
from ddht.tools.factories.boot_info import BootInfoFactory

KEY_RAW = b"unicornsrainbowsunicornsrainbows"
KEY_HEX = KEY_RAW.hex()
KEY_HEX_PREFIXED = "0x" + KEY_HEX
KEY = keys.PrivateKey(KEY_RAW)


@pytest.mark.parametrize(
    "args,expected",
    (
        ((), BootInfoFactory()),
        (("--port", "12345"), BootInfoFactory(port=12345)),
        (
            ("--listen-address", "192.168.0.1"),
            BootInfoFactory(listen_on=ipaddress.ip_address("192.168.0.1")),
        ),
        (
            ("--base-dir", "~/test-home-gets-resolved"),
            BootInfoFactory(
                base_dir=pathlib.Path("~/test-home-gets-resolved").expanduser()
            ),
        ),
        (
            ("--base-dir", "./../test-relative-gets-resolved"),
            BootInfoFactory(
                base_dir=pathlib.Path("./../test-relative-gets-resolved").resolve()
            ),
        ),
        (("--private-key", KEY_HEX), BootInfoFactory(private_key=KEY)),
    ),
)
def test_cli_args_to_boot_info(args, expected):
    actual = BootInfo.from_cli_args(args)
    assert actual == expected
