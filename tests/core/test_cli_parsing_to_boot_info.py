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
    "args,factory_kwargs",
    (
        ((), {}),
        (("--port", "12345"), dict(port=12345)),
        (
            ("--listen-address", "192.168.0.1"),
            dict(listen_on=ipaddress.ip_address("192.168.0.1")),
        ),
        (
            ("--base-dir", "~/test-home-gets-resolved"),
            dict(
                base_dir=pathlib.Path("~/test-home-gets-resolved").expanduser()
            ),
        ),
        (
            ("--base-dir", "./../test-relative-gets-resolved"),
            dict(
                base_dir=pathlib.Path("./../test-relative-gets-resolved").resolve()
            ),
        ),
        (("--private-key", KEY_HEX), dict(private_key=KEY)),
    ),
)
def test_cli_args_to_boot_info(args, factory_kwargs):
    expected = BootInfoFactory(**factory_kwargs)
    actual = BootInfo.from_cli_args(args)
    assert actual == expected
