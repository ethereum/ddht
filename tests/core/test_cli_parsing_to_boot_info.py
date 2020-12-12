import ipaddress
import pathlib

from eth_keys import keys
import pytest

from ddht.boot_info import BootInfo
from ddht.constants import ProtocolVersion
from ddht.tools.factories.boot_info import BOOTNODES_V5, BootInfoFactory
from ddht.tools.factories.discovery import ENRFactory

KEY_RAW = b"unicornsrainbowsunicornsrainbows"
KEY_HEX = KEY_RAW.hex()
KEY_HEX_PREFIXED = "0x" + KEY_HEX
KEY = keys.PrivateKey(KEY_RAW)


ENR_A = ENRFactory()
ENR_B = ENRFactory()


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
            dict(base_dir=pathlib.Path("~/test-home-gets-resolved").expanduser()),
        ),
        (
            ("--base-dir", "./../test-relative-gets-resolved"),
            dict(base_dir=pathlib.Path("./../test-relative-gets-resolved").resolve()),
        ),
        (("--private-key", KEY_HEX), dict(private_key=KEY)),
        (("--bootnode", repr(ENR_A)), (dict(bootnodes=(ENR_A,))),),
        (
            ("--bootnode", repr(ENR_A), "--bootnode", repr(ENR_B)),
            (dict(bootnodes=(ENR_A, ENR_B))),
        ),
        (("--disable-upnp",), (dict(is_upnp_enabled=False)),),
        # protocol version
        (
            ("--protocol-version", "v5"),
            dict(protocol_version=ProtocolVersion.v5, bootnodes=BOOTNODES_V5),
        ),
        (("--protocol-version", "v5.1"), {}),
    ),
)
def test_cli_args_to_boot_info(args, factory_kwargs):
    expected = BootInfoFactory(**factory_kwargs)
    actual = BootInfo.from_cli_args(args)
    assert actual == expected
