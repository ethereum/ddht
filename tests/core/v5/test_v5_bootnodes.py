from eth_enr import ENR
import pytest

from ddht.v5.constants import DEFAULT_BOOTNODES


@pytest.mark.parametrize(
    "enr_repr", DEFAULT_BOOTNODES,
)
def test_default_bootnodes_valid(enr_repr):
    enr = ENR.from_repr(enr_repr)
    assert b"ip" in enr or b"ip6" in enr
    assert b"udp" in enr
