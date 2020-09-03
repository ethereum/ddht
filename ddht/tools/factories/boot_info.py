from typing import Dict, Tuple

from eth_enr import ENR
import factory

from ddht.boot_info import BootInfo
from ddht.constants import DEFAULT_PORT, ProtocolVersion
from ddht.v5.constants import DEFAULT_BOOTNODES as DEFAULT_V5_BOOTNODES
from ddht.v5_1.constants import DEFAULT_BOOTNODES as DEFAULT_V51_BOOTNODES
from ddht.xdg import get_xdg_ddht_root

BOOTNODES_V5 = tuple(ENR.from_repr(enr_repr) for enr_repr in DEFAULT_V5_BOOTNODES)
BOOTNODES_V5_1 = tuple(ENR.from_repr(enr_repr) for enr_repr in DEFAULT_V51_BOOTNODES)

BOOTNODES: Dict[ProtocolVersion, Tuple[ENR, ...]] = {
    ProtocolVersion.v5: BOOTNODES_V5,
    ProtocolVersion.v5_1: BOOTNODES_V5_1,
}


class BootInfoFactory(factory.Factory):  # type: ignore
    class Meta:
        model = BootInfo

    protocol_version = ProtocolVersion.v5
    private_key = None
    base_dir = factory.LazyFunction(get_xdg_ddht_root)
    port = DEFAULT_PORT
    listen_on = None
    bootnodes = factory.LazyAttribute(lambda o: BOOTNODES[o.protocol_version])
    is_ephemeral = False
    is_upnp_enabled = True
