import factory

from ddht.boot_info import BootInfo
from ddht.constants import DEFAULT_BOOTNODES, DEFAULT_PORT
from ddht.enr import ENR
from ddht.xdg import get_xdg_ddht_root


class BootInfoFactory(factory.Factory):  # type: ignore
    class Meta:
        model = BootInfo

    private_key = None
    base_dir = factory.LazyFunction(get_xdg_ddht_root)
    port = DEFAULT_PORT
    listen_on = None
    bootnodes = factory.LazyFunction(
        lambda: tuple(ENR.from_repr(enr_repr) for enr_repr in DEFAULT_BOOTNODES)
    )
    is_ephemeral = False
    is_upnp_enabled = True
