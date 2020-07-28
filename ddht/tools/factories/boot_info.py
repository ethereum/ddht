import factory

from ddht.boot_info import BootInfo
from ddht.constants import DEFAULT_BOOTNODES, DEFAULT_LISTEN, DEFAULT_PORT
from ddht.enr import ENR
from ddht.xdg import get_xdg_ddht_root


class BootInfoFactory(factory.Factory):  # type: ignore
    class Meta:
        model = BootInfo

    base_dir = factory.LazyFunction(get_xdg_ddht_root)
    port = DEFAULT_PORT
    listen_on = DEFAULT_LISTEN
    bootnodes = factory.LazyFunction(
        lambda: tuple(ENR.from_repr(enr_repr) for enr_repr in DEFAULT_BOOTNODES)
    )
