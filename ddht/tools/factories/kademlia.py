from typing import Any

import factory

from ddht.abc import AddressAPI
from ddht.kademlia import Address
from ddht.tools.factories.socket import robust_get_open_port

IPAddressFactory = factory.Faker("ipv4")


class AddressFactory(factory.Factory):  # type: ignore
    class Meta:
        model = Address

    ip = IPAddressFactory
    udp_port = tcp_port = factory.LazyFunction(robust_get_open_port)

    @classmethod
    def localhost(cls, *args: Any, **kwargs: Any) -> AddressAPI:
        return cls(*args, ip="127.0.0.1", **kwargs)
