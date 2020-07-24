from typing import Any

from eth_utils import int_to_big_endian
import factory

from ddht.abc import AddressAPI
from ddht.kademlia import Address

from .keys import PublicKeyFactory
from .socket import get_open_port

IPAddressFactory = factory.Faker("ipv4")


class AddressFactory(factory.Factory):
    class Meta:
        model = Address

    ip = IPAddressFactory
    udp_port = tcp_port = factory.LazyFunction(get_open_port)

    @classmethod
    def localhost(cls, *args: Any, **kwargs: Any) -> AddressAPI:
        return cls(*args, ip="127.0.0.1", **kwargs)
