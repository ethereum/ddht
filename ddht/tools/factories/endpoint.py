import socket
from typing import Any

import factory

from ddht.endpoint import Endpoint
from ddht.tools.factories.socket import robust_get_open_port

LOCALHOST = socket.inet_aton("127.0.0.1")


class EndpointFactory(factory.Factory):  # type: ignore
    class Meta:
        model = Endpoint

    ip_address = factory.LazyFunction(
        lambda: socket.inet_aton(factory.Faker("ipv4").generate({}))
    )
    port = factory.LazyFunction(robust_get_open_port)

    @classmethod
    def localhost(cls, *args: Any, **kwargs: Any) -> Endpoint:
        return cls(*args, ip_address=LOCALHOST, **kwargs)
