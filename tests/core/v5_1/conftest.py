import pytest

from ddht.tools.driver import Network


@pytest.fixture
def network():
    return Network()


@pytest.fixture
async def alice(network):
    return network.node()


@pytest.fixture
async def bob(network):
    return network.node()


@pytest.fixture
async def driver(network, alice, bob):
    return network.session_pair(alice, bob)
