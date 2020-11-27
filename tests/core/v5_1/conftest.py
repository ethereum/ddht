from eth_enr import OldSequenceNumber
import pytest

from ddht.tools.driver import Tester


@pytest.fixture
def tester():
    return Tester()


#
# Nodes
#
@pytest.fixture
async def alice(tester, bob):
    node = tester.node()
    try:
        node.enr_db.set_enr(bob.enr)
    except OldSequenceNumber:
        pass
    return node


@pytest.fixture
async def bob(tester):
    return tester.node()


@pytest.fixture
async def carol(tester):
    return tester.node()


@pytest.fixture
async def driver(tester, alice, bob):
    return tester.session_pair(alice, bob)


#
# Clients
#
@pytest.fixture
async def alice_client(alice, bob, carol):
    async with alice.client() as alice_client:
        yield alice_client


@pytest.fixture
async def bob_client(alice, bob):
    async with bob.client() as bob_client:
        yield bob_client


#
# Networks
#
@pytest.fixture
async def alice_network(alice, bob):
    async with alice.network() as alice_network:
        yield alice_network


@pytest.fixture
async def bob_network(alice, bob):
    async with bob.network() as bob_network:
        yield bob_network
