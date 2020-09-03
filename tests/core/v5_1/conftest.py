import pytest

from ddht.tools.driver import Tester


@pytest.fixture
def tester():
    return Tester()


@pytest.fixture
async def alice(tester, bob):
    node = tester.node()
    node.enr_db.set_enr(bob.enr)
    return node


@pytest.fixture
async def bob(tester):
    return tester.node()


@pytest.fixture
async def driver(tester, alice, bob):
    return tester.session_pair(alice, bob)
