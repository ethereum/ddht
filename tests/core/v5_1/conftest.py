import pytest

from ddht.tools.driver import Tester


@pytest.fixture
def tester():
    return Tester()


@pytest.fixture
async def alice(tester):
    return tester.node()


@pytest.fixture
async def bob(tester):
    return tester.node()


@pytest.fixture
async def driver(tester, alice, bob):
    return tester.session_pair(alice, bob)
