import pytest


@pytest.fixture
async def alice_alexandria(alice, alice_network):
    async with alice.alexandria(alice_network) as alice_alexandria:
        yield alice_alexandria


@pytest.fixture
async def bob_alexandria(bob, bob_network):
    async with bob.alexandria(bob_network) as bob_alexandria:
        yield bob_alexandria
