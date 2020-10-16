import pytest


@pytest.fixture
async def alice_alexandria_client(alice, alice_network):
    async with alice.alexandria.client(alice_network) as alice_alexandria_client:
        yield alice_alexandria_client


@pytest.fixture
async def bob_alexandria_client(bob, bob_network):
    async with bob.alexandria.client(bob_network) as bob_alexandria_client:
        yield bob_alexandria_client


@pytest.fixture
async def alice_alexandria_network(alice, alice_network):
    async with alice.alexandria.network(alice_network) as alice_alexandria:
        yield alice_alexandria


@pytest.fixture
async def bob_alexandria_network(bob, bob_network):
    async with bob.alexandria.network(bob_network) as bob_alexandria:
        yield bob_alexandria
