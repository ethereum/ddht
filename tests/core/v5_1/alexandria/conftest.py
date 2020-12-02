import sqlite3

import pytest

from ddht.v5_1.alexandria.advertisement_db import create_tables


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


@pytest.fixture
def base_conn():
    return sqlite3.connect(":memory:")


@pytest.fixture
def conn(base_conn):
    create_tables(base_conn)
    return base_conn
