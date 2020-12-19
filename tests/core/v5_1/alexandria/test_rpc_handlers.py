from async_service import background_trio_service
from eth_utils import decode_hex, encode_hex
import pytest
import rlp
import trio
from web3 import IPCProvider, Web3

from ddht.rpc import RPCServer
from ddht.tools.w3_alexandria import AlexandriaModule
from ddht.v5_1.alexandria.rlp_sedes import BlockHeader
from ddht.v5_1.alexandria.rpc_handlers import get_alexandria_rpc_handlers


@pytest.fixture
async def rpc_server(ipc_path, alice, alice_alexandria_network):
    server = RPCServer(ipc_path, get_alexandria_rpc_handlers(alice_alexandria_network))
    async with background_trio_service(server):
        await server.wait_serving()
        yield server


@pytest.fixture
def w3(rpc_server, ipc_path):
    return Web3(
        IPCProvider(ipc_path, timeout=30), modules={"alexandria": (AlexandriaModule,)}
    )


@pytest.mark.trio
async def test_alexandria_rpc_add_pinned_content(
    make_request, alice, alice_alexandria_network
):
    content_key = b"\x00-dummy-key"
    content = b"dummy-content"

    pinned_storage = alice_alexandria_network.pinned_content_storage
    local_advertisement_db = alice_alexandria_network.local_advertisement_db
    assert not pinned_storage.has_content(content_key)
    assert not tuple(local_advertisement_db.query(content_key=content_key))

    await make_request(
        "alexandria_addPinnedContent", [content_key.hex(), content.hex()]
    )

    assert pinned_storage.has_content(content_key)
    assert tuple(local_advertisement_db.query(content_key=content_key))


@pytest.mark.trio
async def test_alexandria_rpc_add_pinned_content_w3(
    w3, alice, alice_alexandria_network
):
    content_key = b"\x00-dummy-key"
    content = b"dummy-content"

    pinned_storage = alice_alexandria_network.pinned_content_storage
    local_advertisement_db = alice_alexandria_network.local_advertisement_db
    assert not pinned_storage.has_content(content_key)
    assert not tuple(local_advertisement_db.query(content_key=content_key))

    await trio.to_thread.run_sync(
        w3.alexandria.add_pinned_content, content_key, content,
    )

    assert pinned_storage.has_content(content_key)
    assert tuple(local_advertisement_db.query(content_key=content_key))


@pytest.mark.trio
async def test_alexandria_rpc_add_commons_content(
    make_request, alice, alice_alexandria_network
):
    content_key = b"\x00-dummy-key"
    content = b"dummy-content"

    commons_storage = alice_alexandria_network.commons_content_storage
    local_advertisement_db = alice_alexandria_network.local_advertisement_db
    assert not commons_storage.has_content(content_key)
    assert not tuple(local_advertisement_db.query(content_key=content_key))

    await make_request(
        "alexandria_addCommonsContent", [content_key.hex(), content.hex()]
    )

    assert commons_storage.has_content(content_key)
    assert tuple(local_advertisement_db.query(content_key=content_key))


@pytest.mark.trio
async def test_alexandria_rpc_add_commons_content_w3(
    w3, alice, alice_alexandria_network
):
    content_key = b"\x00-dummy-key"
    content = b"dummy-content"

    commons_storage = alice_alexandria_network.commons_content_storage
    local_advertisement_db = alice_alexandria_network.local_advertisement_db
    assert not commons_storage.has_content(content_key)
    assert not tuple(local_advertisement_db.query(content_key=content_key))

    await trio.to_thread.run_sync(
        w3.alexandria.add_commons_content, content_key, content,
    )

    assert commons_storage.has_content(content_key)
    assert tuple(local_advertisement_db.query(content_key=content_key))


@pytest.mark.trio
async def test_alexandria_rpc_get_pinned_content(
    make_request, alice, alice_alexandria_network
):
    content_key = b"\x00-dummy-key"
    content = b"dummy-content"

    pinned_storage = alice_alexandria_network.pinned_content_storage
    pinned_storage.set_content(content_key, content)

    result = await make_request("alexandria_getPinnedContent", [content_key.hex()])

    assert result == encode_hex(content)


@pytest.mark.trio
async def test_alexandria_rpc_get_pinned_content_w3(
    w3, alice, alice_alexandria_network
):
    content_key = b"\x00-dummy-key"
    content = b"dummy-content"

    pinned_storage = alice_alexandria_network.pinned_content_storage
    pinned_storage.set_content(content_key, content)

    result = await trio.to_thread.run_sync(
        w3.alexandria.get_pinned_content, content_key
    )

    assert result == content


@pytest.mark.trio
async def test_alexandria_rpc_get_commons_content(
    make_request, alice, alice_alexandria_network
):
    content_key = b"\x00-dummy-key"
    content = b"dummy-content"

    commons_storage = alice_alexandria_network.commons_content_storage
    commons_storage.set_content(content_key, content)

    result = await make_request("alexandria_getCommonsContent", [content_key.hex()])

    assert result == encode_hex(content)


@pytest.mark.trio
async def test_alexandria_rpc_get_commons_content_w3(
    w3, alice, alice_alexandria_network
):
    content_key = b"\x00-dummy-key"
    content = b"dummy-content"

    commons_storage = alice_alexandria_network.commons_content_storage
    commons_storage.set_content(content_key, content)

    result = await trio.to_thread.run_sync(
        w3.alexandria.get_commons_content, content_key
    )

    assert result == content


@pytest.mark.trio
async def test_alexandria_rpc_get_unknown_content(
    make_request, alice, alice_alexandria_network
):
    content_key = b"\x00-dummy-key"

    with pytest.raises(Exception, match="Not Found"):
        await make_request("alexandria_getPinnedContent", [content_key.hex()])
    with pytest.raises(Exception, match="Not Found"):
        await make_request("alexandria_getCommonsContent", [content_key.hex()])


@pytest.mark.trio
async def test_alexandria_rpc_delete_pinned_content(
    make_request, alice, alice_alexandria_network
):
    content_key = b"\x00-dummy-key"
    content = b"dummy-content"

    pinned_storage = alice_alexandria_network.pinned_content_storage
    pinned_storage.set_content(content_key, content)
    assert pinned_storage.has_content(content_key)

    await make_request("alexandria_deletePinnedContent", [content_key.hex()])

    assert not pinned_storage.has_content(content_key)


@pytest.mark.trio
async def test_alexandria_rpc_delete_pinned_content_w3(
    w3, alice, alice_alexandria_network
):
    content_key = b"\x00-dummy-key"
    content = b"dummy-content"

    pinned_storage = alice_alexandria_network.pinned_content_storage
    pinned_storage.set_content(content_key, content)
    assert pinned_storage.has_content(content_key)

    await trio.to_thread.run_sync(w3.alexandria.delete_pinned_content, content_key)

    assert not pinned_storage.has_content(content_key)


@pytest.mark.trio
async def test_alexandria_rpc_delete_commons_content(
    make_request, alice, alice_alexandria_network
):
    content_key = b"\x00-dummy-key"
    content = b"dummy-content"

    commons_storage = alice_alexandria_network.commons_content_storage
    commons_storage.set_content(content_key, content)
    assert commons_storage.has_content(content_key)

    await make_request("alexandria_deleteCommonsContent", [content_key.hex()])

    assert not commons_storage.has_content(content_key)


@pytest.mark.trio
async def test_alexandria_rpc_delete_commons_content_w3(
    w3, alice, alice_alexandria_network
):
    content_key = b"\x00-dummy-key"
    content = b"dummy-content"

    commons_storage = alice_alexandria_network.commons_content_storage
    commons_storage.set_content(content_key, content)
    assert commons_storage.has_content(content_key)

    await trio.to_thread.run_sync(w3.alexandria.delete_commons_content, content_key)

    assert not commons_storage.has_content(content_key)


@pytest.mark.trio
async def test_alexandria_rpc_delete_unknown_content(
    make_request, alice, alice_alexandria_network
):
    content_key = b"\x00-dummy-key"

    with pytest.raises(Exception, match="Not Found"):
        await make_request("alexandria_deletePinnedContent", [content_key.hex()])
    with pytest.raises(Exception, match="Not Found"):
        await make_request("alexandria_deleteCommonsContent", [content_key.hex()])


@pytest.mark.trio
async def test_alexandria_rpc_get_content(
    make_request, alice, bob, alice_alexandria_network, bob_alexandria_network,
):
    content_key_a = b"\x00-dummy-key-a"
    content_key_b = b"\x00-dummy-key-b"
    content_key_c = b"\x00-dummy-key-c"

    content_a = b"dummy-content-a"
    content_b = b"dummy-content-b"
    content_c = b"dummy-content-c"

    await alice_alexandria_network.bond(bob.node_id)

    await bob_alexandria_network.pinned_content_manager.process_content(
        content_key_a, content_a,
    )
    await alice_alexandria_network.pinned_content_manager.process_content(
        content_key_b, content_b,
    )
    await alice_alexandria_network.commons_content_manager.process_content(
        content_key_c, content_c,
    )

    actual_content_a = await make_request(
        "alexandria_getContent", [content_key_a.hex()]
    )
    actual_content_b = await make_request(
        "alexandria_getContent", [content_key_b.hex()]
    )
    actual_content_c = await make_request(
        "alexandria_getContent", [content_key_c.hex()]
    )

    assert actual_content_a == encode_hex(content_a)
    assert actual_content_b == encode_hex(content_b)
    assert actual_content_c == encode_hex(content_c)


@pytest.mark.trio
async def test_alexandria_rpc_get_content_w3(
    w3, alice, bob, alice_alexandria_network, bob_alexandria_network,
):
    content_key_a = b"\x00-dummy-key-a"
    content_key_b = b"\x00-dummy-key-b"
    content_key_c = b"\x00-dummy-key-c"

    content_a = b"dummy-content-a"
    content_b = b"dummy-content-b"
    content_c = b"dummy-content-c"

    await alice_alexandria_network.bond(bob.node_id)

    await bob_alexandria_network.pinned_content_manager.process_content(
        content_key_a, content_a,
    )
    await alice_alexandria_network.pinned_content_manager.process_content(
        content_key_b, content_b,
    )
    await alice_alexandria_network.commons_content_manager.process_content(
        content_key_c, content_c,
    )

    actual_content_a = await trio.to_thread.run_sync(
        w3.alexandria.get_content, content_key_a
    )
    actual_content_b = await trio.to_thread.run_sync(
        w3.alexandria.get_content, content_key_b
    )
    actual_content_c = await trio.to_thread.run_sync(
        w3.alexandria.get_content, content_key_c
    )

    assert actual_content_a == content_a
    assert actual_content_b == content_b
    assert actual_content_c == content_c


GENESIS_RLP = decode_hex(
    "f90214a000000000000000000000000000000000000000000000000000000000"
    "00000000a01dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142"
    "fd40d49347940000000000000000000000000000000000000000a0d7f8974fb5"
    "ac78d9ac099b9ad5018bedc2ce0a72dad1827a1709da30580f0544a056e81f17"
    "1bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421a056e81f"
    "171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421b90100"
    "0000000000000000000000000000000000000000000000000000000000000000"
    "0000000000000000000000000000000000000000000000000000000000000000"
    "0000000000000000000000000000000000000000000000000000000000000000"
    "0000000000000000000000000000000000000000000000000000000000000000"
    "0000000000000000000000000000000000000000000000000000000000000000"
    "0000000000000000000000000000000000000000000000000000000000000000"
    "0000000000000000000000000000000000000000000000000000000000000000"
    "0000000000000000000000000000000000000000000000000000000000000000"
    "850400000000808213888080a011bbe8db4e347b4e8c937c1c8370e4b5ed33ad"
    "b3db69cbdb7a38e1e50b1b82faa0000000000000000000000000000000000000"
    "0000000000000000000000000000880000000000000042"
)
GENESIS = rlp.decode(GENESIS_RLP, sedes=BlockHeader)


@pytest.mark.trio
async def test_alexandria_rpc_get_block_header(
    w3, alice, bob, alice_alexandria_network, bob_alexandria_network,
):
    await alice_alexandria_network.bond(bob.node_id)

    await alice_alexandria_network.pinned_content_manager.process_content(
        b"\x01" + GENESIS.hash, GENESIS_RLP,
    )

    genesis_header = await trio.to_thread.run_sync(
        w3.alexandria.get_block_header, GENESIS.hash
    )

    assert genesis_header == GENESIS
