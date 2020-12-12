from async_service import background_trio_service
from eth_utils import encode_hex
import pytest

from ddht.rpc import RPCServer
from ddht.v5_1.alexandria.rpc_handlers import get_alexandria_rpc_handlers


@pytest.fixture
async def rpc_server(ipc_path, alice, alice_alexandria_network):
    server = RPCServer(ipc_path, get_alexandria_rpc_handlers(alice_alexandria_network))
    async with background_trio_service(server):
        await server.wait_serving()
        yield server


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
async def test_alexandria_rpc_delete_unknown_content(
    make_request, alice, alice_alexandria_network
):
    content_key = b"\x00-dummy-key"

    with pytest.raises(Exception, match="Not Found"):
        await make_request("alexandria_deletePinnedContent", [content_key.hex()])
    with pytest.raises(Exception, match="Not Found"):
        await make_request("alexandria_deleteCommonsContent", [content_key.hex()])
