import pathlib
import tempfile

import pytest
import pytest_trio
import trio


@pytest.fixture(autouse=True)
def xdg_home(monkeypatch):
    with tempfile.TemporaryDirectory() as temp_xdg:
        monkeypatch.setenv("XDG_DATA_HOME", temp_xdg)
        yield pathlib.Path(temp_xdg)


@pytest_trio.trio_fixture
async def socket_pair():
    sending_socket = trio.socket.socket(
        family=trio.socket.AF_INET, type=trio.socket.SOCK_DGRAM,
    )
    receiving_socket = trio.socket.socket(
        family=trio.socket.AF_INET, type=trio.socket.SOCK_DGRAM,
    )
    # specifying 0 as port number results in using random available port
    await sending_socket.bind(("127.0.0.1", 0))
    await receiving_socket.bind(("127.0.0.1", 0))
    return sending_socket, receiving_socket
