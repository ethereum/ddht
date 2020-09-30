import io
import itertools
import json

from eth_utils.toolz import take
import pytest
import trio

from ddht.rpc import RPCRequest, read_json


@pytest.fixture(name="make_raw_request")
async def _make_raw_request(ipc_path, rpc_server):
    socket = await trio.open_unix_socket(str(ipc_path))
    async with socket:
        buffer = io.StringIO()

        async def make_raw_request(raw_request: str):
            with trio.fail_after(2):
                data = raw_request.encode("utf8")
                data_iter = iter(data)
                while True:
                    chunk = bytes(take(1024, data_iter))
                    if chunk:
                        try:
                            await socket.send_all(chunk)
                        except trio.BrokenResourceError:
                            break
                    else:
                        break
                return await read_json(socket.socket, buffer)

        yield make_raw_request


@pytest.fixture(name="make_request")
async def _make_request(make_raw_request):
    id_counter = itertools.count()

    async def make_request(method, params=None):
        if params is None:
            params = []
        request = RPCRequest(
            jsonrpc="2.0", method=method, params=params, id=next(id_counter),
        )
        raw_request = json.dumps(request)

        raw_response = await make_raw_request(raw_request)

        if "error" in raw_response:
            raise Exception(raw_response)
        elif "result" in raw_response:
            return raw_response["result"]
        else:
            raise Exception("Invariant")

    yield make_request
