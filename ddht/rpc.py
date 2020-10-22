from abc import abstractmethod
import collections
import io
import json
import logging
import pathlib
from typing import Any, Dict, Generic, Mapping, Tuple, TypeVar, cast

from async_service import Service
from eth_utils import ValidationError
import trio

from ddht.abc import RPCHandlerAPI, RPCRequest, RPCResponse
from ddht.exceptions import DecodingError
from ddht.typing import JSON

NEW_LINE = "\n"


def strip_non_json_prefix(raw_request: str) -> Tuple[str, str]:
    if raw_request and raw_request[0] != "{":
        prefix, bracket, rest = raw_request.partition("{")
        return prefix.strip(), bracket + rest
    else:
        return "", raw_request


logger = logging.getLogger("ddht.rpc")
decoder = json.JSONDecoder()


MAXIMUM_RPC_PAYLOAD_SIZE = 1024 * 1024  # 1 MB


async def read_json(
    socket: trio.socket.SocketType,
    buffer: io.StringIO,
    decoder: json.JSONDecoder = decoder,
) -> JSON:
    request: JSON

    while True:
        data = await socket.recv(1024)
        buffer.write(data.decode())

        bad_prefix, raw_request = strip_non_json_prefix(buffer.getvalue())
        if bad_prefix:
            logger.info("Client started request with non json data: %r", bad_prefix)
            await write_error(socket, f"Cannot parse json: {bad_prefix}")
            raise DecodingError(f"Invalid JSON payload: prefix={bad_prefix}")

        if len(raw_request) > MAXIMUM_RPC_PAYLOAD_SIZE:
            error_msg = (
                f"RPC payload exceeds maximum size: {len(raw_request)} "
                f"> {MAXIMUM_RPC_PAYLOAD_SIZE}"
            )
            await write_error(socket, error_msg)
            raise DecodingError(error_msg)

        try:
            request, offset = decoder.raw_decode(raw_request)
        except json.JSONDecodeError:
            # invalid json request, keep reading data until a valid json is formed
            if raw_request:
                logger.debug(
                    "Invalid JSON, waiting for rest of message: %r", raw_request,
                )
            else:
                await trio.sleep(0.01)
            continue

        buffer.seek(0)
        buffer.write(raw_request[offset:])
        buffer.truncate()

        break

    return request


async def write_error(socket: trio.socket.SocketType, message: str) -> None:
    json_error = json.dumps({"error": message})
    await socket.send(json_error.encode("utf8"))


def validate_request(request: Mapping[Any, Any]) -> None:
    try:
        version = request["jsonrpc"]
    except KeyError as err:
        raise ValidationError("Missing 'jsonrpc' key") from err
    else:
        if version != "2.0":
            raise ValidationError(f"Invalid version: {version}")

    if "method" not in request:
        raise ValidationError("Missing 'method' key")
    if "params" in request:
        if not isinstance(request["params"], list):
            raise ValidationError(
                f"The `params` value must be a list.  Got: {type(request['params'])}"
            )


def generate_error_response(request: RPCRequest, error: str) -> RPCResponse:
    response = RPCResponse(
        id=request.get("id", -1),
        jsonrpc=request.get("jsonrpc", "2.0"),
        error=str(error),
    )
    return response


def generate_success_response(request: RPCRequest, result: Any,) -> RPCResponse:
    response = RPCResponse(
        id=request.get("id", -1), jsonrpc=request.get("jsonrpc", "2.0"), result=result,
    )

    return response


TParams = TypeVar("TParams")
TResult = TypeVar("TResult")


class RPCError(Exception):
    pass


class RPCHandler(RPCHandlerAPI, Generic[TParams, TResult]):
    """
    Class to simplify some boilerplate when writing an RPCHandlerAPI
    implementation.
    """

    async def __call__(self, request: RPCRequest) -> RPCResponse:
        try:
            params = self.extract_params(request)
            result = await self.do_call(params)
        except RPCError as err:
            return generate_error_response(request, str(err))
        else:
            return generate_success_response(request, result)

    @abstractmethod
    async def do_call(self, params: TParams) -> TResult:
        """
        The return value of this function will be used as the `result` key in a
        success response.  To return an error response, raise an
        :class:`ddht.rpc.RPCError` which will be used as the `error` key in the
        response.
        """
        ...

    def extract_params(self, request: RPCRequest) -> TParams:
        return request.get("params", [])  # type: ignore


class UnknownMethodHandler(RPCHandlerAPI):
    async def __call__(self, request: RPCRequest) -> RPCResponse:
        return generate_error_response(
            request, f"Unknown RPC method: {request['method']}"
        )


fallback_handler = UnknownMethodHandler()


class RPCServer(Service):
    logger = logging.getLogger("ddht.RPCServer")
    _handlers: Dict[str, RPCHandlerAPI]

    def __init__(
        self, ipc_path: pathlib.Path, handlers: Dict[str, RPCHandlerAPI]
    ) -> None:
        self.ipc_path = ipc_path
        self._handlers = handlers
        self._serving = trio.Event()

    async def wait_serving(self) -> None:
        await self._serving.wait()

    async def run(self) -> None:
        self.manager.run_daemon_task(self.serve, self.ipc_path)
        try:
            await self.manager.wait_finished()
        finally:
            self.ipc_path.unlink(missing_ok=True)

    async def execute_rpc(self, request: RPCRequest) -> str:
        method = request["method"]

        self.logger.debug("RPCServer handling request: %s", method)

        handler = self._handlers.get(method, fallback_handler)
        try:
            response = await handler(request)
        except Exception as err:
            self.logger.error("Error handling request: %s  error: %s", request, err)
            self.logger.debug("Error handling request: %s", request, exc_info=True)
            response = generate_error_response(request, f"Unexpected Error: {err}")

        return json.dumps(response)

    async def serve(self, ipc_path: pathlib.Path) -> None:
        self.logger.info("Starting RPC server over IPC socket: %s", ipc_path)

        with trio.socket.socket(trio.socket.AF_UNIX, trio.socket.SOCK_STREAM) as sock:
            await sock.bind(str(ipc_path))

            # Allow up to 10 pending connections.
            sock.listen(10)

            self._serving.set()

            while self.manager.is_running:
                conn, addr = await sock.accept()
                self.logger.debug("Server accepted connection: %r", addr)
                self.manager.run_task(self._handle_connection, conn)

    async def _handle_connection(self, socket: trio.socket.SocketType) -> None:
        buffer = io.StringIO()

        with socket:
            while True:
                try:
                    request = await read_json(socket, buffer)
                except DecodingError:
                    # If the connection receives bad JSON, close the connection.
                    return

                if not isinstance(request, collections.abc.Mapping):
                    logger.debug("Invalid payload: %s", type(request))
                    await write_error(socket, "Invalid Request: not a mapping")
                    continue

                if not request:
                    self.logger.debug("Client sent empty request")
                    await write_error(socket, "Invalid Request: empty")
                    continue

                try:
                    validate_request(request)
                except ValidationError as err:
                    await write_error(socket, str(err))
                    continue

                try:
                    result = await self.execute_rpc(cast(RPCRequest, request))
                except Exception as e:
                    self.logger.exception("Unrecognized exception while executing RPC")
                    await write_error(socket, "unknown failure: " + str(e))
                else:
                    if not result.endswith(NEW_LINE):
                        result += NEW_LINE

                    try:
                        await socket.send(result.encode("utf8"))
                    except BrokenPipeError:
                        break
