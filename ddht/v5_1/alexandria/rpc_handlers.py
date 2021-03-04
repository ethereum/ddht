from typing import Iterable, Tuple

from eth_utils import encode_hex, to_dict
import trio

from ddht.abc import RPCHandlerAPI
from ddht.rpc import RPCError, RPCHandler, RPCRequest, extract_params
from ddht.v5_1.alexandria.abc import AlexandriaNetworkAPI, ContentStorageAPI
from ddht.v5_1.alexandria.content_storage import ContentNotFound
from ddht.v5_1.alexandria.typing import ContentKey
from ddht.validation import validate_and_convert_hexstr, validate_params_length


class AddContentHandler(RPCHandler[Tuple[ContentKey, bytes], None]):
    def __init__(self, storage: ContentStorageAPI) -> None:
        self._storage = storage

    def extract_params(self, request: RPCRequest) -> Tuple[ContentKey, bytes]:
        raw_params = extract_params(request)

        validate_params_length(raw_params, 2)

        content_key_hex, content_hex = raw_params

        content_key = ContentKey(validate_and_convert_hexstr(content_key_hex)[0])
        content = validate_and_convert_hexstr(content_hex)[0]

        return content_key, content

    async def do_call(self, params: Tuple[ContentKey, bytes]) -> None:
        content_key, content = params
        self._storage.set_content(content_key, content)


class GetContentHandler(RPCHandler[ContentKey, str]):
    def __init__(self, storage: ContentStorageAPI) -> None:
        self._storage = storage

    def extract_params(self, request: RPCRequest) -> ContentKey:
        raw_params = extract_params(request)

        validate_params_length(raw_params, 1)

        content_key_hex = raw_params[0]
        content_key = ContentKey(validate_and_convert_hexstr(content_key_hex)[0])

        return content_key

    async def do_call(self, params: ContentKey) -> str:
        content_key = params
        try:
            content = self._storage.get_content(content_key)
        except ContentNotFound as err:
            raise RPCError(str(err)) from err
        else:
            return encode_hex(content)


class DeleteContentHandler(RPCHandler[ContentKey, None]):
    def __init__(self, storage: ContentStorageAPI) -> None:
        self._storage = storage

    def extract_params(self, request: RPCRequest) -> ContentKey:
        raw_params = extract_params(request)

        validate_params_length(raw_params, 1)

        content_key_hex = raw_params[0]
        content_key = ContentKey(validate_and_convert_hexstr(content_key_hex)[0])

        return content_key

    async def do_call(self, params: ContentKey) -> None:
        content_key = params
        try:
            self._storage.delete_content(content_key)
        except ContentNotFound as err:
            raise RPCError(str(err)) from err


class RetrieveContentHandler(RPCHandler[ContentKey, str]):
    def __init__(self, network: AlexandriaNetworkAPI) -> None:
        self._network = network

    def extract_params(self, request: RPCRequest) -> ContentKey:
        raw_params = extract_params(request)

        validate_params_length(raw_params, 1)

        content_key_hex = raw_params[0]
        content_key = ContentKey(validate_and_convert_hexstr(content_key_hex)[0])

        return content_key

    async def do_call(self, params: ContentKey) -> str:
        content_key = params

        content: bytes

        try:
            content = self._network.storage.get_content(content_key)
        except ContentNotFound:
            pass
        else:
            return encode_hex(content)

        try:
            content = await self._network.retrieve_content(content_key)
        except trio.TooSlowError as err:
            raise RPCError(
                f"Timeout retrieving content: content_key={content_key.hex()}"
            ) from err
        else:
            return encode_hex(content)


@to_dict
def get_alexandria_rpc_handlers(
    network: AlexandriaNetworkAPI,
) -> Iterable[Tuple[str, RPCHandlerAPI]]:
    yield (
        "alexandria_addContent",
        AddContentHandler(network.storage),
    )
    yield (
        "alexandria_getContent",
        GetContentHandler(network.storage),
    )
    yield (
        "alexandria_deleteContent",
        DeleteContentHandler(network.storage),
    )
    yield (
        "alexandria_retrieveContent",
        RetrieveContentHandler(network),
    )
