from typing import Iterable, Tuple

from eth_utils import encode_hex, to_dict

from ddht.abc import RPCHandlerAPI
from ddht.rpc import RPCError, RPCHandler, RPCRequest, extract_params
from ddht.v5_1.alexandria.abc import (
    AlexandriaNetworkAPI,
    ContentManagerAPI,
    ContentStorageAPI,
)
from ddht.v5_1.alexandria.content_storage import ContentNotFound
from ddht.v5_1.alexandria.typing import ContentKey
from ddht.validation import validate_and_convert_hexstr, validate_params_length


class AddContentHandler(RPCHandler[Tuple[ContentKey, bytes], None]):
    def __init__(self, content_manager: ContentManagerAPI) -> None:
        self._content_manager = content_manager

    def extract_params(self, request: RPCRequest) -> Tuple[ContentKey, bytes]:
        raw_params = extract_params(request)

        validate_params_length(raw_params, 2)

        content_key_hex, content_hex = raw_params

        content_key = ContentKey(validate_and_convert_hexstr(content_key_hex)[0])
        content = validate_and_convert_hexstr(content_hex)[0]

        return content_key, content

    async def do_call(self, params: Tuple[ContentKey, bytes]) -> None:
        content_key, content = params
        await self._content_manager.process_content(content_key, content)


class GetContentHandler(RPCHandler[ContentKey, str]):
    def __init__(self, content_storage: ContentStorageAPI) -> None:
        self._content_storage = content_storage

    def extract_params(self, request: RPCRequest) -> ContentKey:
        raw_params = extract_params(request)

        validate_params_length(raw_params, 1)

        content_key_hex = raw_params[0]
        content_key = ContentKey(validate_and_convert_hexstr(content_key_hex)[0])

        return content_key

    async def do_call(self, params: ContentKey) -> str:
        content_key = params
        try:
            content = self._content_storage.get_content(content_key)
        except ContentNotFound as err:
            raise RPCError(str(err)) from err
        else:
            return encode_hex(content)


class DeleteContentHandler(RPCHandler[ContentKey, None]):
    def __init__(self, content_storage: ContentStorageAPI) -> None:
        self._content_storage = content_storage

    def extract_params(self, request: RPCRequest) -> ContentKey:
        raw_params = extract_params(request)

        validate_params_length(raw_params, 1)

        content_key_hex = raw_params[0]
        content_key = ContentKey(validate_and_convert_hexstr(content_key_hex)[0])

        return content_key

    async def do_call(self, params: ContentKey) -> None:
        content_key = params
        try:
            self._content_storage.delete_content(content_key)
        except ContentNotFound as err:
            raise RPCError(str(err)) from err


@to_dict
def get_alexandria_rpc_handlers(
    network: AlexandriaNetworkAPI,
) -> Iterable[Tuple[str, RPCHandlerAPI]]:
    yield (
        "alexandria_addPinnedContent",
        AddContentHandler(network.pinned_content_manager),
    )
    yield (
        "alexandria_addCommonsContent",
        AddContentHandler(network.commons_content_manager),
    )
    yield (
        "alexandria_getPinnedContent",
        GetContentHandler(network.pinned_content_storage),
    )
    yield (
        "alexandria_getCommonsContent",
        GetContentHandler(network.commons_content_storage),
    )
    yield (
        "alexandria_deletePinnedContent",
        DeleteContentHandler(network.pinned_content_storage),
    )
    yield (
        "alexandria_deleteCommonsContent",
        DeleteContentHandler(network.commons_content_storage),
    )
