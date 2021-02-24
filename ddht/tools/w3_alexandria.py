from typing import Any, Callable, Tuple

from eth_utils import decode_hex, encode_hex

try:
    import web3  # noqa: F401
except ImportError:
    raise ImportError("The web3.py library is required")


from eth_typing import HexStr
from web3.method import Method
from web3.module import ModuleV2
from web3.types import RPCEndpoint

from ddht.v5_1.alexandria.typing import ContentKey


class RPC:
    # core
    getContent = RPCEndpoint("alexandria_getContent")

    # commons
    getCommonsContent = RPCEndpoint("alexandria_getCommonsContent")
    addCommonsContent = RPCEndpoint("alexandria_addCommonsContent")
    deleteCommonsContent = RPCEndpoint("alexandria_deleteCommonsContent")

    # pinned
    getPinnedContent = RPCEndpoint("alexandria_getPinnedContent")
    addPinnedContent = RPCEndpoint("alexandria_addPinnedContent")
    deletePinnedContent = RPCEndpoint("alexandria_deletePinnedContent")


#
# Mungers
# See: https://github.com/ethereum/web3.py/blob/002151020cecd826a694ded2fdc10cc70e73e636/web3/method.py#L77  # noqa: E501
#
def content_key_munger(module: Any, content_key: ContentKey,) -> Tuple[HexStr]:
    """
    Normalizes the inputs JSON-RPC endpoints that take a single `ContentKey`
    """
    return (encode_hex(content_key),)


def content_key_and_content_munger(
    module: Any, content_key: ContentKey, content: bytes,
) -> Tuple[HexStr, HexStr]:
    """
    Normalizes the inputs JSON-RPC endpoints that take a 2-tuple of
    `(ContentKey, bytes)`
    """
    return (
        encode_hex(content_key),
        encode_hex(content),
    )


class AlexandriaModule(ModuleV2):  # type: ignore
    """
    A web3.py module that exposes high level APIs for interacting with the
    discovery v5 network.
    """
    #
    # Live Content Retrieval
    #
    retrieve_content: Method[Callable[[ContentKey], bytes]] = Method(
        RPC.retrieveContent,
        result_formatters=lambda method, module: decode_hex,
        mungers=(content_key_munger,),
    )

    #
    # Local Storage
    #
    get_content: Method[Callable[[ContentKey], bytes]] = Method(
        RPC.getContent,
        result_formatters=lambda method, module: decode_hex,
        mungers=(content_key_munger,),
    )
    add_content: Method[Callable[[ContentKey], bytes]] = Method(
        RPC.addContent, mungers=(content_key_and_content_munger,),
    )
    delete_content: Method[Callable[[ContentKey], bytes]] = Method(
        RPC.deleteContent, mungers=(content_key_munger,),
    )
