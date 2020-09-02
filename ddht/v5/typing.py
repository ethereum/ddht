from typing import TYPE_CHECKING, NamedTuple, NewType, Optional

from eth_enr.abc import ENRAPI
from eth_typing import Hash32

from ddht.base_message import BaseMessage
from ddht.typing import SessionKeys

if TYPE_CHECKING:
    from ddht.v5.packets import AuthHeaderPacket  # noqa: F401

Tag = NewType("Tag", bytes)

Topic = NewType("Topic", Hash32)


class HandshakeResult(NamedTuple):
    session_keys: SessionKeys
    enr: Optional[ENRAPI]
    message: Optional[BaseMessage]
    auth_header_packet: Optional["AuthHeaderPacket"]
