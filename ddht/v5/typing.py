from typing import (
    NamedTuple,
    NewType,
    Optional,
    TYPE_CHECKING,
)

from eth_typing import (
    Hash32,
)

from ddht.typing import SessionKeys

if TYPE_CHECKING:
    from ddht.enr import (  # noqa: F401
        ENR,
    )
    from ddht.v5.messages import (  # noqa: F401
        BaseMessage,
    )
    from ddht.v5.packets import (  # noqa: F401
        AuthHeaderPacket,
    )

Tag = NewType("Tag", bytes)

Topic = NewType("Topic", Hash32)


class HandshakeResult(NamedTuple):
    session_keys: SessionKeys
    enr: Optional["ENR"]
    message: Optional["BaseMessage"]
    auth_header_packet: Optional["AuthHeaderPacket"]
