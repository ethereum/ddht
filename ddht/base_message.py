from eth_utils import int_to_big_endian
import rlp


class BaseMessage(rlp.Serializable):  # type: ignore
    message_type: int

    def to_bytes(self) -> bytes:
        return b"".join((int_to_big_endian(self.message_type), rlp.encode(self)))
