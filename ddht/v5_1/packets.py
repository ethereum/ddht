from dataclasses import dataclass, field
from io import BytesIO
import secrets
import struct
from typing import Generic, NamedTuple, Optional, TypeVar, Union, cast

from eth_enr.abc import ENRAPI
from eth_enr.sedes import ENRSedes
from eth_typing import NodeID
from eth_utils.toolz import take
import rlp

from ddht.base_message import BaseMessage
from ddht.encryption import aesctr_decrypt_stream, aesctr_encrypt, aesgcm_encrypt
from ddht.exceptions import DecodingError
from ddht.typing import AES128Key, IDNonce, Nonce

PROTOCOL_ID = b"discv5  "


UINT8_TO_BYTES = {v: bytes([v]) for v in range(256)}


@dataclass(frozen=True)
class MessagePacket:
    aes_gcm_nonce: Nonce  # 96 bit AES/GCM nonce

    flag: int = field(init=False, repr=False, default=0)

    def to_wire_bytes(self) -> bytes:
        return self.aes_gcm_nonce

    @classmethod
    def from_wire_bytes(cls, data: bytes) -> "MessagePacket":
        if len(data) != 12:
            raise DecodingError(
                f"Invalid length for MessagePacket: length={len(data)}  data={data.hex()}"
            )
        return cls(cast(Nonce, data))


@dataclass(frozen=True)
class WhoAreYouPacket:
    request_nonce: Nonce  # uint96
    id_nonce: IDNonce  # uint256
    enr_sequence_number: int  # uint64

    flag: int = field(init=False, repr=False, default=1)

    def to_wire_bytes(self) -> bytes:
        return b"".join(
            (
                self.request_nonce,
                self.id_nonce,
                struct.pack(">Q", self.enr_sequence_number),
            )
        )

    @classmethod
    def from_wire_bytes(cls, data: bytes) -> "WhoAreYouPacket":
        if len(data) != 52:
            raise DecodingError(
                f"Invalid length for WhoAreYouPacket: length={len(data)}  data={data.hex()}"
            )
        stream = BytesIO(data)
        request_nonce = cast(Nonce, stream.read(12))
        id_nonce = cast(IDNonce, stream.read(32))
        enr_sequence_number = int.from_bytes(stream.read(8), "big")
        return cls(request_nonce, id_nonce, enr_sequence_number)


class HandshakeHeader(NamedTuple):
    version: int  # uint8  (1 for v4)
    signature_size: int  # uint8  (64 for v4)
    ephemeral_key_size: int  # uint8 (33 for v4)

    def to_wire_bytes(self) -> bytes:
        return b"".join(
            (
                UINT8_TO_BYTES[self.version],
                UINT8_TO_BYTES[self.signature_size],
                UINT8_TO_BYTES[self.ephemeral_key_size],
            )
        )

    @classmethod
    def v4_header(cls) -> "HandshakeHeader":
        return cls(1, 64, 33)

    @classmethod
    def from_wire_bytes(cls, data: bytes) -> "HandshakeHeader":
        if len(data) != 3:
            raise DecodingError(
                f"Invalid length for HandshakeHeader: length={len(data)}  data={data.hex()}"
            )
        version = data[0]
        signature_size = data[1]
        ephemeral_key_size = data[2]
        return cls(version, signature_size, ephemeral_key_size)


@dataclass(frozen=True)
class HandshakePacket:
    auth_data_head: HandshakeHeader
    id_signature: bytes
    ephemeral_public_key: bytes
    record: Optional[ENRAPI]

    flag: int = field(init=False, repr=False, default=2)

    def to_wire_bytes(self) -> bytes:
        return b"".join(
            (
                self.auth_data_head.to_wire_bytes(),
                self.id_signature,
                self.ephemeral_public_key,
                (b"" if self.record is None else rlp.encode(self.record, ENRSedes)),
            )
        )

    @classmethod
    def from_wire_bytes(cls, data: bytes) -> "HandshakePacket":
        stream = BytesIO(data)
        auth_data_head = HandshakeHeader.from_wire_bytes(stream.read(3))
        expected_length = (
            3 + auth_data_head.signature_size + auth_data_head.ephemeral_key_size
        )
        if len(data) < expected_length:
            raise DecodingError(
                f"Invalid length for HandshakePacket: "
                f"expected={expected_length}  actual={len(data)}  "
                f"data={data.hex()}"
            )
        id_signature = stream.read(auth_data_head.signature_size)
        ephemeral_public_key = stream.read(auth_data_head.ephemeral_key_size)
        enr_bytes = stream.read()
        if len(enr_bytes) > 0:
            try:
                enr = rlp.decode(enr_bytes, sedes=ENRSedes)
            except rlp.DecodingError as err:
                # re-raise using the local library DecodingError instead of the
                # rlp one
                raise DecodingError(str(err)) from err
        else:
            enr = None

        return cls(auth_data_head, id_signature, ephemeral_public_key, enr,)


class Header(NamedTuple):
    protocol_id: bytes
    source_node_id: NodeID
    flag: int  # uint8
    auth_data_size: int  # uint16

    def to_wire_bytes(self) -> bytes:
        return b"".join(
            (
                self.protocol_id,
                self.source_node_id,
                UINT8_TO_BYTES[self.flag],
                self.auth_data_size.to_bytes(2, "big"),
            )
        )

    @classmethod
    def from_wire_bytes(cls, data: bytes) -> "Header":
        if len(data) != 43:
            raise DecodingError(
                f"Invalid length for Header: length={len(data)}  data={data.hex()}"
            )
        protocol_id = data[:8]
        remote_node_id = cast(NodeID, data[8:40])
        flag = data[40]
        auth_data_size = int.from_bytes(data[41:43], "big")
        return cls(protocol_id, remote_node_id, flag, auth_data_size)


AuthData = Union[MessagePacket, WhoAreYouPacket, HandshakePacket]
TAuthData = TypeVar("TAuthData", bound=AuthData)


@dataclass(frozen=True)
class Packet(Generic[TAuthData]):
    header: Header
    auth_data: TAuthData
    message_cipher_text: bytes
    dest_node_id: NodeID

    def __str__(self) -> str:
        return (
            f"Packet[{self.auth_data.__class__.__name__}]"
            f"(header={self.header}, auth_data={self.auth_data}, "
            f"message_cipher_text={self.message_cipher_text!r})"
        )

    @property
    def is_message(self) -> bool:
        return type(self.auth_data) is MessagePacket

    @property
    def is_who_are_you(self) -> bool:
        return type(self.auth_data) is WhoAreYouPacket

    @property
    def is_handshake(self) -> bool:
        return type(self.auth_data) is HandshakePacket

    @classmethod
    def prepare(
        cls,
        *,
        nonce: Nonce,
        initiator_key: AES128Key,
        message: BaseMessage,
        auth_data: TAuthData,
        source_node_id: NodeID,
        dest_node_id: NodeID,
        protocol_id: bytes = PROTOCOL_ID,
    ) -> "Packet[TAuthData]":
        auth_data_bytes = auth_data.to_wire_bytes()
        auth_data_size = len(auth_data_bytes)
        header = Header(protocol_id, source_node_id, auth_data.flag, auth_data_size,)
        message_cipher_text = aesgcm_encrypt(
            key=initiator_key,
            nonce=nonce,
            plain_text=message.to_bytes(),
            authenticated_data=header.to_wire_bytes() + auth_data.to_wire_bytes(),
        )
        return cls(
            header=header,
            auth_data=auth_data,
            message_cipher_text=message_cipher_text,
            dest_node_id=dest_node_id,
        )

    def to_wire_bytes(self) -> bytes:
        auth_data_bytes = self.auth_data.to_wire_bytes()
        header_wire_bytes = self.header.to_wire_bytes()
        plain_header = header_wire_bytes + auth_data_bytes
        masking_key = cast(AES128Key, self.dest_node_id[:16])
        masking_iv = secrets.token_bytes(16)
        masked_header = aesctr_encrypt(masking_key, masking_iv, plain_header)
        return b"".join((masking_iv, masked_header, self.message_cipher_text,))


AnyPacket = Union[
    Packet[MessagePacket], Packet[WhoAreYouPacket], Packet[HandshakePacket],
]


def decode_packet(data: bytes, local_node_id: NodeID,) -> AnyPacket:
    iv = data[:16]
    masking_key = cast(AES128Key, local_node_id[:16])
    cipher_text_stream = aesctr_decrypt_stream(masking_key, iv, data[16:])

    # Decode the header
    header_bytes = bytes(take(43, cipher_text_stream))
    header = Header.from_wire_bytes(header_bytes)

    auth_data_bytes = bytes(take(header.auth_data_size, cipher_text_stream))
    auth_data: Union[MessagePacket, WhoAreYouPacket, HandshakePacket]
    if header.flag == 0:
        auth_data = MessagePacket.from_wire_bytes(auth_data_bytes)
    elif header.flag == 1:
        auth_data = WhoAreYouPacket.from_wire_bytes(auth_data_bytes)
    elif header.flag == 2:
        auth_data = HandshakePacket.from_wire_bytes(auth_data_bytes)
    else:
        raise DecodingError(f"Unable to decode datagram: {data.hex()}", data)

    message_cipher_text = data[16 + 43 + header.auth_data_size :]

    return cast(
        AnyPacket,
        Packet(header, auth_data, message_cipher_text, dest_node_id=local_node_id),
    )
