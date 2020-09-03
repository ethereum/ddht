import secrets
from typing import NamedTuple, Optional

from eth_typing import NodeID
import factory
import trio

from ddht.base_message import AnyInboundMessage, BaseMessage
from ddht.typing import AES128Key, Nonce
from ddht.v5_1.envelope import OutboundEnvelope
from ddht.v5_1.packets import (
    PROTOCOL_ID,
    HandshakeHeader,
    HandshakePacket,
    MessagePacket,
    Packet,
    TAuthData,
    WhoAreYouPacket,
)
from ddht.v5_1.session import EmptyMessage, RandomMessage


class WhoAreYouPacketFactory(factory.Factory):  # type: ignore
    class Meta:
        model = WhoAreYouPacket

    request_nonce = factory.LazyFunction(lambda: secrets.token_bytes(12))
    id_nonce = factory.LazyFunction(lambda: secrets.token_bytes(32))
    enr_sequence_number = 0


class HandshakeHeaderFactory(factory.Factory):  # type: ignore
    class Meta:
        model = HandshakeHeader

    version = 1
    signature_size = 64
    ephemeral_key_size = 33


class HandshakePacketFactory(factory.Factory):  # type: ignore
    class Meta:
        model = HandshakePacket

    auth_data_head = factory.SubFactory(HandshakeHeaderFactory)
    id_signature = factory.LazyFunction(lambda: secrets.token_bytes(64))
    ephemeral_public_key = factory.LazyFunction(lambda: secrets.token_bytes(33))
    record = None


class PacketFactory:
    @staticmethod
    def _prepare(
        *,
        nonce: Optional[Nonce] = None,
        initiator_key: Optional[AES128Key] = None,
        message: BaseMessage,
        auth_data: TAuthData,
        source_node_id: Optional[NodeID] = None,
        dest_node_id: Optional[NodeID] = None,
        protocol_id: bytes = PROTOCOL_ID
    ) -> Packet[TAuthData]:
        if nonce is None:
            nonce = Nonce(secrets.token_bytes(12))

        if initiator_key is None:
            initiator_key = AES128Key(secrets.token_bytes(16))

        if source_node_id is None:
            source_node_id = NodeID(secrets.token_bytes(32))

        if dest_node_id is None:
            dest_node_id = NodeID(secrets.token_bytes(32))

        return Packet.prepare(
            nonce=nonce,
            initiator_key=initiator_key,
            message=message,
            auth_data=auth_data,
            source_node_id=source_node_id,
            dest_node_id=dest_node_id,
            protocol_id=protocol_id,
        )

    @classmethod
    def message(
        cls,
        *,
        nonce: Optional[Nonce] = None,
        initiator_key: Optional[AES128Key] = None,
        message: Optional[BaseMessage] = None,
        source_node_id: Optional[NodeID] = None,
        dest_node_id: Optional[NodeID] = None,
        protocol_id: bytes = PROTOCOL_ID
    ) -> Packet[MessagePacket]:
        if nonce is None:
            nonce = Nonce(secrets.token_bytes(12))

        auth_data = MessagePacket(nonce)

        if message is None:
            message = RandomMessage()

        return cls._prepare(
            nonce=nonce,
            initiator_key=initiator_key,
            message=message,
            auth_data=auth_data,
            source_node_id=source_node_id,
            dest_node_id=dest_node_id,
            protocol_id=protocol_id,
        )

    @classmethod
    def who_are_you(
        cls,
        *,
        nonce: Optional[Nonce] = None,
        initiator_key: Optional[AES128Key] = None,
        message: Optional[BaseMessage] = None,
        source_node_id: Optional[NodeID] = None,
        dest_node_id: Optional[NodeID] = None,
        protocol_id: bytes = PROTOCOL_ID
    ) -> Packet[MessagePacket]:
        auth_data = WhoAreYouPacketFactory()
        message = EmptyMessage()

        return cls._prepare(
            nonce=nonce,
            initiator_key=initiator_key,
            message=message,
            auth_data=auth_data,
            source_node_id=source_node_id,
            dest_node_id=dest_node_id,
            protocol_id=protocol_id,
        )

    @classmethod
    def handshake(
        cls,
        *,
        nonce: Optional[Nonce] = None,
        initiator_key: Optional[AES128Key] = None,
        message: Optional[BaseMessage] = None,
        source_node_id: Optional[NodeID] = None,
        dest_node_id: Optional[NodeID] = None,
        protocol_id: bytes = PROTOCOL_ID
    ) -> Packet[MessagePacket]:
        auth_data = HandshakePacketFactory()

        if message is None:
            message = EmptyMessage()

        return cls._prepare(
            nonce=nonce,
            initiator_key=initiator_key,
            message=message,
            auth_data=auth_data,
            source_node_id=source_node_id,
            dest_node_id=dest_node_id,
            protocol_id=protocol_id,
        )


class SessionChannels(NamedTuple):
    inbound_message_send_channel: trio.abc.SendChannel[AnyInboundMessage]
    inbound_message_receive_channel: trio.abc.ReceiveChannel[AnyInboundMessage]
    outbound_envelope_send_channel: trio.abc.SendChannel[OutboundEnvelope]
    outbound_envelope_receive_channel: trio.abc.ReceiveChannel[OutboundEnvelope]

    @classmethod
    def init(cls) -> "SessionChannels":
        (
            inbound_message_send_channel,
            inbound_message_receive_channel,
        ) = trio.open_memory_channel[AnyInboundMessage](256)
        (
            outbound_envelope_send_channel,
            outbound_envelope_receive_channel,
        ) = trio.open_memory_channel[OutboundEnvelope](256)
        return cls(
            inbound_message_send_channel,
            inbound_message_receive_channel,
            outbound_envelope_send_channel,
            outbound_envelope_receive_channel,
        )
