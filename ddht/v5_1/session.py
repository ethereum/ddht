import enum
import logging
import secrets
from typing import Tuple, cast
import uuid

from eth_keys import keys
import rlp
import trio

from ddht.abc import NodeDBAPI
from ddht.base_message import BaseMessage, IncomingMessage, OutgoingMessage
from ddht.encryption import aesgcm_decrypt
from ddht.endpoint import Endpoint
from ddht.enr import ENR
from ddht.message_registry import MessageTypeRegistry
from ddht.typing import AES128Key, IDNonce, NodeID, Nonce, SessionKeys
from ddht.v5_1.abc import EventsAPI, SessionAPI
from ddht.v5_1.envelope import IncomingEnvelope, OutgoingEnvelope
from ddht.v5_1.events import Events
from ddht.v5_1.messages import v51_registry
from ddht.v5_1.packets import (
    HandshakeHeader,
    HandshakePacket,
    MessagePacket,
    Packet,
    WhoAreYouPacket,
)

RANDOM_ENCRYPTED_DATA_SIZE = 12


class SessionStatus(enum.Enum):
    BEFORE = enum.auto()
    DURING = enum.auto()
    AFTER = enum.auto()


class BaseSession(SessionAPI):
    _remote_node_id: NodeID
    _keys: SessionKeys

    logger = logging.getLogger("ddht.session.Session")

    def __init__(
        self,
        local_private_key: bytes,
        local_node_id: NodeID,
        remote_endpoint: Endpoint,
        node_db: NodeDBAPI,
        incoming_message_send_channel: trio.abc.SendChannel[IncomingMessage],
        outgoing_envelope_send_channel: trio.abc.SendChannel[OutgoingEnvelope],
        message_type_registry: MessageTypeRegistry = v51_registry,
        events: EventsAPI = None,
    ) -> None:
        self.id = uuid.uuid4()

        if events is None:
            events = Events()

        self._events = events

        self._local_private_key = local_private_key
        self._local_node_id = local_node_id
        self.remote_endpoint = remote_endpoint
        self._node_db = node_db

        self._message_type_registry = message_type_registry

        self._status = SessionStatus.BEFORE

        (
            self._outgoing_message_buffer_send_channel,
            self._outgoing_message_buffer_receive_channel,
        ) = trio.open_memory_channel[BaseMessage](256)
        (
            self._incoming_envelope_buffer_send_channel,
            self._incoming_envelope_buffer_receive_channel,
        ) = trio.open_memory_channel[IncomingEnvelope](256)

        self._incoming_message_send_channel = incoming_message_send_channel
        self._outgoing_envelope_send_channel = outgoing_envelope_send_channel

    @property
    def is_before_handshake(self) -> bool:
        return self._status is SessionStatus.BEFORE

    @property
    def is_during_handshake(self) -> bool:
        return self._status is SessionStatus.DURING

    @property
    def is_after_handshake(self) -> bool:
        return self._status is SessionStatus.AFTER

    @property
    def keys(self) -> SessionKeys:
        if self.is_after_handshake:
            return self._keys
        raise AttributeError(
            "Session keys are not available until after the handshake has completed"
        )

    async def _process_message_buffers(self) -> None:
        if not self.is_after_handshake:
            raise Exception("TODO")
        await self._incoming_envelope_buffer_send_channel.aclose()
        async with self._incoming_envelope_buffer_receive_channel:
            async for envelope in self._incoming_envelope_buffer_receive_channel:
                await self.handle_incoming_envelope(envelope)

        await self._outgoing_message_buffer_send_channel.aclose()
        async with self._outgoing_message_buffer_receive_channel:
            async for message in self._outgoing_message_buffer_receive_channel:
                await self.handle_outgoing_message(message)


class RandomMessage(BaseMessage):
    def __init__(self) -> None:
        self.random_data = secrets.token_bytes(RANDOM_ENCRYPTED_DATA_SIZE)

    def to_bytes(self) -> bytes:
        return self.random_data


class EmptyMessage(BaseMessage):
    def to_bytes(self) -> bytes:
        return b""


#
# Initiator
#
class SessionInitiator(BaseSession):
    is_initiator = True

    def __init__(
        self,
        local_private_key: bytes,
        local_node_id: NodeID,
        remote_node_id: NodeID,
        remote_endpoint: Endpoint,
        node_db: NodeDBAPI,
        incoming_message_send_channel: trio.abc.SendChannel[IncomingMessage],
        outgoing_envelope_send_channel: trio.abc.SendChannel[OutgoingEnvelope],
        message_type_registry: MessageTypeRegistry = v51_registry,
    ) -> None:
        super().__init__(
            local_private_key=local_private_key,
            local_node_id=local_node_id,
            remote_endpoint=remote_endpoint,
            node_db=node_db,
            incoming_message_send_channel=incoming_message_send_channel,
            outgoing_envelope_send_channel=outgoing_envelope_send_channel,
            message_type_registry=message_type_registry,
        )
        self._remote_node_id = remote_node_id

    @property
    def remote_node_id(self) -> NodeID:
        return self._remote_node_id

    @property
    def remote_enr(self) -> ENR:
        return self._node_db.get_enr(self.remote_node_id)

    async def handle_outgoing_message(self, message: OutgoingMessage) -> None:
        if self.is_after_handshake:
            raise NotImplementedError
        elif self.is_during_handshake:
            raise NotImplementedError
        elif self.is_before_handshake:
            self.logger.debug(
                "%s: outgoing message triggered handshake initiation: %s",
                self,
                message,
            )
            self._initial_message = message
            self._status = SessionStatus.DURING
            await self._send_handshake_initiation()
        else:
            raise Exception("Invariant: All states handled")

    async def handle_incoming_envelope(self, envelope: IncomingEnvelope) -> None:
        if self.is_after_handshake:
            raise NotImplementedError
        elif self.is_during_handshake:
            if envelope.packet.is_who_are_you:
                (
                    self._keys,
                    ephemeral_public_key,
                ) = await self._receive_handshake_response(
                    cast(Packet[WhoAreYouPacket], envelope.packet)
                )
                self._status = SessionStatus.AFTER

                await self._send_handshake_completion(
                    self._keys,
                    ephemeral_public_key,
                    cast(Packet[WhoAreYouPacket], envelope.packet),
                )
                await self._process_message_buffers()
            else:
                self.logger.debug(
                    "%s: expected who-are-you packet.  got %s", self, envelope
                )
                self._incoming_envelope_buffer_send_channel.send_nowait(envelope)
        elif self.is_before_handshake:
            # Likely that both nodes are handshaking with each other at the
            # same time...
            self._incoming_envelope_buffer_send_channel.send_nowait(envelope)
        else:
            raise Exception("Invariant: All states handled")

    async def _send_handshake_initiation(self) -> None:
        self._initiating_packet = Packet.prepare(
            nonce=cast(Nonce, secrets.token_bytes(12)),
            initiator_key=cast(AES128Key, secrets.token_bytes(16)),
            message=RandomMessage(),
            auth_data=MessagePacket(aes_gcm_nonce=cast(Nonce, secrets.token_bytes(12))),
            source_node_id=self._local_node_id,
            dest_node_id=self.remote_node_id,
        )
        await self._outgoing_envelope_send_channel.send(
            OutgoingEnvelope(
                packet=self._initiating_packet, receiver_endpoint=self.remote_endpoint,
            )
        )

    async def _receive_handshake_response(
        self, packet: Packet[WhoAreYouPacket],
    ) -> Tuple[SessionKeys, bytes]:
        self.logger.debug("%s: receiving handshake response", self)

        # compute session keys
        ephemeral_private_key = keys.PrivateKey(secrets.token_bytes(32))

        remote_enr = self.remote_enr
        session_keys = remote_enr.identity_scheme.compute_session_keys(
            local_private_key=ephemeral_private_key.to_bytes(),
            remote_public_key=remote_enr.public_key,
            local_node_id=self._local_node_id,
            remote_node_id=self.remote_node_id,
            id_nonce=packet.auth_data.id_nonce,
            is_locally_initiated=True,
        )

        return session_keys, ephemeral_private_key.public_key.to_compressed_bytes()

    async def _send_handshake_completion(
        self,
        session_keys: SessionKeys,
        ephemeral_public_key: bytes,
        packet: Packet[WhoAreYouPacket],
    ) -> None:
        self.logger.debug("%s: sending handshake completion", self)

        local_enr = self._node_db.get_enr(self._local_node_id)

        # prepare response packet
        id_nonce_signature = local_enr.identity_scheme.create_id_nonce_signature(
            id_nonce=packet.auth_data.id_nonce,
            ephemeral_public_key=ephemeral_public_key,
            private_key=self._local_private_key,
        )

        auth_data = HandshakePacket(
            auth_data_head=HandshakeHeader.v4_header(),
            id_signature=id_nonce_signature,
            ephemeral_public_key=ephemeral_public_key,
            record=(
                local_enr
                if packet.auth_data.enr_sequence_number < local_enr.sequence_number
                else None
            ),
        )
        handshake_packet = Packet.prepare(
            nonce=packet.auth_data.request_nonce,
            initiator_key=self.keys.encryption_key,
            message=self._initial_message.message,
            auth_data=auth_data,
            source_node_id=self._local_node_id,
            dest_node_id=self._remote_node_id,
        )

        await self._outgoing_envelope_send_channel.send(
            OutgoingEnvelope(
                packet=handshake_packet, receiver_endpoint=self.remote_endpoint,
            )
        )


class SessionRecipient(BaseSession):
    is_initiator = False

    @property
    def remote_node_id(self) -> NodeID:
        if self.is_before_handshake:
            raise AttributeError("NodeID for remote not yet known")
        return self._remote_node_id

    async def handle_outgoing_message(self, message: OutgoingMessage) -> None:
        if self.is_after_handshake:
            raise NotImplementedError
        elif self.is_during_handshake:
            raise NotImplementedError
        elif self.is_before_handshake:
            raise NotImplementedError
        else:
            raise Exception("Invariant: All states handled")

    async def handle_incoming_envelope(self, envelope: IncomingEnvelope) -> None:
        if self.is_after_handshake:
            raise NotImplementedError
        elif self.is_during_handshake:
            if envelope.packet.is_handshake:
                self._keys = await self._receive_handshake_completion(
                    cast(Packet[HandshakePacket], envelope.packet),
                )
                self._status = SessionStatus.AFTER
            else:
                self.logger.error(
                    "%s: received unexpected message during handshake: %s", envelope
                )
                self._incoming_envelope_buffer_send_channel.send_nowait(envelope)
        elif self.is_before_handshake:
            if envelope.packet.is_message:
                self.logger.debug("%s: received handshake initiation", self)
                self._status = SessionStatus.DURING
                self._remote_node_id = envelope.packet.header.source_node_id
                await self._send_handshake_response(
                    cast(Packet[MessagePacket], envelope.packet),
                    envelope.sender_endpoint,
                )
            else:
                self.logger.error(
                    "%s: received unexpected message before handshake: %s", envelope
                )
                # TODO: full buffer handling
                # TODO: manage buffer...
                self._incoming_envelope_buffer_send_channel.send_nowait(envelope)
        else:
            raise Exception("Invariant: All states handled")

    async def _send_handshake_response(
        self, packet: Packet[MessagePacket], sender_endpoint: Endpoint
    ) -> None:
        self.logger.debug("%s: sending handshake response", self)
        try:
            remote_enr = self._node_db.get_enr(packet.header.source_node_id)
        except KeyError:
            enr_sequence_number = 0
        else:
            enr_sequence_number = remote_enr.sequence_number

        auth_data = WhoAreYouPacket(
            request_nonce=packet.auth_data.aes_gcm_nonce,
            id_nonce=cast(IDNonce, secrets.token_bytes(32)),
            enr_sequence_number=enr_sequence_number,
        )

        self.handshake_response_packet = Packet.prepare(
            nonce=cast(Nonce, secrets.token_bytes(12)),
            initiator_key=cast(AES128Key, secrets.token_bytes(16)),
            message=EmptyMessage(),
            auth_data=auth_data,
            source_node_id=self._local_node_id,
            dest_node_id=packet.header.source_node_id,
        )
        await self._outgoing_envelope_send_channel.send(
            OutgoingEnvelope(
                packet=self.handshake_response_packet,
                receiver_endpoint=sender_endpoint,
            )
        )

    async def _receive_handshake_completion(
        self, packet: Packet[HandshakePacket]
    ) -> SessionKeys:
        self.logger.debug("%s: received handshake completion", self)

        if not isinstance(packet.auth_data, HandshakePacket):
            raise Exception(f"Invalid packet type: {type(packet.auth_data)}")

        if packet.auth_data.record is not None:
            remote_enr = packet.auth_data.record
            self._node_db.set_enr(remote_enr)
        else:
            remote_enr = self._node_db.get_enr(self.remote_node_id)

        session_keys = remote_enr.identity_scheme.compute_session_keys(
            local_private_key=self._local_private_key,
            remote_public_key=packet.auth_data.ephemeral_public_key,
            local_node_id=self._local_node_id,
            remote_node_id=self.remote_node_id,
            id_nonce=self.handshake_response_packet.auth_data.id_nonce,
            is_locally_initiated=False,
        )

        authenticated_data = (
            packet.header.to_wire_bytes() + packet.auth_data.to_wire_bytes()
        )
        message_plain_text = aesgcm_decrypt(
            key=session_keys.decryption_key,
            nonce=self.handshake_response_packet.auth_data.request_nonce,
            cipher_text=packet.message_cipher_text,
            authenticated_data=authenticated_data,
        )
        message_type = message_plain_text[0]
        message_sedes = self._message_type_registry[message_type]
        message = rlp.decode(message_plain_text[1:], sedes=message_sedes)

        await self._incoming_message_send_channel.send(
            IncomingMessage(
                message=message,
                sender_endpoint=self.remote_endpoint,
                sender_node_id=self.remote_node_id,
            )
        )
        return session_keys
