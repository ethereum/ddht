from abc import abstractmethod
import enum
import itertools
import secrets
from typing import Any, Optional, Tuple, Type, cast
import uuid

from eth_enr import ENRAPI, ENRDatabaseAPI, IdentitySchemeAPI, OldSequenceNumber
from eth_keys import keys
from eth_typing import NodeID
from eth_utils import ExtendedDebugLogger, ValidationError, get_extended_debug_logger
import trio

from ddht._utils import humanize_node_id
from ddht.abc import HandshakeSchemeAPI, HandshakeSchemeRegistryAPI
from ddht.base_message import AnyInboundMessage, AnyOutboundMessage, BaseMessage
from ddht.endpoint import Endpoint
from ddht.exceptions import DecryptionError, HandshakeFailure
from ddht.message_registry import MessageTypeRegistry
from ddht.typing import AES128Key, IDNonce, Nonce, SessionKeys
from ddht.v5_1.abc import EventsAPI, SessionAPI
from ddht.v5_1.constants import SESSION_IDLE_TIMEOUT
from ddht.v5_1.envelope import InboundEnvelope, OutboundEnvelope
from ddht.v5_1.events import Events
from ddht.v5_1.handshake_schemes import v51_handshake_scheme_registry
from ddht.v5_1.messages import decode_message, v51_registry
from ddht.v5_1.packets import (
    HandshakeHeader,
    HandshakePacket,
    MessagePacket,
    Packet,
    WhoAreYouPacket,
)

RANDOM_ENCRYPTED_DATA_SIZE = 12


class SessionStatus(enum.Enum):
    BEFORE = "|"
    DURING = "~"
    AFTER = "-"


class BaseSession(SessionAPI):
    _remote_node_id: NodeID
    _keys: SessionKeys

    _handshake_scheme_registry: HandshakeSchemeRegistryAPI = v51_handshake_scheme_registry

    logger: ExtendedDebugLogger

    def __init__(
        self,
        local_private_key: bytes,
        local_node_id: NodeID,
        remote_endpoint: Endpoint,
        enr_db: ENRDatabaseAPI,
        inbound_message_send_channel: trio.abc.SendChannel[AnyInboundMessage],
        outbound_envelope_send_channel: trio.abc.SendChannel[OutboundEnvelope],
        message_type_registry: MessageTypeRegistry = v51_registry,
        events: Optional[EventsAPI] = None,
    ) -> None:
        self.logger = get_extended_debug_logger("ddht.Session")

        self.id = uuid.uuid4()

        self.created_at = trio.current_time()
        self._handshake_complete = trio.Event()

        if events is None:
            events = Events()

        self._events = events
        self._nonce_counter = itertools.count()

        self._local_private_key = local_private_key
        self._local_node_id = local_node_id
        self.remote_endpoint = remote_endpoint
        self._enr_db = enr_db

        self._message_type_registry = message_type_registry

        self._status = SessionStatus.BEFORE

        (
            self._outbound_message_buffer_send_channel,
            self._outbound_message_buffer_receive_channel,
        ) = trio.open_memory_channel[AnyOutboundMessage](256)

        self._inbound_message_send_channel = inbound_message_send_channel
        self._outbound_envelope_send_channel = outbound_envelope_send_channel

    def __str__(self) -> str:
        if self.is_initiator:
            connector = f"-{self._status.value}->"
        else:
            connector = f"<-{self._status.value}-"

        if self.is_after_handshake:
            remote_display = (
                f"{humanize_node_id(self.remote_node_id)}@{self.remote_endpoint}"
            )
        else:
            remote_display = f"UNKNOWN@{self.remote_endpoint}"

        return (
            "Session["
            f"{humanize_node_id(self._local_node_id)}"
            f"{connector}"
            f"{remote_display}"
            "]"
        )

    @property
    def is_recipient(self) -> bool:
        return not self.is_initiator

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
    def is_timed_out(self) -> bool:
        if self.is_after_handshake:
            return False
        return self.timeout_at <= trio.current_time()

    @property
    def timeout_at(self) -> float:
        return self.created_at + SESSION_IDLE_TIMEOUT

    @property
    def local_enr(self) -> ENRAPI:
        return self._enr_db.get_enr(self._local_node_id)

    @property
    def identity_scheme(self) -> Type[IdentitySchemeAPI]:
        return self.local_enr.identity_scheme

    @property
    def handshake_scheme(self) -> Type[HandshakeSchemeAPI[Any]]:
        return self._handshake_scheme_registry[self.identity_scheme]

    @property
    def keys(self) -> SessionKeys:
        if self.is_after_handshake:
            return self._keys
        raise AttributeError(
            "Session keys are not available until after the handshake has completed"
        )

    @abstractmethod
    async def _process_message_buffers(self) -> None:
        ...

    async def await_handshake_completion(self) -> None:
        if self.is_after_handshake:
            raise Exception("Handshake is already complete")

        await self._handshake_complete.wait()

    def decode_message(self, packet: Packet[MessagePacket]) -> BaseMessage:
        return decode_message(
            self.keys.decryption_key,
            packet.header.aes_gcm_nonce,
            packet.message_cipher_text,
            packet.challenge_data,
            self._message_type_registry,
        )

    def get_encryption_nonce(self) -> Nonce:
        return Nonce(
            next(self._nonce_counter).to_bytes(4, "big") + secrets.token_bytes(8)
        )

    def prepare_envelope(self, message: AnyOutboundMessage) -> OutboundEnvelope:
        if not self.is_after_handshake:
            raise Exception("Invalid")
        nonce = self.get_encryption_nonce()
        auth_data = MessagePacket(source_node_id=self._local_node_id)
        packet = Packet.prepare(
            aes_gcm_nonce=nonce,
            initiator_key=self.keys.encryption_key,
            message=message.message,
            auth_data=auth_data,
            dest_node_id=self.remote_node_id,
        )
        outbound_envelope = OutboundEnvelope(packet, self.remote_endpoint)
        return outbound_envelope


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
        enr_db: ENRDatabaseAPI,
        inbound_message_send_channel: trio.abc.SendChannel[AnyInboundMessage],
        outbound_envelope_send_channel: trio.abc.SendChannel[OutboundEnvelope],
        message_type_registry: MessageTypeRegistry = v51_registry,
        events: Optional[EventsAPI] = None,
    ) -> None:
        super().__init__(
            local_private_key=local_private_key,
            local_node_id=local_node_id,
            remote_endpoint=remote_endpoint,
            enr_db=enr_db,
            inbound_message_send_channel=inbound_message_send_channel,
            outbound_envelope_send_channel=outbound_envelope_send_channel,
            message_type_registry=message_type_registry,
            events=events,
        )
        self._remote_node_id = remote_node_id

    @property
    def remote_node_id(self) -> NodeID:
        return self._remote_node_id

    @property
    def remote_enr(self) -> ENRAPI:
        return self._enr_db.get_enr(self.remote_node_id)

    async def handle_outbound_message(self, message: AnyOutboundMessage) -> None:
        self.logger.debug2("%s: handling outbound message: %s", self, message)

        if self.is_after_handshake:
            envelope = self.prepare_envelope(message)
            await self._events.packet_sent.trigger((self, envelope))
            await self._outbound_envelope_send_channel.send(envelope)
            self.logger.debug2("%s: Sent message: %s", self, message)
        elif self.is_during_handshake:
            try:
                self._outbound_message_buffer_send_channel.send_nowait(message)
            except trio.WouldBlock:
                self.logger.debug2(
                    "%s: Discarding message due to full outbound message buffer: %s",
                    self,
                    message,
                )
        elif self.is_before_handshake:
            self.logger.debug2(
                "%s: outbound message triggered handshake initiation: %s",
                self,
                message,
            )
            self._initial_message = message
            self._status = SessionStatus.DURING
            await self._send_handshake_initiation()
        else:
            raise Exception("Invariant: All states handled")

    async def handle_inbound_envelope(self, envelope: InboundEnvelope) -> bool:
        self.logger.debug2("%s: handling inbound envelope: %s", self, envelope)
        await self._events.packet_received.trigger((self, envelope))

        if self.is_after_handshake:
            if envelope.packet.is_message:
                try:
                    message = self.decode_message(
                        cast(Packet[MessagePacket], envelope.packet)
                    )
                except DecryptionError:
                    self.logger.debug(
                        "%s: Discarding undecryptable packet: %s", self, envelope
                    )
                    await self._events.packet_discarded.trigger((self, envelope))
                    return False
                else:
                    await self._inbound_message_send_channel.send(
                        AnyInboundMessage(
                            message=message,
                            sender_endpoint=self.remote_endpoint,
                            sender_node_id=self.remote_node_id,
                        )
                    )
                    return True
            else:
                self.logger.debug("%s: Discarding MessagePacket: %s", self, envelope)
                await self._events.packet_discarded.trigger((self, envelope))
                return False
        elif self.is_during_handshake:
            if envelope.packet.is_who_are_you:
                try:
                    (
                        self._keys,
                        ephemeral_public_key,
                    ) = await self._receive_handshake_response(
                        cast(Packet[WhoAreYouPacket], envelope.packet)
                    )
                except HandshakeFailure as err:
                    self.logger.debug("%s: HandshakeFailure: %s", self, err)
                    return False
                self._status = SessionStatus.AFTER
                self._handshake_complete.set()
                self._last_message_received_at = trio.current_time()
                await self._events.session_handshake_complete.trigger(self)

                await self._send_handshake_completion(
                    self._keys,
                    ephemeral_public_key,
                    cast(Packet[WhoAreYouPacket], envelope.packet),
                )
                await self._process_message_buffers()
                return True
            else:
                self.logger.debug(
                    "%s: Discarding non WhoAreYouPacket: %s", self, envelope
                )
                await self._events.packet_discarded.trigger((self, envelope))
                return False
        elif self.is_before_handshake:
            self.logger.debug("%s: Discarding: %s", self, envelope)
            await self._events.packet_discarded.trigger((self, envelope))
            return False
        else:
            raise Exception("Invariant: All states handled")

    async def _send_handshake_initiation(self) -> None:
        self._initiating_packet = Packet.prepare(
            aes_gcm_nonce=cast(Nonce, secrets.token_bytes(12)),
            initiator_key=cast(AES128Key, secrets.token_bytes(16)),
            message=RandomMessage(),
            auth_data=MessagePacket(source_node_id=self._local_node_id),
            dest_node_id=self.remote_node_id,
        )
        envelope = OutboundEnvelope(
            packet=self._initiating_packet, receiver_endpoint=self.remote_endpoint,
        )
        await self._events.packet_sent.trigger((self, envelope))
        await self._outbound_envelope_send_channel.send(envelope)

    async def _receive_handshake_response(
        self, packet: Packet[WhoAreYouPacket],
    ) -> Tuple[SessionKeys, bytes]:
        self.logger.debug("%s: receiving handshake response", self)

        if packet.header.aes_gcm_nonce != self._initiating_packet.header.aes_gcm_nonce:
            raise HandshakeFailure(
                f"WhoAreYou packet nonce does not match request nonce: "
                f"expected={self._initiating_packet.header.aes_gcm_nonce.hex()}  "
                f"actual={packet.header.aes_gcm_nonce.hex()}"
            )

        # compute session keys
        ephemeral_private_key = keys.PrivateKey(secrets.token_bytes(32))

        remote_enr = self.remote_enr
        session_keys = self.handshake_scheme.compute_session_keys(
            local_private_key=ephemeral_private_key.to_bytes(),
            remote_public_key=remote_enr.public_key,
            local_node_id=self._local_node_id,
            remote_node_id=self.remote_node_id,
            salt=packet.challenge_data,
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

        local_enr = self.local_enr

        # prepare response packet
        signature_inputs = self.handshake_scheme.signature_inputs_cls(
            iv=packet.iv,
            header=packet.header,
            who_are_you=packet.auth_data,
            ephemeral_public_key=ephemeral_public_key,
            recipient_node_id=self.remote_node_id,
        )
        id_nonce_signature = self.handshake_scheme.create_id_nonce_signature(
            signature_inputs=signature_inputs, private_key=self._local_private_key,
        )

        auth_data = HandshakePacket(
            auth_data_head=HandshakeHeader(
                source_node_id=self._local_node_id,
                signature_size=len(id_nonce_signature),
                ephemeral_key_size=len(ephemeral_public_key),
            ),
            id_signature=id_nonce_signature,
            ephemeral_public_key=ephemeral_public_key,
            record=(
                local_enr
                if (
                    packet.auth_data.enr_sequence_number < local_enr.sequence_number
                    or packet.auth_data.enr_sequence_number == 0
                )
                else None
            ),
        )
        handshake_packet = Packet.prepare(
            aes_gcm_nonce=packet.header.aes_gcm_nonce,
            initiator_key=self.keys.encryption_key,
            message=self._initial_message.message,
            auth_data=auth_data,
            dest_node_id=self._remote_node_id,
        )

        envelope = OutboundEnvelope(
            packet=handshake_packet, receiver_endpoint=self.remote_endpoint,
        )
        await self._events.packet_sent.trigger((self, envelope))
        await self._outbound_envelope_send_channel.send(envelope)

    async def _process_message_buffers(self) -> None:
        if not self.is_after_handshake:
            raise Exception("Invalid")

        await self._outbound_message_buffer_send_channel.aclose()
        async with self._outbound_message_buffer_receive_channel:
            async for message in self._outbound_message_buffer_receive_channel:
                self.logger.debug("%s: processing buffered message: %s", self, message)
                await self.handle_outbound_message(message)


class SessionRecipient(BaseSession):
    is_initiator = False

    def __init__(
        self,
        local_private_key: bytes,
        local_node_id: NodeID,
        remote_endpoint: Endpoint,
        enr_db: ENRDatabaseAPI,
        inbound_message_send_channel: trio.abc.SendChannel[AnyInboundMessage],
        outbound_envelope_send_channel: trio.abc.SendChannel[OutboundEnvelope],
        message_type_registry: MessageTypeRegistry = v51_registry,
        events: Optional[EventsAPI] = None,
    ) -> None:
        super().__init__(
            local_private_key=local_private_key,
            local_node_id=local_node_id,
            remote_endpoint=remote_endpoint,
            enr_db=enr_db,
            inbound_message_send_channel=inbound_message_send_channel,
            outbound_envelope_send_channel=outbound_envelope_send_channel,
            message_type_registry=message_type_registry,
            events=events,
        )
        (
            self._inbound_envelope_buffer_send_channel,
            self._inbound_envelope_buffer_receive_channel,
        ) = trio.open_memory_channel[InboundEnvelope](256)

    @property
    def remote_node_id(self) -> NodeID:
        if self.is_before_handshake:
            raise AttributeError("NodeID for remote not yet known")
        return self._remote_node_id

    async def handle_outbound_message(self, message: AnyOutboundMessage) -> None:
        self.logger.debug2("%s: handling outbound message: %s", self, message)

        if self.is_after_handshake:
            envelope = self.prepare_envelope(message)
            await self._events.packet_sent.trigger((self, envelope))
            await self._outbound_envelope_send_channel.send(envelope)
            self.logger.debug2("%s: Sent message: %s", self, message)
        elif self.is_during_handshake:
            try:
                self._outbound_message_buffer_send_channel.send_nowait(message)
            except trio.WouldBlock:
                self.logger.debug(
                    "%s: Discarding message due to full outbound message buffer: %s",
                    self,
                    message,
                )
        elif self.is_before_handshake:
            raise Exception(
                "SessionRecipient cannot send messages prior to handshake initiation"
            )
        else:
            raise Exception("Invariant: All states handled")

    async def handle_inbound_envelope(self, envelope: InboundEnvelope) -> bool:
        self.logger.debug2("%s: handling inbound envelope: %s", self, envelope)
        await self._events.packet_received.trigger((self, envelope))

        if self.is_after_handshake:
            if envelope.packet.is_message:
                try:
                    message = self.decode_message(
                        cast(Packet[MessagePacket], envelope.packet)
                    )
                except DecryptionError:
                    self.logger.debug(
                        "%s: Discarding undecryptable packet: %s", self, envelope
                    )
                    await self._events.packet_discarded.trigger((self, envelope))
                    return False
                else:
                    await self._inbound_message_send_channel.send(
                        AnyInboundMessage(
                            message=message,
                            sender_endpoint=self.remote_endpoint,
                            sender_node_id=self.remote_node_id,
                        )
                    )
                    return True
            else:
                self.logger.debug(
                    "%s: Discarding non Message packet: %s", self, envelope
                )
                await self._events.packet_discarded.trigger((self, envelope))
                return False
        elif self.is_during_handshake:
            if envelope.packet.is_handshake:
                try:
                    self._keys = await self._receive_handshake_completion(
                        cast(Packet[HandshakePacket], envelope.packet),
                    )
                except HandshakeFailure:
                    self.logger.debug(
                        "%s: Discarding invalid Handshake packet: %s",
                        self,
                        envelope,
                        exc_info=True,
                    )
                    await self._events.packet_discarded.trigger((self, envelope))
                    return False
                else:
                    self._status = SessionStatus.AFTER
                    self._handshake_complete.set()
                    await self._events.session_handshake_complete.trigger(self)
                    await self._process_message_buffers()
                    return True
            elif envelope.packet.is_message:
                self.logger.debug2("%s: Buffering Message: %s", self, envelope)
                try:
                    self._inbound_envelope_buffer_send_channel.send_nowait(envelope)
                except trio.WouldBlock:
                    self.logger.debug2(
                        "%s: Discarding inbound envelope due to full buffer: %s",
                        self,
                        envelope,
                    )
                    await self._events.packet_discarded.trigger((self, envelope))
                # We cannot actually know whether this packet will be properly
                # handled.  However, of the available choices, returning
                # `False` here is optimal.
                #
                # If we return `True` and the other side is indeed trying to
                # initiate a new session, we will not end up sending the
                # `WhoAreYouPacket` necessary to go through the handshake
                # process and the initiator will timeout waiting for a
                # response.
                #
                # If we return `False` then the dispatcher will initiate a new
                # session, triggering the sending of a `WhoAreYouPacket`.  If
                # the initiator **is not** trying to initiate a new session,
                # this packet will simply be ignored.  If they are, then they
                # will proceed with the handshake.
                return False
            elif envelope.packet.is_who_are_you:
                self.logger.debug(
                    "%s: Discarding WhoAreYouPacket packet: %s", self, envelope
                )
                await self._events.packet_discarded.trigger((self, envelope))
                return False
            else:
                raise Exception(f"Unrecognized packet: {envelope}")
        elif self.is_before_handshake:
            if envelope.packet.is_message:
                self.logger.debug("%s: received handshake initiation", self)
                self._status = SessionStatus.DURING
                self._remote_node_id = envelope.packet.auth_data.source_node_id  # type: ignore
                await self._send_handshake_response(
                    cast(Packet[MessagePacket], envelope.packet),
                    envelope.sender_endpoint,
                )
                return True
            else:
                self.logger.debug(
                    "%s: Discarding non MessagePacket: %s", self, envelope
                )
                await self._events.packet_discarded.trigger((self, envelope))
                return False
        else:
            raise Exception("Invariant: All states handled")

    async def _send_handshake_response(
        self, packet: Packet[MessagePacket], sender_endpoint: Endpoint
    ) -> None:
        self.logger.debug("%s: sending handshake response", self)
        try:
            remote_enr = self._enr_db.get_enr(packet.auth_data.source_node_id)
        except KeyError:
            enr_sequence_number = 0
        else:
            enr_sequence_number = remote_enr.sequence_number

        auth_data = WhoAreYouPacket(
            id_nonce=cast(IDNonce, secrets.token_bytes(16)),
            enr_sequence_number=enr_sequence_number,
        )

        self.handshake_response_packet = Packet.prepare(
            aes_gcm_nonce=packet.header.aes_gcm_nonce,
            initiator_key=cast(AES128Key, secrets.token_bytes(16)),
            message=EmptyMessage(),
            auth_data=auth_data,
            dest_node_id=packet.auth_data.source_node_id,
        )
        envelope = OutboundEnvelope(
            packet=self.handshake_response_packet, receiver_endpoint=sender_endpoint,
        )
        await self._events.packet_sent.trigger((self, envelope))
        await self._outbound_envelope_send_channel.send(envelope)

    async def _receive_handshake_completion(
        self, packet: Packet[HandshakePacket]
    ) -> SessionKeys:
        self.logger.debug("%s: received handshake completion", self)

        if not isinstance(packet.auth_data, HandshakePacket):
            raise Exception(f"Invalid packet type: {type(packet.auth_data)}")

        if packet.auth_data.record is not None:
            remote_enr = packet.auth_data.record
            try:
                self._enr_db.set_enr(remote_enr)
            except OldSequenceNumber:
                pass
        else:
            remote_enr = self._enr_db.get_enr(self.remote_node_id)

        handshake_scheme = self.handshake_scheme
        signature_inputs = handshake_scheme.signature_inputs_cls(
            iv=self.handshake_response_packet.iv,
            header=self.handshake_response_packet.header,
            who_are_you=self.handshake_response_packet.auth_data,
            ephemeral_public_key=packet.auth_data.ephemeral_public_key,
            recipient_node_id=self._local_node_id,
        )
        # Verify the id_nonce_signature which ensures that the remote node has
        # not lied about their node_id
        try:
            handshake_scheme.validate_id_nonce_signature(
                signature_inputs=signature_inputs,
                signature=packet.auth_data.id_signature,
                public_key=remote_enr.public_key,
            )
        except ValidationError as err:
            raise HandshakeFailure(str(err)) from err

        session_keys = handshake_scheme.compute_session_keys(
            local_private_key=self._local_private_key,
            remote_public_key=packet.auth_data.ephemeral_public_key,
            local_node_id=self._local_node_id,
            remote_node_id=self.remote_node_id,
            salt=self.handshake_response_packet.challenge_data,
            is_locally_initiated=False,
        )

        message = decode_message(
            session_keys.decryption_key,
            packet.header.aes_gcm_nonce,
            packet.message_cipher_text,
            packet.challenge_data,
            self._message_type_registry,
        )

        await self._inbound_message_send_channel.send(
            AnyInboundMessage(
                message=message,
                sender_endpoint=self.remote_endpoint,
                sender_node_id=self.remote_node_id,
            )
        )
        return session_keys

    async def _process_message_buffers(self) -> None:
        if not self.is_after_handshake:
            raise Exception("Invalid")

        await self._inbound_envelope_buffer_send_channel.aclose()
        async with self._inbound_envelope_buffer_receive_channel:
            async for envelope in self._inbound_envelope_buffer_receive_channel:
                self.logger.debug(
                    "%s: processing buffered envelope: %s", self, envelope
                )
                await self.handle_inbound_envelope(envelope)

        await self._outbound_message_buffer_send_channel.aclose()
        async with self._outbound_message_buffer_receive_channel:
            async for message in self._outbound_message_buffer_receive_channel:
                self.logger.debug("%s: processing buffered message: %s", self, message)
                await self.handle_outbound_message(message)
