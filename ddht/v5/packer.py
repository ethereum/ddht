import logging
from typing import Dict, List, NamedTuple, Optional

from async_service import LifecycleError, Service, TrioManager
from eth_enr.abc import ENRAPI, ENRDatabaseAPI
from eth_typing import NodeID
from eth_utils import ValidationError, encode_hex
import trio
from trio.abc import ReceiveChannel, SendChannel

from ddht.abc import MessageTypeRegistryAPI
from ddht.base_message import AnyInboundMessage, AnyOutboundMessage
from ddht.endpoint import Endpoint
from ddht.exceptions import DecryptionError, HandshakeFailure
from ddht.typing import Nonce, SessionKeys
from ddht.v5.abc import HandshakeParticipantAPI
from ddht.v5.channel_services import InboundPacket, OutboundPacket
from ddht.v5.constants import HANDSHAKE_TIMEOUT
from ddht.v5.handshake import HandshakeInitiator, HandshakeRecipient
from ddht.v5.messages import BaseMessage
from ddht.v5.packets import AuthTagPacket, get_random_auth_tag
from ddht.v5.tags import compute_tag, recover_source_id_from_tag


class PeerPacker(Service):
    handshake_participant: Optional[HandshakeParticipantAPI] = None
    session_keys: Optional[SessionKeys] = None

    handshake_successful_event: trio.Event

    def __init__(
        self,
        local_private_key: bytes,
        local_node_id: NodeID,
        remote_node_id: NodeID,
        enr_db: ENRDatabaseAPI,
        message_type_registry: MessageTypeRegistryAPI,
        inbound_packet_receive_channel: ReceiveChannel[InboundPacket],
        inbound_message_send_channel: SendChannel[AnyInboundMessage],
        outbound_message_receive_channel: ReceiveChannel[AnyOutboundMessage],
        outbound_packet_send_channel: SendChannel[OutboundPacket],
    ) -> None:
        self.local_private_key = local_private_key
        self.local_node_id = local_node_id
        self.remote_node_id = remote_node_id
        self.enr_db = enr_db
        self.message_type_registry = message_type_registry

        self.inbound_packet_receive_channel = inbound_packet_receive_channel
        self.inbound_message_send_channel = inbound_message_send_channel
        self.outbound_message_receive_channel = outbound_message_receive_channel
        self.outbound_packet_send_channel = outbound_packet_send_channel

        self.logger = logging.getLogger(
            f"ddht.v5.packer.PeerPacker[{encode_hex(remote_node_id)[2:10]}]"
        )

        self.outbound_message_backlog: List[AnyOutboundMessage] = []

        # This lock ensures that at all times, at most one inbound packet or outbound message is
        # being processed.
        self.handling_lock = trio.Lock()

    def __str__(self) -> str:
        return f"{self.__class__.__name__}[{encode_hex(self.remote_node_id)[2:10]}]"

    async def run(self) -> None:
        async with self.inbound_packet_receive_channel, self.inbound_message_send_channel, self.outbound_message_receive_channel, self.outbound_packet_send_channel:  # noqa: E501
            self.manager.run_daemon_task(self.handle_inbound_packets)
            self.manager.run_daemon_task(self.handle_outbound_messages)
            await self.manager.wait_finished()

    async def handle_inbound_packets(self) -> None:
        async for inbound_packet in self.inbound_packet_receive_channel:
            async with self.handling_lock:
                await self.handle_inbound_packet(inbound_packet)

    async def handle_inbound_packet(self, inbound_packet: InboundPacket) -> None:
        if self.is_pre_handshake:
            await self.handle_inbound_packet_pre_handshake(inbound_packet)
        elif self.is_during_handshake:
            await self.handle_inbound_packet_during_handshake(inbound_packet)
        elif self.is_post_handshake:
            await self.handle_inbound_packet_post_handshake(inbound_packet)
        else:
            raise Exception("Invariant: All states handled")

    async def handle_outbound_messages(self) -> None:
        async for outbound_message in self.outbound_message_receive_channel:
            async with self.handling_lock:
                await self.handle_outbound_message(outbound_message)

    async def handle_outbound_message(
        self, outbound_message: AnyOutboundMessage
    ) -> None:
        if self.is_pre_handshake:
            await self.handle_outbound_message_pre_handshake(outbound_message)
        elif self.is_during_handshake:
            await self.handle_outbound_message_during_handshake(outbound_message)
        elif self.is_post_handshake:
            await self.handle_outbound_message_post_handshake(outbound_message)
        else:
            raise Exception("Invariant: All states handled")

    #
    # Inbound packet handlers
    #
    async def handle_inbound_packet_pre_handshake(
        self, inbound_packet: InboundPacket
    ) -> None:
        if not self.is_pre_handshake:
            raise ValueError("Can only handle packets pre handshake")

        if isinstance(inbound_packet.packet, AuthTagPacket):
            remote_enr: Optional[ENRAPI]

            try:
                remote_enr = self.enr_db.get_enr(self.remote_node_id)
            except KeyError:
                remote_enr = None
            try:
                local_enr = self.enr_db.get_enr(self.local_node_id)
            except KeyError:
                raise ValueError(
                    f"Unable to find local ENR in DB by node id {encode_hex(self.local_node_id)}"
                )

            self.logger.debug("Received %s as handshake initiation", inbound_packet)
            self.start_handshake_as_recipient(
                auth_tag=inbound_packet.packet.auth_tag,
                local_enr=local_enr,
                remote_enr=remote_enr,
            )
            self.logger.debug("Responding with WhoAreYou packet")
            await self.send_first_handshake_packet(inbound_packet.sender_endpoint)
        else:
            self.logger.debug(
                "Dropping %s as handshake has not been started yet", inbound_packet
            )

    async def handle_inbound_packet_during_handshake(
        self, inbound_packet: InboundPacket
    ) -> None:
        if not self.is_during_handshake:
            raise ValueError("Can only handle packets during handshake")
        if self.handshake_participant is None:
            raise TypeError(
                "handshake_participant is None even though handshake is in progress"
            )

        packet = inbound_packet.packet

        if self.handshake_participant.is_response_packet(packet):
            self.logger.debug(
                "Received %s as handshake response", packet.__class__.__name__
            )
        else:
            self.logger.debug(
                "Dropping %s unexpectedly received during handshake", inbound_packet
            )
            return

        try:
            handshake_result = self.handshake_participant.complete_handshake(packet)
        except HandshakeFailure as handshake_failure:
            self.logger.warning(
                "Handshake with %s has failed: %s",
                encode_hex(self.remote_node_id),
                handshake_failure,
            )
            raise  # let the service fail
        else:
            self.logger.info(
                "Handshake with %s was successful", encode_hex(self.remote_node_id)
            )
            self.handshake_successful_event.set()

            # copy message backlog before we reset it
            outbound_message_backlog = tuple(self.outbound_message_backlog)
            self.reset_handshake_state()
            self.session_keys = handshake_result.session_keys
            if not self.is_post_handshake:
                raise Exception(
                    "Invariant: As session_keys are set now, peer packer is in post handshake state"
                )

            if handshake_result.enr is not None:
                self.logger.debug("Updating ENR in DB with %r", handshake_result.enr)
                self.enr_db.set_enr(handshake_result.enr)

            if handshake_result.auth_header_packet is not None:
                outbound_packet = OutboundPacket(
                    handshake_result.auth_header_packet, inbound_packet.sender_endpoint
                )
                self.logger.debug(
                    "Sending %s packet to let peer complete handshake", outbound_packet
                )
                await self.outbound_packet_send_channel.send(outbound_packet)

            if handshake_result.message:
                inbound_message = AnyInboundMessage(
                    handshake_result.message,
                    inbound_packet.sender_endpoint,
                    self.remote_node_id,
                )
                self.logger.debug("Received %s during handshake", inbound_message)
                await self.inbound_message_send_channel.send(inbound_message)

            self.logger.debug(
                "Sending %d messages from backlog", len(outbound_message_backlog)
            )
            for outbound_message in outbound_message_backlog:
                await self.handle_outbound_message(outbound_message)

    async def handle_inbound_packet_post_handshake(
        self, inbound_packet: InboundPacket
    ) -> None:
        if not self.is_post_handshake:
            raise ValueError("Can only handle packets post handshake")
        if self.session_keys is None:
            raise TypeError(
                "session_keys are None even though handshake has been completed"
            )

        if isinstance(inbound_packet.packet, AuthTagPacket):
            try:
                message = inbound_packet.packet.decrypt_message(
                    self.session_keys.decryption_key, self.message_type_registry
                )
            except DecryptionError:
                self.logger.info(
                    "Failed to decrypt message from peer, starting another handshake as recipient"
                )
                self.reset_handshake_state()
                await self.handle_inbound_packet_pre_handshake(inbound_packet)
            except ValidationError as validation_error:
                self.logger.debug("Received invalid packet: %s", validation_error)
                self.manager.cancel()
                return
            else:
                inbound_message = AnyInboundMessage(
                    message, inbound_packet.sender_endpoint, self.remote_node_id
                )
                self.logger.debug("Received %s", inbound_message)
                await self.inbound_message_send_channel.send(inbound_message)
        else:
            self.logger.debug(
                "Dropping %s as handshake has already been completed", inbound_packet
            )

    #
    # Outbound message handlers
    #
    async def handle_outbound_message_pre_handshake(
        self, outbound_message: AnyOutboundMessage
    ) -> None:
        if not self.is_pre_handshake:
            raise ValueError("Can only handle message pre handshake")

        try:
            local_enr = self.enr_db.get_enr(self.local_node_id)
        except KeyError:
            raise ValueError(
                f"Unable to find local ENR in DB by node id {encode_hex(self.local_node_id)}"
            )
        try:
            remote_enr = self.enr_db.get_enr(self.remote_node_id)
        except KeyError:
            self.logger.warning(
                "Unable to initiate handshake with %s as their ENR is not present in the DB",
                encode_hex(self.remote_node_id),
            )
            raise HandshakeFailure()

        self.logger.info("Initiating handshake to send %s", outbound_message)
        self.start_handshake_as_initiator(
            local_enr=local_enr, remote_enr=remote_enr, message=outbound_message.message
        )
        self.logger.debug("Sending initiating packet")
        await self.send_first_handshake_packet(outbound_message.receiver_endpoint)

    async def handle_outbound_message_during_handshake(
        self, outbound_message: AnyOutboundMessage
    ) -> None:
        if not self.is_during_handshake:
            raise ValueError("Can only handle message during handshake")

        self.logger.debug(
            "Putting %s on message backlog as handshake is in progress already",
            outbound_message,
        )
        self.outbound_message_backlog.append(outbound_message)
        await trio.sleep(0)

    async def handle_outbound_message_post_handshake(
        self, outbound_message: AnyOutboundMessage
    ) -> None:
        if not self.is_post_handshake:
            raise ValueError("Can only handle message post handshake")
        if self.session_keys is None:
            raise TypeError(
                "session_keys are None even though handshake has been completed"
            )

        packet = AuthTagPacket.prepare(
            tag=compute_tag(self.local_node_id, self.remote_node_id),
            auth_tag=get_random_auth_tag(),
            message=outbound_message.message,
            key=self.session_keys.encryption_key,
        )
        outbound_packet = OutboundPacket(packet, outbound_message.receiver_endpoint)
        self.logger.debug("Sending %s", outbound_message)
        await self.outbound_packet_send_channel.send(outbound_packet)

    #
    # Start Handshake Methods
    #
    def start_handshake_as_initiator(
        self, local_enr: ENRAPI, remote_enr: ENRAPI, message: BaseMessage
    ) -> None:
        if not self.is_pre_handshake:
            raise ValueError("Can only register handshake when its not started yet")

        self.handshake_participant = HandshakeInitiator(
            local_private_key=self.local_private_key,
            local_enr=local_enr,
            remote_enr=remote_enr,
            initial_message=message,
        )
        self.handshake_successful_event = trio.Event()
        self.manager.run_task(
            self.check_handshake_timeout, self.handshake_successful_event
        )

        if not self.is_during_handshake:
            raise Exception(
                "Invariant: After a handshake is started, the handshake is in progress"
            )

    def start_handshake_as_recipient(
        self, auth_tag: Nonce, local_enr: ENRAPI, remote_enr: Optional[ENRAPI]
    ) -> None:
        if not self.is_pre_handshake:
            raise ValueError("Can only register handshake when its not started yet")

        self.handshake_participant = HandshakeRecipient(
            local_private_key=self.local_private_key,
            local_enr=local_enr,
            remote_node_id=self.remote_node_id,
            remote_enr=remote_enr,
            initiating_packet_auth_tag=auth_tag,
        )
        self.handshake_successful_event = trio.Event()
        self.manager.run_task(
            self.check_handshake_timeout, self.handshake_successful_event
        )

        if not self.is_during_handshake:
            raise Exception(
                "Invariant: After a handshake is started, the handshake is in progress"
            )

    async def check_handshake_timeout(
        self, handshake_successful_event: trio.Event
    ) -> None:
        try:
            with trio.fail_after(HANDSHAKE_TIMEOUT):
                # Only the timeout for successful handshakes has to be checked as a failure during
                # handshake will make the service as a whole fail.
                await handshake_successful_event.wait()
        except trio.TooSlowError:
            self.logger.warning(
                "Handshake with %s has timed out", encode_hex(self.remote_node_id)
            )
            self.manager.cancel()

    #
    # Handshake states
    #
    @property
    def is_pre_handshake(self) -> bool:
        """True if neither session keys are available nor a handshake is in progress."""
        return self.handshake_participant is None and self.session_keys is None

    @property
    def is_during_handshake(self) -> bool:
        """True if a handshake is in progress, but not completed yet."""
        return self.handshake_participant is not None

    @property
    def is_post_handshake(self) -> bool:
        """True if session keys from a preceding handshake are available."""
        return self.handshake_participant is None and self.session_keys is not None

    def reset_handshake_state(self) -> None:
        """
        Return to the pre handshake state.

        This deletes the session keys, the handshake participant instance, and all messages on the
        message backlog. After this method is called, a new handshake can be initiated.
        """
        if self.is_pre_handshake:
            raise ValueError("Handshake is already in pre state")
        self.handshake_participant = None
        self.session_keys = None
        self.outbound_message_backlog.clear()
        self.handshake_finished_event = None

    def is_expecting_handshake_packet(self, inbound_packet: InboundPacket) -> bool:
        """
        Check if the peer packer is waiting for the given packet to complete a handshake.

        This should be called before putting the packet in question on the peer's inbound packet
        channel.
        """
        return (
            self.is_during_handshake
            and self.handshake_participant is not None
            and self.handshake_participant.is_response_packet(inbound_packet.packet)
        )

    async def send_first_handshake_packet(self, receiver_endpoint: Endpoint) -> None:
        if self.handshake_participant is None:
            raise Exception("Invariant: this code path should not occur")
        outbound_packet = OutboundPacket(
            self.handshake_participant.first_packet_to_send, receiver_endpoint
        )
        await self.outbound_packet_send_channel.send(outbound_packet)


class ManagedPeerPacker(NamedTuple):
    peer_packer: PeerPacker
    manager: TrioManager
    inbound_packet_send_channel: SendChannel[InboundPacket]
    outbound_message_send_channel: SendChannel[AnyOutboundMessage]


class Packer(Service):
    def __init__(
        self,
        local_private_key: bytes,
        local_node_id: NodeID,
        enr_db: ENRDatabaseAPI,
        message_type_registry: MessageTypeRegistryAPI,
        inbound_packet_receive_channel: ReceiveChannel[InboundPacket],
        inbound_message_send_channel: SendChannel[AnyInboundMessage],
        outbound_message_receive_channel: ReceiveChannel[AnyOutboundMessage],
        outbound_packet_send_channel: SendChannel[OutboundPacket],
    ) -> None:
        self.local_private_key = local_private_key
        self.local_node_id = local_node_id
        self.enr_db = enr_db
        self.message_type_registry = message_type_registry

        self.inbound_packet_receive_channel = inbound_packet_receive_channel
        self.inbound_message_send_channel = inbound_message_send_channel
        self.outbound_message_receive_channel = outbound_message_receive_channel
        self.outbound_packet_send_channel = outbound_packet_send_channel

        self.logger = logging.getLogger("ddht.v5.packer.Packer")

        self.managed_peer_packers: Dict[NodeID, ManagedPeerPacker] = {}

    async def run(self) -> None:
        self.manager.run_daemon_task(self.handle_inbound_packets)
        self.manager.run_daemon_task(self.handle_outbound_messages)
        await self.manager.wait_finished()

    async def handle_inbound_packets(self) -> None:
        async for inbound_packet in self.inbound_packet_receive_channel:
            expecting_managed_peer_packers = tuple(
                managed_peer_packer
                for managed_peer_packer in self.managed_peer_packers.values()
                if managed_peer_packer.peer_packer.is_expecting_handshake_packet(
                    inbound_packet
                )
            )
            if len(expecting_managed_peer_packers) >= 2:
                self.logger.warning(
                    "Multiple peer packers are expecting %s: %s",
                    inbound_packet,
                    ", ".join(
                        encode_hex(managed_peer_packer.peer_packer.local_node_id)
                        for managed_peer_packer in expecting_managed_peer_packers
                    ),
                )

            if expecting_managed_peer_packers:
                for managed_peer_packer in expecting_managed_peer_packers:
                    self.logger.debug(
                        "Passing %s to %s for handshake",
                        inbound_packet,
                        managed_peer_packer.peer_packer,
                    )
                    try:
                        await managed_peer_packer.inbound_packet_send_channel.send(
                            inbound_packet
                        )
                    except trio.BrokenResourceError:
                        self.logger.warning(
                            "Dropping packet as channel to %s is closed"
                            % managed_peer_packer.peer_packer
                        )

            elif isinstance(inbound_packet.packet, AuthTagPacket):
                tag = inbound_packet.packet.tag
                remote_node_id = recover_source_id_from_tag(tag, self.local_node_id)

                if not self.is_peer_packer_registered(remote_node_id):
                    self.logger.info(
                        "Launching peer packer for %s to handle %s",
                        encode_hex(remote_node_id),
                        inbound_packet,
                    )
                    self.register_peer_packer(remote_node_id)
                    self.manager.run_task(self.run_peer_packer, remote_node_id)

                managed_peer_packer = self.managed_peer_packers[remote_node_id]
                self.logger.debug(
                    "Passing %s from %s to responsible peer packer",
                    inbound_packet,
                    encode_hex(remote_node_id),
                )
                try:
                    await managed_peer_packer.inbound_packet_send_channel.send(
                        inbound_packet
                    )
                except trio.BrokenResourceError:
                    self.logger.warning(
                        "Dropping packet as channel to %s is closed"
                        % managed_peer_packer.peer_packer
                    )

            else:
                self.logger.warning(
                    "Dropping unprompted handshake packet %s", inbound_packet
                )

    async def handle_outbound_messages(self) -> None:
        async for outbound_message in self.outbound_message_receive_channel:
            remote_node_id = outbound_message.receiver_node_id
            if not self.is_peer_packer_registered(remote_node_id):
                self.logger.info(
                    "Launching peer packer for %s to handle %s",
                    encode_hex(remote_node_id),
                    outbound_message,
                )
                self.register_peer_packer(remote_node_id)
                self.manager.run_task(self.run_peer_packer, remote_node_id)

            self.logger.debug(
                "Passing %s from %s to responsible peer packer",
                outbound_message,
                encode_hex(remote_node_id),
            )
            managed_peer_packer = self.managed_peer_packers[remote_node_id]
            try:
                await managed_peer_packer.outbound_message_send_channel.send(
                    outbound_message
                )
            except trio.BrokenResourceError:
                if not managed_peer_packer.manager.is_cancelled:
                    raise

    #
    # Peer packer handling
    #
    def is_peer_packer_registered(self, remote_node_id: NodeID) -> bool:
        return remote_node_id in self.managed_peer_packers

    def register_peer_packer(self, remote_node_id: NodeID) -> None:
        if self.is_peer_packer_registered(remote_node_id):
            raise ValueError(
                f"Peer packer for {encode_hex(remote_node_id)} is already registered"
            )

        inbound_packet_channels = trio.open_memory_channel[InboundPacket](0)
        outbound_message_channels = trio.open_memory_channel[AnyOutboundMessage](0)

        peer_packer = PeerPacker(
            local_private_key=self.local_private_key,
            local_node_id=self.local_node_id,
            remote_node_id=remote_node_id,
            enr_db=self.enr_db,
            message_type_registry=self.message_type_registry,
            inbound_packet_receive_channel=inbound_packet_channels[1],
            # These channels are the standard `trio.abc.XXXChannel` interfaces.
            # The `clone` method is only available on `MemoryXXXChannel` types
            # which trio currently doesn't expose in a way that allows us to
            # type these channels as those types.  Thus, we need to tell mypy
            # to ignore this since it doesn't recognize the standard
            # `trio.abc.XXXChannel` interfaces as having a `clone()` method.
            inbound_message_send_channel=self.inbound_message_send_channel.clone(),  # type: ignore  # noqa: E501
            outbound_message_receive_channel=outbound_message_channels[1],
            outbound_packet_send_channel=self.outbound_packet_send_channel.clone(),  # type: ignore
        )

        manager = TrioManager(peer_packer)

        self.managed_peer_packers[remote_node_id] = ManagedPeerPacker(
            peer_packer=peer_packer,
            manager=manager,
            inbound_packet_send_channel=inbound_packet_channels[0],
            outbound_message_send_channel=outbound_message_channels[0],
        )

    def deregister_peer_packer(self, remote_node_id: NodeID) -> None:
        if not self.is_peer_packer_registered(remote_node_id):
            raise ValueError(
                f"Peer packer for {encode_hex(remote_node_id)} is not registered"
            )
        managed_peer_packer = self.managed_peer_packers[remote_node_id]
        if managed_peer_packer.manager.is_running:
            raise ValueError(
                f"Peer packer for {encode_hex(remote_node_id)} is still running"
            )

        self.managed_peer_packers.pop(remote_node_id)

    async def run_peer_packer(self, remote_node_id: NodeID) -> None:
        if not self.is_peer_packer_registered(remote_node_id):
            raise ValueError(
                "Peer packer for {encode_hex(remote_node_id)} is not registered"
            )
        managed_peer_packer = self.managed_peer_packers[remote_node_id]

        try:
            await managed_peer_packer.manager.run()
        except LifecycleError as lifecycle_error:
            raise ValueError(
                "Peer packer for {encode_hex(remote_node_id)} has already been started"
            ) from lifecycle_error
        except HandshakeFailure:
            # peer packer has logged a warning already
            self.logger.debug(
                "Peer packer %s has failed to do handshake with %s",
                managed_peer_packer.peer_packer,
                encode_hex(remote_node_id),
            )
        finally:
            self.logger.info(
                "Deregistering peer packer %s", managed_peer_packer.peer_packer
            )
            self.deregister_peer_packer(remote_node_id)
