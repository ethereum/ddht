import secrets
from typing import AsyncIterator, List, Optional

from async_generator import asynccontextmanager
import trio

from ddht.base_message import AnyInboundMessage, AnyOutboundMessage, BaseMessage
from ddht.tools.driver._utils import no_hang
from ddht.tools.driver.abc import (
    EnvelopePair,
    NodeAPI,
    SessionDriverAPI,
    SessionPairAPI,
)
from ddht.tools.factories.v5_1 import SessionChannels
from ddht.v5_1.abc import EventsAPI, SessionAPI
from ddht.v5_1.envelope import InboundEnvelope
from ddht.v5_1.messages import PingMessage, PongMessage
from ddht.v5_1.packets import AnyPacket


class SessionDriver(SessionDriverAPI):
    def __init__(
        self,
        node: NodeAPI,
        remote: NodeAPI,
        session: SessionAPI,
        channels: SessionChannels,
    ) -> None:
        self.node = node
        self.remote = remote
        self.session = session
        self.channels = channels

    @property
    def events(self) -> EventsAPI:
        return self.node.events

    @no_hang
    async def send_message(self, message: BaseMessage) -> None:
        await self.session.handle_outbound_message(
            AnyOutboundMessage(
                message=message,
                receiver_endpoint=self.remote.endpoint,
                receiver_node_id=self.remote.node_id,
            )
        )

    @no_hang
    async def next_message(self) -> AnyInboundMessage:
        return await self.channels.inbound_message_receive_channel.receive()

    @no_hang
    async def send_ping(self, request_id: Optional[int] = None) -> PingMessage:
        if request_id is None:
            request_id = secrets.randbits(32)
        message = PingMessage(request_id, self.node.enr.sequence_number)
        await self.send_message(message)
        return message

    @no_hang
    async def send_pong(self, request_id: Optional[int] = None) -> PongMessage:
        if request_id is None:
            request_id = secrets.randbits(32)
        message = PongMessage(
            request_id,
            self.node.enr.sequence_number,
            self.node.endpoint.ip_address,
            self.node.endpoint.port,
        )
        await self.send_message(message)
        return message


class SessionPair(SessionPairAPI):
    def __init__(
        self,
        initiator: NodeAPI,
        initiator_session: SessionAPI,
        initiator_channels: SessionChannels,
        recipient: NodeAPI,
        recipient_session: SessionAPI,
        recipient_channels: SessionChannels,
    ) -> None:
        self.initiator = SessionDriver(
            initiator, recipient, initiator_session, initiator_channels
        )
        self.recipient = SessionDriver(
            recipient, initiator, recipient_session, recipient_channels
        )

    @no_hang
    async def transmit_one(self, source: SessionDriverAPI) -> EnvelopePair:
        if source.session.id == self.initiator.session.id:
            target = self.recipient
        elif source.session.id == self.recipient.session.id:
            target = self.initiator
        else:
            raise Exception(
                f"Unrecognized source: {source}: Must be one of "
                f"{self.initiator} or {self.recipient}"
            )

        outbound_envelope = (
            await source.channels.outbound_envelope_receive_channel.receive()
        )
        inbound_envelope = InboundEnvelope(
            outbound_envelope.packet, sender_endpoint=source.node.endpoint,
        )
        await target.session.handle_inbound_envelope(inbound_envelope)
        return EnvelopePair(outbound_envelope, inbound_envelope)

    @no_hang
    async def _transmit_all(self, source: SessionDriverAPI) -> None:
        transmitted: List[EnvelopePair] = []

        while True:
            envelope_pair = await self.transmit_one(source)
            transmitted.append(envelope_pair)

        return tuple(transmitted)

    @asynccontextmanager
    async def transmit(self) -> AsyncIterator[None]:
        async with trio.open_nursery() as nursery:
            nursery.start_soon(self._transmit_all, self.initiator)
            nursery.start_soon(self._transmit_all, self.recipient)

            try:
                yield
            finally:
                nursery.cancel_scope.cancel()

    @no_hang
    async def send_packet(self, packet: AnyPacket) -> None:
        if packet.header.source_node_id == self.initiator.node.node_id:
            await self.recipient.session.handle_inbound_envelope(
                InboundEnvelope(
                    packet=packet, sender_endpoint=self.initiator.node.endpoint,
                )
            )
        elif packet.header.source_node_id == self.recipient.node.node_id:
            await self.initiator.session.handle_inbound_envelope(
                InboundEnvelope(
                    packet=packet, sender_endpoint=self.recipient.node.endpoint,
                )
            )
        else:
            raise Exception(
                f"No matching node-id: {packet.header.source_node_id.hex()}"
            )

    @no_hang
    async def handshake(self) -> None:
        if (
            not self.initiator.session.is_before_handshake
            or not self.recipient.session.is_before_handshake
        ):  # noqa: E501
            raise Exception("Invalid state.")

        async with self.transmit():
            async with self.initiator.events.session_handshake_complete.subscribe_and_wait():  # noqa: E501
                async with self.recipient.events.session_handshake_complete.subscribe_and_wait():  # noqa: E501
                    await self.initiator.send_ping()
            # consume the ping message sent during handshake
            await self.recipient.next_message()
