from contextlib import AsyncExitStack
import functools
import logging
import secrets
from typing import (
    Any,
    AsyncIterator,
    Callable,
    Dict,
    List,
    Optional,
    Set,
    Tuple,
    TypeVar,
    cast,
)

from async_generator import asynccontextmanager
from async_service import background_trio_service
from eth_keys import keys
from eth_utils import humanize_hash
import trio

from ddht.abc import NodeDBAPI
from ddht.base_message import AnyInboundMessage, AnyOutboundMessage, BaseMessage
from ddht.constants import IP_V4_ADDRESS_ENR_KEY, UDP_PORT_ENR_KEY
from ddht.endpoint import Endpoint
from ddht.tools.driver.abc import (
    EnvelopePair,
    NetworkAPI,
    NodeAPI,
    SessionDriverAPI,
    SessionPairAPI,
)
from ddht.tools.factories.endpoint import EndpointFactory
from ddht.tools.factories.enr import ENRFactory
from ddht.tools.factories.keys import PrivateKeyFactory
from ddht.tools.factories.node_db import NodeDBFactory
from ddht.tools.factories.v5_1 import SessionChannels
from ddht.typing import NodeID
from ddht.v5_1.abc import ClientAPI, DispatcherAPI, EventsAPI, SessionAPI
from ddht.v5_1.client import Client
from ddht.v5_1.dispatcher import Dispatcher
from ddht.v5_1.envelope import InboundEnvelope
from ddht.v5_1.events import Events
from ddht.v5_1.messages import PingMessage, PongMessage
from ddht.v5_1.packets import AnyPacket
from ddht.v5_1.pool import Pool


class Node(NodeAPI):
    def __init__(
        self,
        private_key: keys.PrivateKey,
        endpoint: Endpoint,
        node_db: NodeDBAPI,
        events: Optional[EventsAPI] = None,
    ) -> None:
        self.node_db = node_db
        self.enr = ENRFactory(
            private_key=private_key.to_bytes(),
            address__ip=endpoint.ip_address,
            address__udp_port=endpoint.port,
        )
        self.node_db.set_enr(self.enr)
        if events is None:
            events = Events()
        self.events = events
        self.channels = SessionChannels.init()
        self.pool = Pool(
            local_private_key=private_key,
            local_node_id=self.enr.node_id,
            node_db=self.node_db,
            outbound_envelope_send_channel=self.channels.outbound_envelope_send_channel,
            inbound_message_send_channel=self.channels.inbound_message_send_channel,
            events=self.events,
        )

    def __str__(self) -> str:
        return f"{humanize_hash(self.node_id)}@{self.endpoint}"  # type: ignore

    @property
    def private_key(self) -> keys.PrivateKey:
        return self.pool.local_private_key

    @property
    def endpoint(self) -> Endpoint:
        return Endpoint(self.enr[IP_V4_ADDRESS_ENR_KEY], self.enr[UDP_PORT_ENR_KEY],)

    @property
    def node_id(self) -> NodeID:
        return self.enr.node_id

    @asynccontextmanager
    async def client(self) -> AsyncIterator[ClientAPI]:
        client = Client(
            local_private_key=self.private_key,
            listen_on=self.endpoint,
            node_db=self.node_db,
            events=self.events,
        )
        async with background_trio_service(client):
            await client.wait_listening()
            yield client


HANG_TIMEOUT = 10


TCallable = TypeVar("TCallable", bound=Callable[..., Any])


def no_hang(fn: TCallable) -> TCallable:
    @functools.wraps(fn)
    async def wrapper(*args: Any, **kwargs: Any) -> Any:
        with trio.fail_after(HANG_TIMEOUT):
            return await fn(*args, **kwargs)

    return cast(TCallable, wrapper)


class SessionDriver(SessionDriverAPI):
    def __init__(self, node: NodeAPI, remote: NodeAPI, session: SessionAPI) -> None:
        self.node = node
        self.remote = remote
        self.session = session

    @property
    def events(self) -> EventsAPI:
        return self.node.events

    @no_hang
    async def send_message(self, message: BaseMessage) -> None:
        self.session.logger.info("SENDING: %s", message)
        await self.session.handle_outbound_message(
            AnyOutboundMessage(
                message=message,
                receiver_endpoint=self.remote.endpoint,
                receiver_node_id=self.remote.node_id,
            )
        )

    @no_hang
    async def next_message(self) -> AnyInboundMessage:
        return await self.node.channels.inbound_message_receive_channel.receive()

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
        recipient: NodeAPI,
        recipient_session: SessionAPI,
    ) -> None:
        self.initiator = SessionDriver(initiator, recipient, initiator_session)
        self.recipient = SessionDriver(recipient, initiator, recipient_session)

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
            await source.node.channels.outbound_envelope_receive_channel.receive()
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


async def _staple(
    sender: NodeAPI,
    inbound_envelope_send_channel: trio.abc.SendChannel[InboundEnvelope],
) -> None:
    async for outbound_envelope in sender.channels.outbound_envelope_receive_channel:
        inbound_envelope = InboundEnvelope(outbound_envelope.packet, sender.endpoint)
        await inbound_envelope_send_channel.send(inbound_envelope)


@asynccontextmanager
async def staple(
    sender: NodeAPI,
    inbound_envelope_send_channel: trio.abc.SendChannel[InboundEnvelope],
) -> AsyncIterator[None]:
    async with trio.open_nursery() as nursery:
        nursery.start_soon(_staple, sender, inbound_envelope_send_channel)
        try:
            yield
        finally:
            nursery.cancel_scope.cancel()


class Network(NetworkAPI):
    logger = logging.getLogger("ddht.driver.Network")

    _inbound_envelope_send_channels: Dict[
        NodeID, Tuple[DispatcherAPI, trio.abc.SendChannel[InboundEnvelope]]
    ]
    _running_dispatchers: Set[Tuple[NodeID, NodeID]]

    def __init__(self) -> None:
        self._inbound_envelope_send_channels = {}
        self._running_dispatchers = set()

    def node(
        self,
        private_key: Optional[keys.PrivateKey] = None,
        endpoint: Optional[Endpoint] = None,
        node_db: Optional[NodeDBAPI] = None,
        events: Optional[EventsAPI] = None,
    ) -> Node:
        if private_key is None:
            private_key = PrivateKeyFactory()
        if endpoint is None:
            endpoint = EndpointFactory.localhost()
        if node_db is None:
            node_db = NodeDBFactory()
        return Node(
            private_key=private_key, endpoint=endpoint, node_db=node_db, events=events
        )

    def session_pair(
        self,
        initiator: Optional[NodeAPI] = None,
        recipient: Optional[NodeAPI] = None,
        initiator_session: Optional[SessionAPI] = None,
        recipient_session: Optional[SessionAPI] = None,
    ) -> SessionPair:
        if initiator is None:
            initiator = self.node()
        if recipient is None:
            recipient = self.node()

        initiator_session = initiator.pool.initiate_session(
            recipient.endpoint, recipient.node_id
        )
        # the initiator always needs to have the remote enr present in their database
        initiator.node_db.set_enr(recipient.enr)

        recipient_session = recipient.pool.receive_session(initiator.endpoint)

        return SessionPair(
            initiator=initiator,
            initiator_session=initiator_session,
            recipient=recipient,
            recipient_session=recipient_session,
        )

    @asynccontextmanager
    async def dispatcher_pair(
        self, node_a: NodeAPI, node_b: NodeAPI
    ) -> AsyncIterator[Tuple[DispatcherAPI, DispatcherAPI]]:
        if node_a.node_id < node_b.node_id:
            left = node_a
            right = node_b
        elif node_b.node_id < node_a.node_id:
            left = node_b
            right = node_a
        else:
            raise Exception("Cannot pair with self")

        self.logger.debug("setting up dispatcher pair: %s <> %s", left, right)

        async with AsyncExitStack() as stack:
            if left.node_id in self._inbound_envelope_send_channels:
                self.logger.debug("dispatcher already present for %s", left)
                (
                    left_dispatcher,
                    left_inbound_envelope_send_channel,
                ) = self._inbound_envelope_send_channels[left.node_id]
            else:
                self.logger.debug("setting up new dispatcher for %s", left)
                (
                    left_inbound_envelope_send_channel,
                    left_inbound_envelope_receive_channel,
                ) = trio.open_memory_channel[InboundEnvelope](256)
                left_dispatcher = Dispatcher(
                    left_inbound_envelope_receive_channel,
                    left.channels.inbound_message_receive_channel,
                    left.pool,
                    left.node_db,
                    events=left.events,
                )
                self._inbound_envelope_send_channels[left.node_id] = (
                    left_dispatcher,
                    left_inbound_envelope_send_channel,
                )
                await stack.enter_async_context(
                    background_trio_service(left_dispatcher)
                )

            if right.node_id in self._inbound_envelope_send_channels:
                self.logger.debug("dispatcher already present for %s", right)
                (
                    right_dispatcher,
                    right_inbound_envelope_send_channel,
                ) = self._inbound_envelope_send_channels[right.node_id]
            else:
                self.logger.debug("setting up new dispatcher for %s", right)
                (
                    right_inbound_envelope_send_channel,
                    right_inbound_envelope_receive_channel,
                ) = trio.open_memory_channel[InboundEnvelope](256)
                right_dispatcher = Dispatcher(
                    right_inbound_envelope_receive_channel,
                    right.channels.inbound_message_receive_channel,
                    right.pool,
                    right.node_db,
                    events=right.events,
                )
                self._inbound_envelope_send_channels[right.node_id] = (
                    right_dispatcher,
                    right_inbound_envelope_send_channel,
                )
                await stack.enter_async_context(
                    background_trio_service(right_dispatcher)
                )

            if left is node_a:
                dispatchers = (left_dispatcher, right_dispatcher)
            elif left is node_b:
                dispatchers = (right_dispatcher, left_dispatcher)
            else:
                raise Exception("Invariant")

            async with staple(left, right_inbound_envelope_send_channel.clone()):  # type: ignore
                async with staple(right, left_inbound_envelope_send_channel.clone()):  # type: ignore  # noqa: E501
                    self._running_dispatchers.add((left.node_id, right.node_id))
                    try:
                        yield dispatchers
                    finally:
                        self._running_dispatchers.remove((left.node_id, right.node_id))
