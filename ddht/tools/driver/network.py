import functools
from typing import Any, Callable, TypeVar, cast

from eth_keys import keys
import trio

from ddht.abc import NodeDBAPI
from ddht.base_message import BaseMessage, IncomingMessage, OutgoingMessage
from ddht.constants import IP_V4_ADDRESS_ENR_KEY, UDP_PORT_ENR_KEY
from ddht.endpoint import Endpoint
from ddht.tools.driver.abc import (
    EnvelopePair,
    NetworkAPI,
    NodeAPI,
    SessionChannels,
    SessionDriverAPI,
    SessionPairAPI,
)
from ddht.tools.factories.discovery import EndpointFactory, ENRFactory
from ddht.tools.factories.keys import PrivateKeyFactory
from ddht.tools.factories.node_db import NodeDBFactory
from ddht.typing import NodeID
from ddht.v5_1.abc import SessionAPI
from ddht.v5_1.envelope import IncomingEnvelope
from ddht.v5_1.session import SessionInitiator, SessionRecipient


class Node(NodeAPI):
    def __init__(
        self, private_key: keys.PrivateKey, endpoint: Endpoint, node_db: NodeDBAPI
    ) -> None:
        self.private_key = private_key
        self.node_db = node_db
        self.enr = ENRFactory(
            private_key=private_key.to_bytes(),
            address__ip=endpoint.ip_address,
            address__udp_port=endpoint.port,
        )
        self.node_db.set_enr(self.enr)

    @property
    def endpoint(self) -> Endpoint:
        return Endpoint(self.enr[IP_V4_ADDRESS_ENR_KEY], self.enr[UDP_PORT_ENR_KEY],)

    @property
    def node_id(self) -> NodeID:
        return self.enr.node_id


HANG_TIMEOUT = 10


TCallable = TypeVar("TCallable", bound=Callable[..., Any])


def no_hang(fn: TCallable) -> TCallable:
    @functools.wraps(fn)
    async def wrapper(*args: Any, **kwargs: Any) -> Any:
        with trio.fail_after(HANG_TIMEOUT):
            return await fn(*args, **kwargs)

    return cast(TCallable, wrapper)


class SessionDriver(SessionDriverAPI):
    session: SessionAPI
    channels: SessionChannels

    def __init__(self, session: SessionAPI, channels: SessionChannels) -> None:
        self.session = session
        self.channels = channels

    @no_hang
    async def send_message(self, message: BaseMessage) -> None:
        await self.session.handle_outgoing_message(
            OutgoingMessage(
                message=message,
                receiver_endpoint=self.session.remote_endpoint,
                receiver_node_id=self.session.remote_node_id,
            )
        )

    @no_hang
    async def next_message(self) -> IncomingMessage:
        return await self.channels.incoming_message_receive_channel.receive()


class SessionPair(SessionPairAPI):
    def __init__(
        self,
        initiator: SessionAPI,
        initiator_channels: SessionChannels,
        recipient: SessionAPI,
        recipient_channels: SessionChannels,
    ) -> None:
        self.initiator = SessionDriver(initiator, initiator_channels)
        self.recipient = SessionDriver(recipient, recipient_channels)

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

        outgoing_envelope = (
            await source.channels.outgoing_envelope_receive_channel.receive()
        )
        incoming_envelope = IncomingEnvelope(
            outgoing_envelope.packet, sender_endpoint=source.session.remote_endpoint,
        )
        await target.session.handle_incoming_envelope(incoming_envelope)
        return EnvelopePair(outgoing_envelope, incoming_envelope)


class Network(NetworkAPI):
    def node(self) -> Node:
        return Node(
            private_key=PrivateKeyFactory(),
            endpoint=EndpointFactory(),
            node_db=NodeDBFactory(),
        )

    def session_pair(self, initiator: NodeAPI, recipient: NodeAPI) -> SessionPair:
        initiator_channels = SessionChannels.init()
        initiator_session = SessionInitiator(
            local_private_key=initiator.private_key.to_bytes(),
            local_node_id=initiator.node_id,
            remote_endpoint=recipient.endpoint,
            remote_node_id=recipient.node_id,
            node_db=initiator.node_db,
            incoming_message_send_channel=initiator_channels.incoming_message_send_channel,
            outgoing_envelope_send_channel=initiator_channels.outgoing_envelope_send_channel,
        )
        # the initiator always needs to have the remote enr present in their database
        initiator.node_db.set_enr(recipient.enr)

        recipient_channels = SessionChannels.init()
        recipient_session = SessionRecipient(
            local_private_key=recipient.private_key.to_bytes(),
            local_node_id=recipient.node_id,
            remote_endpoint=initiator.endpoint,
            node_db=recipient.node_db,
            incoming_message_send_channel=recipient_channels.incoming_message_send_channel,
            outgoing_envelope_send_channel=recipient_channels.outgoing_envelope_send_channel,
        )
        return SessionPair(
            initiator=initiator_session,
            initiator_channels=initiator_channels,
            recipient=recipient_session,
            recipient_channels=recipient_channels,
        )
