from contextlib import AsyncExitStack
import logging
from typing import AsyncIterator, Dict, NamedTuple, Optional, Set, Tuple

from async_generator import asynccontextmanager
from async_service import background_trio_service
from eth_enr import ENRDB, ENRDatabaseAPI
from eth_keys import keys
from eth_typing import NodeID
import trio

from ddht._utils import humanize_node_id
from ddht.endpoint import Endpoint
from ddht.tools.driver.abc import NodeAPI, SessionPairAPI, TesterAPI
from ddht.tools.driver.node import Node
from ddht.tools.driver.session import SessionPair
from ddht.tools.factories.endpoint import EndpointFactory
from ddht.tools.factories.keys import PrivateKeyFactory
from ddht.tools.factories.v5_1 import SessionChannels
from ddht.v5_1.abc import DispatcherAPI, EventsAPI, PoolAPI, SessionAPI
from ddht.v5_1.dispatcher import Dispatcher
from ddht.v5_1.envelope import InboundEnvelope, OutboundEnvelope
from ddht.v5_1.pool import Pool

logger = logging.getLogger("ddht.testing.staple")


async def _staple(
    sender: NodeAPI,
    outbound_envelope_receive_channel: trio.abc.ReceiveChannel[OutboundEnvelope],
    inbound_envelope_send_channel: trio.abc.SendChannel[InboundEnvelope],
) -> None:
    logger.info("Stapling: %s", sender)
    async for outbound_envelope in outbound_envelope_receive_channel:
        logger.info("Relaying envelope: %s", outbound_envelope)
        inbound_envelope = InboundEnvelope(outbound_envelope.packet, sender.endpoint)
        await inbound_envelope_send_channel.send(inbound_envelope)


@asynccontextmanager
async def staple(
    sender: NodeAPI,
    outbound_envelope_receive_channel: trio.abc.ReceiveChannel[OutboundEnvelope],
    inbound_envelope_send_channel: trio.abc.SendChannel[InboundEnvelope],
) -> AsyncIterator[None]:
    async with trio.open_nursery() as nursery:
        nursery.start_soon(
            _staple,
            sender,
            outbound_envelope_receive_channel,
            inbound_envelope_send_channel,
        )
        try:
            yield
        finally:
            nursery.cancel_scope.cancel()


class ManagedDispatcher(NamedTuple):
    dispatcher: DispatcherAPI
    send_channel: trio.abc.SendChannel[InboundEnvelope]


class PoolAndChannels(NamedTuple):
    pool: PoolAPI
    channels: SessionChannels


class Tester(TesterAPI):
    logger = logging.getLogger("ddht.driver.Tester")

    _pools: Dict[NodeID, PoolAndChannels]
    _managed_dispatchers: Dict[NodeID, ManagedDispatcher]
    _running_dispatchers: Set[Tuple[NodeID, NodeID]]

    def __init__(self) -> None:
        self._pools = {}
        self._managed_dispatchers = {}
        self._running_dispatchers = set()

    def register_pool(self, pool: PoolAPI, channels: SessionChannels) -> None:
        if pool.local_node_id in self._pools:
            raise Exception("Already registered")
        self._pools[pool.local_node_id] = PoolAndChannels(pool, channels)

    def _get_or_create_pool_for_node(self, node: NodeAPI) -> PoolAndChannels:
        if node.node_id not in self._pools:
            channels = SessionChannels.init()
            pool = Pool(
                local_private_key=node.private_key,
                local_node_id=node.enr.node_id,
                enr_db=node.enr_db,
                outbound_envelope_send_channel=channels.outbound_envelope_send_channel,
                inbound_message_send_channel=channels.inbound_message_send_channel,
                events=node.events,
            )
            self._pools[node.node_id] = PoolAndChannels(pool, channels)
        return self._pools[node.node_id]

    def node(
        self,
        private_key: Optional[keys.PrivateKey] = None,
        endpoint: Optional[Endpoint] = None,
        enr_db: Optional[ENRDatabaseAPI] = None,
        events: Optional[EventsAPI] = None,
    ) -> Node:
        if private_key is None:
            private_key = PrivateKeyFactory()
        if endpoint is None:
            endpoint = EndpointFactory.localhost()
        if enr_db is None:
            enr_db = ENRDB({})
        return Node(
            private_key=private_key, endpoint=endpoint, enr_db=enr_db, events=events
        )

    def session_pair(
        self,
        initiator: Optional[NodeAPI] = None,
        recipient: Optional[NodeAPI] = None,
        initiator_session: Optional[SessionAPI] = None,
        recipient_session: Optional[SessionAPI] = None,
    ) -> SessionPairAPI:
        if initiator is None:
            initiator = self.node()
        if recipient is None:
            recipient = self.node()

        initiator_pool, initiator_channels = self._get_or_create_pool_for_node(
            initiator
        )

        initiator_session = initiator_pool.initiate_session(
            recipient.endpoint, recipient.node_id
        )
        # the initiator always needs to have the remote enr present in their database
        initiator.enr_db.set_enr(recipient.enr)

        recipient_pool, recipient_channels = self._get_or_create_pool_for_node(
            recipient
        )

        recipient_session = recipient_pool.receive_session(initiator.endpoint)

        return SessionPair(
            initiator=initiator,
            initiator_session=initiator_session,
            initiator_channels=initiator_channels,
            recipient=recipient,
            recipient_session=recipient_session,
            recipient_channels=recipient_channels,
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

        key = (left.node_id, right.node_id)
        if key in self._running_dispatchers:
            raise Exception(
                "Already running dispatchers for: "
                f"{humanize_node_id(left.node_id)} <-> "
                f"{humanize_node_id(right.node_id)}"
            )

        self.logger.info("setting up dispatcher pair: %s <> %s", left, right)

        async with AsyncExitStack() as stack:
            left_pool, left_channels = self._get_or_create_pool_for_node(left)

            if left.node_id in self._managed_dispatchers:
                self.logger.info("dispatcher already present for %s", left)
                left_managed_dispatcher = self._managed_dispatchers[left.node_id]
            else:
                self.logger.info("setting up new dispatcher for %s", left)
                (
                    left_inbound_envelope_send_channel,
                    left_inbound_envelope_receive_channel,
                ) = trio.open_memory_channel[InboundEnvelope](256)

                left_dispatcher = Dispatcher(
                    left_inbound_envelope_receive_channel,
                    left_channels.inbound_message_receive_channel,
                    left_pool,
                    left.enr_db,
                    events=left.events,
                )
                left_managed_dispatcher = ManagedDispatcher(
                    dispatcher=left_dispatcher,
                    send_channel=left_inbound_envelope_send_channel,
                )
                self._managed_dispatchers[left.node_id] = left_managed_dispatcher
                await stack.enter_async_context(
                    background_trio_service(left_dispatcher)
                )

            right_pool, right_channels = self._get_or_create_pool_for_node(right)

            if right.node_id in self._managed_dispatchers:
                self.logger.info("dispatcher already present for %s", right)
                right_managed_dispatcher = self._managed_dispatchers[right.node_id]
            else:
                self.logger.info("setting up new dispatcher for %s", right)
                (
                    right_inbound_envelope_send_channel,
                    right_inbound_envelope_receive_channel,
                ) = trio.open_memory_channel[InboundEnvelope](256)

                right_dispatcher = Dispatcher(
                    right_inbound_envelope_receive_channel,
                    right_channels.inbound_message_receive_channel,
                    right_pool,
                    right.enr_db,
                    events=right.events,
                )
                right_managed_dispatcher = ManagedDispatcher(
                    dispatcher=right_dispatcher,
                    send_channel=right_inbound_envelope_send_channel,
                )
                self._managed_dispatchers[right.node_id] = right_managed_dispatcher
                await stack.enter_async_context(
                    background_trio_service(right_dispatcher)
                )

            if left is node_a:
                dispatchers = (
                    left_managed_dispatcher.dispatcher,
                    right_managed_dispatcher.dispatcher,
                )
            elif left is node_b:
                dispatchers = (
                    right_managed_dispatcher.dispatcher,
                    left_managed_dispatcher.dispatcher,
                )
            else:
                raise Exception("Invariant")

            async with staple(
                left,
                left_channels.outbound_envelope_receive_channel,
                right_managed_dispatcher.send_channel.clone(),  # type: ignore
            ):
                async with staple(
                    right,
                    right_channels.outbound_envelope_receive_channel,
                    left_managed_dispatcher.send_channel.clone(),  # type: ignore
                ):
                    self._running_dispatchers.add(key)
                    try:
                        yield dispatchers
                    finally:
                        self._running_dispatchers.remove(key)
