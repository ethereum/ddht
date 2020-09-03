import itertools
import logging
import secrets
import time
from typing import List, Optional, Tuple

from async_service import Service
from eth_enr import ENRAPI, ENRDatabaseAPI
from eth_enr.exceptions import OldSequenceNumber
from eth_typing import NodeID
from eth_utils import encode_hex
from eth_utils.toolz import take
from mypy_extensions import TypedDict
import trio
from trio.abc import SendChannel

from ddht._utils import every
from ddht.base_message import (
    AnyInboundMessage,
    AnyOutboundMessage,
    InboundMessage,
    OutboundMessage,
)
from ddht.endpoint import Endpoint
from ddht.enr import partition_enrs
from ddht.exceptions import UnexpectedMessage
from ddht.kademlia import KademliaRoutingTable, compute_log_distance, iter_closest_nodes
from ddht.v5.abc import MessageDispatcherAPI
from ddht.v5.constants import (
    FIND_NODE_RESPONSE_TIMEOUT,
    LOOKUP_PARALLELIZATION_FACTOR,
    LOOKUP_RETRY_THRESHOLD,
    NODES_MESSAGE_PAYLOAD_SIZE,
    REQUEST_RESPONSE_TIMEOUT,
    ROUTING_TABLE_LOOKUP_INTERVAL,
    ROUTING_TABLE_PING_INTERVAL,
)
from ddht.v5.endpoint_tracker import EndpointVote
from ddht.v5.messages import FindNodeMessage, NodesMessage, PingMessage, PongMessage


class BaseRoutingTableManagerComponent(Service):
    """Base class for services that participate in managing the routing table."""

    logger = logging.getLogger(
        "ddht.v5.routing_table_manager.BaseRoutingTableManagerComponent"
    )

    def __init__(
        self,
        local_node_id: NodeID,
        routing_table: KademliaRoutingTable,
        message_dispatcher: MessageDispatcherAPI,
        enr_db: ENRDatabaseAPI,
    ) -> None:
        self.local_node_id = local_node_id
        self.routing_table = routing_table
        self.message_dispatcher = message_dispatcher
        self.enr_db = enr_db

    def update_routing_table(self, node_id: NodeID) -> None:
        """
        Update a peer's entry in the routing table.

        This method should be called, whenever we receive a message from them.
        """
        self.logger.debug("Updating %s in routing table", encode_hex(node_id))
        self.routing_table.update(node_id)

    def get_local_enr(self) -> ENRAPI:
        """Get the local enr from the ENR DB."""
        try:
            local_enr = self.enr_db.get_enr(self.local_node_id)
        except KeyError:
            raise ValueError(
                f"Local ENR with node id {encode_hex(self.local_node_id)} not "
                f"present in db"
            )
        else:
            return local_enr

    async def maybe_request_remote_enr(
        self, inbound_message: AnyInboundMessage
    ) -> None:
        """Request the peers ENR if there is a newer version according to a ping or pong."""
        if not isinstance(inbound_message.message, (PingMessage, PongMessage)):
            raise TypeError(
                f"Only ping and pong messages contain an ENR sequence number, got "
                f"{inbound_message}"
            )

        try:
            remote_enr = self.enr_db.get_enr(inbound_message.sender_node_id)
        except KeyError:
            self.logger.warning(
                "No ENR of %s present in the database even though it should post handshake. "
                "Requesting it now.",
                encode_hex(inbound_message.sender_node_id),
            )
            request_update = True
        else:
            current_sequence_number = remote_enr.sequence_number
            advertized_sequence_number = inbound_message.message.enr_seq

            if current_sequence_number < advertized_sequence_number:
                self.logger.debug(
                    "ENR advertized by %s is newer than ours (sequence number %d > %d)",
                    encode_hex(inbound_message.sender_node_id),
                    advertized_sequence_number,
                    current_sequence_number,
                )
                request_update = True
            elif current_sequence_number == advertized_sequence_number:
                self.logger.debug(
                    "ENR of %s is up to date (sequence number %d)",
                    encode_hex(inbound_message.sender_node_id),
                    advertized_sequence_number,
                )
                request_update = False
            elif current_sequence_number > advertized_sequence_number:
                self.logger.warning(
                    "Peer %s advertizes apparently outdated ENR (sequence number %d < %d)",
                    encode_hex(inbound_message.sender_node_id),
                    advertized_sequence_number,
                    current_sequence_number,
                )
                request_update = False
            else:
                raise Exception("Invariant: Unreachable")

        if request_update:
            await self.request_remote_enr(inbound_message)

    async def request_remote_enr(self, inbound_message: AnyInboundMessage) -> None:
        """Request the ENR of the sender of an inbound message and store it in the ENR db."""
        self.logger.debug(
            "Requesting ENR from %s", encode_hex(inbound_message.sender_node_id)
        )

        find_nodes_message = FindNodeMessage(
            request_id=self.message_dispatcher.get_free_request_id(
                inbound_message.sender_node_id
            ),
            distance=0,  # request enr of the peer directly
        )
        try:
            with trio.fail_after(REQUEST_RESPONSE_TIMEOUT):
                response = await self.message_dispatcher.request(
                    inbound_message.sender_node_id,
                    find_nodes_message,
                    endpoint=inbound_message.sender_endpoint,
                )
        except trio.TooSlowError:
            self.logger.warning(
                "FindNode request to %s has timed out",
                encode_hex(inbound_message.sender_node_id),
            )
            return

        sender_node_id = response.sender_node_id
        self.update_routing_table(sender_node_id)

        if not isinstance(response.message, NodesMessage):
            self.logger.warning(
                "Peer %s responded to FindNode with %s instead of Nodes message",
                encode_hex(sender_node_id),
                response.message.__class__.__name__,
            )
            return
        self.logger.debug("Received Nodes message from %s", encode_hex(sender_node_id))

        if len(response.message.enrs) == 0:
            self.logger.warning(
                "Peer %s responded to FindNode with an empty Nodes message",
                encode_hex(sender_node_id),
            )
        elif len(response.message.enrs) > 1:
            self.logger.warning(
                "Peer %s responded to FindNode with more than one ENR",
                encode_hex(inbound_message.sender_node_id),
            )

        for enr in response.message.enrs:
            if enr.node_id != sender_node_id:
                self.logger.warning(
                    "Peer %s responded to FindNode with ENR from %s",
                    encode_hex(sender_node_id),
                    encode_hex(response.message.enrs[0].node_id),
                )
            self.enr_db.set_enr(enr)


class PingHandlerService(BaseRoutingTableManagerComponent):
    """Responds to Pings with Pongs and requests ENR updates."""

    logger = logging.getLogger("ddht.v5.routing_table_manager.PingHandlerService")

    def __init__(
        self,
        local_node_id: NodeID,
        routing_table: KademliaRoutingTable,
        message_dispatcher: MessageDispatcherAPI,
        enr_db: ENRDatabaseAPI,
        outbound_message_send_channel: SendChannel[OutboundMessage[PongMessage]],
    ) -> None:
        super().__init__(local_node_id, routing_table, message_dispatcher, enr_db)
        self.outbound_message_send_channel = outbound_message_send_channel

    async def run(self) -> None:
        channel_handler_subscription = self.message_dispatcher.add_request_handler(
            PingMessage
        )
        async with channel_handler_subscription:
            async for inbound_message in channel_handler_subscription:
                self.logger.debug(
                    "Handling %s from %s",
                    inbound_message,
                    encode_hex(inbound_message.sender_node_id),
                )
                self.update_routing_table(inbound_message.sender_node_id)
                await self.respond_with_pong(inbound_message)
                self.manager.run_task(self.maybe_request_remote_enr, inbound_message)

    async def respond_with_pong(
        self, inbound_message: InboundMessage[PingMessage]
    ) -> None:
        if not isinstance(inbound_message.message, PingMessage):
            raise TypeError(
                f"Can only respond with Pong to Ping, not "
                f"{inbound_message.message.__class__.__name__}"
            )

        local_enr = self.get_local_enr()

        pong = PongMessage(
            request_id=inbound_message.message.request_id,
            enr_seq=local_enr.sequence_number,
            packet_ip=inbound_message.sender_endpoint.ip_address,
            packet_port=inbound_message.sender_endpoint.port,
        )
        outbound_message = inbound_message.to_response(pong)
        self.logger.debug(
            "Responding with Pong to %s", encode_hex(outbound_message.receiver_node_id)
        )
        await self.outbound_message_send_channel.send(outbound_message)


class FindNodeHandlerService(BaseRoutingTableManagerComponent):
    """Responds to FindNode with Nodes messages."""

    logger = logging.getLogger("ddht.v5.routing_table_manager.FindNodeHandlerService")

    def __init__(
        self,
        local_node_id: NodeID,
        routing_table: KademliaRoutingTable,
        message_dispatcher: MessageDispatcherAPI,
        enr_db: ENRDatabaseAPI,
        outbound_message_send_channel: SendChannel[OutboundMessage[NodesMessage]],
    ) -> None:
        super().__init__(local_node_id, routing_table, message_dispatcher, enr_db)
        self.outbound_message_send_channel = outbound_message_send_channel

    async def run(self) -> None:
        handler_subscription = self.message_dispatcher.add_request_handler(
            FindNodeMessage
        )
        async with handler_subscription:
            async for inbound_message in handler_subscription:
                self.update_routing_table(inbound_message.sender_node_id)

                if not isinstance(inbound_message.message, FindNodeMessage):
                    raise TypeError(
                        f"Received {inbound_message.__class__.__name__} from message dispatcher "
                        f"even though we subscribed to FindNode messages"
                    )

                if inbound_message.message.distance == 0:
                    await self.respond_with_local_enr(inbound_message)
                else:
                    await self.respond_with_remote_enrs(inbound_message)

    async def respond_with_local_enr(
        self, inbound_message: InboundMessage[FindNodeMessage]
    ) -> None:
        """Send a Nodes message containing the local ENR in response to an inbound message."""
        local_enr = self.get_local_enr()
        nodes_message = NodesMessage(
            request_id=inbound_message.message.request_id, total=1, enrs=(local_enr,)
        )
        outbound_message = inbound_message.to_response(nodes_message)

        self.logger.debug(
            "Responding to %s with Nodes message containing local ENR",
            inbound_message.sender_endpoint,
        )
        await self.outbound_message_send_channel.send(outbound_message)

    async def respond_with_remote_enrs(
        self, inbound_message: InboundMessage[FindNodeMessage]
    ) -> None:
        """Send a Nodes message containing ENRs of peers at a given node distance."""
        node_ids = self.routing_table.get_nodes_at_log_distance(
            inbound_message.message.distance
        )

        enrs = []
        for node_id in node_ids:
            try:
                enr = self.enr_db.get_enr(node_id)
            except KeyError:
                self.logger.warning("Missing ENR for node %s", encode_hex(node_id))
            else:
                enrs.append(enr)

        enr_partitions = partition_enrs(enrs, NODES_MESSAGE_PAYLOAD_SIZE) or ((),)
        self.logger.debug(
            "Responding to %s with %d Nodes message containing %d ENRs at distance %d",
            inbound_message.sender_endpoint,
            len(enr_partitions),
            len(enrs),
            inbound_message.message.distance,
        )
        for partition in enr_partitions:
            nodes_message = NodesMessage(
                request_id=inbound_message.message.request_id,
                total=len(enr_partitions),
                enrs=partition,
            )
            outbound_message = inbound_message.to_response(nodes_message)
            await self.outbound_message_send_channel.send(outbound_message)


class PingSenderService(BaseRoutingTableManagerComponent):
    """Regularly sends pings to peers to check if they are still alive or not."""

    logger = logging.getLogger("ddht.v5.routing_table_manager.PingSenderService")

    def __init__(
        self,
        local_node_id: NodeID,
        routing_table: KademliaRoutingTable,
        message_dispatcher: MessageDispatcherAPI,
        enr_db: ENRDatabaseAPI,
        endpoint_vote_send_channel: SendChannel[EndpointVote],
    ) -> None:
        super().__init__(local_node_id, routing_table, message_dispatcher, enr_db)
        self.endpoint_vote_send_channel = endpoint_vote_send_channel

    async def run(self) -> None:
        async for _ in every(ROUTING_TABLE_PING_INTERVAL):  # noqa: F841
            if not self.routing_table.is_empty:
                log_distance = (
                    self.routing_table.get_least_recently_updated_log_distance()
                )
                candidates = self.routing_table.get_nodes_at_log_distance(log_distance)
                node_id = candidates[-1]
                self.logger.debug("Pinging %s", encode_hex(node_id))
                await self.ping(node_id)
            else:
                self.logger.warning("Routing table is empty, no one to ping")

    async def ping(self, node_id: NodeID) -> None:
        local_enr = self.get_local_enr()
        ping = PingMessage(
            request_id=self.message_dispatcher.get_free_request_id(node_id),
            enr_seq=local_enr.sequence_number,
        )

        try:
            with trio.fail_after(REQUEST_RESPONSE_TIMEOUT):
                inbound_message = await self.message_dispatcher.request(node_id, ping)
        except ValueError as value_error:
            self.logger.warning(
                "Failed to send ping to %s: %s", encode_hex(node_id), value_error
            )
        except trio.TooSlowError:
            self.logger.warning("Ping to %s timed out", encode_hex(node_id))
        else:
            if not isinstance(inbound_message.message, PongMessage):
                self.logger.warning(
                    "Peer %s responded to Ping with %s instead of Pong",
                    encode_hex(node_id),
                    inbound_message.message.__class__.__name__,
                )
            else:
                self.logger.debug("Received Pong from %s", encode_hex(node_id))

                self.update_routing_table(node_id)

                pong = inbound_message.message
                local_endpoint = Endpoint(
                    ip_address=pong.packet_ip, port=pong.packet_port
                )
                endpoint_vote = EndpointVote(
                    endpoint=local_endpoint, node_id=node_id, timestamp=time.monotonic()
                )
                await self.endpoint_vote_send_channel.send(endpoint_vote)

                await self.maybe_request_remote_enr(inbound_message)


class LookupService(BaseRoutingTableManagerComponent):
    """Performs recursive lookups."""

    logger = logging.getLogger("ddht.v5.routing_table_manager.LookupService")

    async def run(self) -> None:
        async for _ in every(ROUTING_TABLE_LOOKUP_INTERVAL):
            target = NodeID(secrets.token_bytes(32))
            await self.lookup(target)

    async def lookup(self, target: NodeID) -> None:
        self.logger.info("Looking up %s", encode_hex(target))

        queried_node_ids = set()
        unresponsive_node_ids = set()
        received_enrs: List[ENRAPI] = []
        received_node_ids: List[NodeID] = []

        async def lookup_and_store_response(peer: NodeID) -> None:
            enrs = await self.lookup_at_peer(peer, target)
            queried_node_ids.add(peer)
            if enrs is not None:
                for enr in enrs:
                    received_node_ids.append(enr.node_id)

                    try:
                        self.enr_db.set_enr(enr)
                    except OldSequenceNumber:
                        received_enrs.append(self.enr_db.get_enr(enr.node_id))
                    else:
                        received_enrs.append(enr)
            else:
                unresponsive_node_ids.add(peer)

        for lookup_round_counter in itertools.count():
            candidates = iter_closest_nodes(
                target, self.routing_table, received_node_ids
            )
            responsive_candidates = itertools.dropwhile(
                lambda node: node in unresponsive_node_ids, candidates
            )
            closest_k_candidates = take(
                self.routing_table.bucket_size, responsive_candidates
            )
            closest_k_unqueried_candidates = (
                candidate
                for candidate in closest_k_candidates
                if candidate not in queried_node_ids
            )
            nodes_to_query = tuple(
                take(LOOKUP_PARALLELIZATION_FACTOR, closest_k_unqueried_candidates)
            )

            if nodes_to_query:
                self.logger.debug(
                    "Starting lookup round %d for %s",
                    lookup_round_counter + 1,
                    encode_hex(target),
                )
                async with trio.open_nursery() as nursery:
                    for peer in nodes_to_query:
                        nursery.start_soon(lookup_and_store_response, peer)
            else:
                self.logger.debug(
                    "Lookup for %s finished in %d rounds",
                    encode_hex(target),
                    lookup_round_counter,
                )
                break

    async def lookup_at_peer(
        self, peer: NodeID, target: NodeID
    ) -> Optional[Tuple[ENRAPI, ...]]:
        self.logger.debug(
            "Looking up %s at node %s", encode_hex(target), encode_hex(peer)
        )
        distance = compute_log_distance(peer, target)
        first_attempt = await self.request_nodes(peer, target, distance)
        if first_attempt is None:
            self.logger.debug("Lookup with node %s failed", encode_hex(peer))
            return None
        elif len(first_attempt) >= LOOKUP_RETRY_THRESHOLD:
            self.logger.debug(
                "Node %s responded with %d nodes with single attempt",
                encode_hex(peer),
                len(first_attempt),
            )
            return first_attempt
        else:
            second_attempt = await self.request_nodes(peer, target, distance)
            both_attempts = first_attempt + (second_attempt or ())
            self.logger.debug(
                "Node %s responded with %d nodes in two attempts",
                encode_hex(peer),
                len(both_attempts),
            )
            return both_attempts

    async def request_nodes(
        self, peer: NodeID, target: NodeID, distance: int
    ) -> Optional[Tuple[ENRAPI, ...]]:
        """
        Send a FindNode request to the given peer and return the ENRs in the response.

        If the peer does not respond or fails to respond properly, `None` is returned
        indicating that retrying with a larger distance is futile.
        """
        request = FindNodeMessage(
            request_id=self.message_dispatcher.get_free_request_id(peer),
            distance=distance,
        )
        try:
            with trio.fail_after(FIND_NODE_RESPONSE_TIMEOUT):
                inbound_messages = await self.message_dispatcher.request_nodes(
                    peer, request
                )
        except ValueError as value_error:
            self.logger.warning(
                "Failed to send FindNode to %s: %s", encode_hex(peer), value_error
            )
            return None
        except UnexpectedMessage as unexpected_message_error:
            self.logger.warning(
                "Peer %s sent unexpected message to FindNode request: %s",
                encode_hex(peer),
                unexpected_message_error,
            )
            return None
        except trio.TooSlowError:
            self.logger.warning(
                "Peer %s did not respond in time to FindNode request", encode_hex(peer)
            )
            return None
        else:
            self.update_routing_table(peer)
            enrs = tuple(
                enr
                for inbound_message in inbound_messages
                for enr in inbound_message.message.enrs
            )
            return enrs


SharedComponentKwargType = TypedDict(
    "SharedComponentKwargType",
    {
        "local_node_id": NodeID,
        "routing_table": KademliaRoutingTable,
        "message_dispatcher": MessageDispatcherAPI,
        "enr_db": ENRDatabaseAPI,
    },
)


class RoutingTableManager(Service):
    """Manages the routing table. The actual work is delegated to a few sub components."""

    def __init__(
        self,
        local_node_id: NodeID,
        routing_table: KademliaRoutingTable,
        message_dispatcher: MessageDispatcherAPI,
        enr_db: ENRDatabaseAPI,
        outbound_message_send_channel: SendChannel[AnyOutboundMessage],
        endpoint_vote_send_channel: SendChannel[EndpointVote],
    ) -> None:
        shared_component_kwargs = SharedComponentKwargType(
            {
                "local_node_id": local_node_id,
                "routing_table": routing_table,
                "message_dispatcher": message_dispatcher,
                "enr_db": enr_db,
            }
        )

        self.ping_handler_service = PingHandlerService(
            outbound_message_send_channel=outbound_message_send_channel,
            **shared_component_kwargs,
        )
        self.find_node_handler_service = FindNodeHandlerService(
            outbound_message_send_channel=outbound_message_send_channel,
            **shared_component_kwargs,
        )
        self.ping_sender_service = PingSenderService(
            endpoint_vote_send_channel=endpoint_vote_send_channel,
            **shared_component_kwargs,
        )
        self.lookup_service = LookupService(**shared_component_kwargs)

    async def run(self) -> None:
        child_services = (
            self.ping_handler_service,
            self.find_node_handler_service,
            self.ping_sender_service,
            self.lookup_service,
        )
        for child_service in child_services:
            self.manager.run_daemon_child_service(child_service)

        await self.manager.wait_finished()
