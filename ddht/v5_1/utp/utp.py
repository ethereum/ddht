from contextlib import asynccontextmanager
import enum
import io
from typing import AsyncIterator, Dict, NewType, Tuple, Optional

from async_service import Service, background_trio_service
from eth_typing import NodeID
import trio

from ddht.exceptions import DecodingError
from ddht.v5_1.abc import NetworkAPI
from ddht.v5_1.collator import DataCollator
from ddht.v5_1.messages import TalkRequestMessage
from ddht.v5_1.utp.abc import UTPAPI
from ddht.v5_1.utp.ack_tracker import AckTracker
from ddht.v5_1.utp.packets import Packet, PacketHeader, MAX_PACKET_DATA, PacketType, Segment


ConnectionID = NewType("ConnectionID", int)


class ConnectionStatus(enum.Enum):
    # The default initial state
    EMBRIO = enum.auto()

    # This node has sent the initial SYN packet
    SYN_SENT = enum.auto()

    # This node has received the initial SYN packet
    SYN_RECEIVED = enum.auto()

    # The connection is open and alive
    CONNECTED = enum.auto()

    # The connection has received the FIN packet and is closing
    CLOSING = enum.auto()

    # The connection is closed
    CLOSED = enum.auto()


EMBRIO = ConnectionStatus.EMBRIO
SYN_SENT = ConnectionStatus.SYN_SENT
SYN_RECEIVED = ConnectionStatus.SYN_RECEIVED
CONNECTED = ConnectionStatus.CONNECTED
CLOSING = ConnectionStatus.CLOSING
CLOSED = ConnectionStatus.CLOSED


MAX_SEQ_NR = 2**16 - 1


class Connection(Service):
    outbound_packet_receive: trio.abc.ReceiveChannel[Packet]
    inbound_packet_send: trio.abc.SendChannel[Packet]

    node_id: NodeID

    status: ConnectionStatus

    max_seq_nr: int = MAX_SEQ_NR

    def __init__(self,
                 node_id: NodeID,
                 send_id: ConnectionID,
                 receive_id: ConnectionID,
                 ) -> None:
        self.send_id = send_id
        self.receive_id = receive_id

        self.status = ConnectionStatus.EMBRIO

        (
            self._outbound_packet_send,
            self.outbound_packet_receive,
        ) = trio.open_memory_channel[Packet](256)
        (
            self.inbound_packet_send,
            self._inbound_packet_receive,
        ) = trio.open_memory_channel[Packet](256)

        self.seq_nr = 1
        self.acker = AckTracker()

        self._unacked_packet_buffer: Dict[int, Packet] = {}

        self._collator = DataCollator

        (
            self._outbound_data_send,
            self._outbound_data_receive,
        ) = trio.open_memory_channel[Packet](0)

        (
            self._inbound_data_send,
            self._inbound_data_receive,
        ) = trio.open_memory_channel[bytes](256)
        self._inbound_data_buffer = io.BytesIO()

    @property
    def ack_nr(self) -> Optional[int]:
        if self.acker.ack_nr == 0:
            return None
        else:
            return self.acker.ack_nr - 1

    async def receive_some(self, max_bytes: int) -> bytes:
        self._buffer.seek(0)

        data = self._buffer.read(max_bytes)

        while len(data) < max_bytes:
            try:
                data += self._inbound_receive.receive_nowait()
            except trio.WouldBlock:
                if data:
                    break
                else:
                    data += await self._inbound_receive.receive()

        if len(data) > max_bytes:
            remainder = data[max_bytes:]
            data = data[:max_bytes]

            # The only way we can end up with `remainder` data is if we read
            # new data in over the stream.  In this case, we can know that
            # we've read all information from the buffer and that any extra
            # information should be written to the front of the buffer, and the
            # buffer truncated down to the new size of however much remainder
            # data was left.
            self._buffer.seek(0)
            self._buffer.write(remainder)
            self._buffer.truncate(len(remainder))
            self._buffer.seek(0)

        return data

    async def send_all(self, data: bytes) -> None:
        await self._outbound_data_send.send(data)

    async def run(self) -> None:
        self.manager.run_daemon_task(self._handle_inbound_packets)
        self.manager.run_daemon_task(self._handle_outbound_packets)

        self.manager.run_daemon_task(self._handle_outbound_data)

        await self.manager.wait_finished()

    #
    # Long lived packet handlers
    #
    async def _handle_inbound_packets(self) -> None:
        async with self._inbound_packet_receive as packet_receive:
            async for packet in packet_receive:
                acked = self.ack_tracker.ack(packet.header.seq_nr)
                for seq_nr in acked:
                    self._unacked_packet_buffer.pop(seq_nr, None)

                if packet.header.type is PacketType.DATA:
                    # data packet
                    if self.status is SYN_RECEIVED:
                        self.status = CONNECTED

                    if self.status in {CONNECTED, CLOSING}:
                        if packet.seq_nr > self.max_seq_nr:
                            self.logger.debug(
                                "Ignoring Packet: packet=%s  "
                                "reason=sequent-number-out-of-bounds  max-seq=%d  "
                                "packet-seq=%d",
                                packet,
                                self.max_seq_nr,
                                packet.header.seq_nr,
                            )
                        else:
                            segment = Segment(packet.header.seq_nr, packet.header.data)
                            data_chunks = self.collator.collate(segment)

                            for chunk in data_chunks:
                                await self._inbound_data_send.send(segment.data)

                            await self._send_packet(PacketType.STATE)
                    else:
                        self.logger.debug(
                            "Ignoring Packet: packet=%s  reason=invalid-state  state=%s",
                            packet,
                            self.state,
                        )
                elif packet.header.type is PacketType.FIN:
                    # last packet
                    self.logger.debug(
                        "Closing Connection: connection=%s  reason=FIN",
                        self,
                    )
                    self.status = CLOSING
                    self.max_seq_nr = packet.header.seq_nr
                    await self._send_packet(PacketType.STATE)
                elif packet.header.type is PacketType.STATE:
                    if self.state is SYN_SENT:
                        self.state = CONNECTED
                    # ack packet
                    self.logger.debug(
                        "Acking Packet: packet=%s",
                        packet,
                    )
                elif packet.header.type is PacketType.RESET:
                    # hard termination
                    self.logger.debug(
                        "Closing Connection: connection=%s  reason=RESET",
                        self,
                    )
                    self.status = CLOSED
                    self.manager.cancel()
                elif packet.header.type is PacketType.SYN:
                    # first packet over the connection
                    if self.status is EMBRIO:
                        self.status = SYN_RECEIVED
                    else:
                        self.logger.debug(
                            "Ignoring Packet: packet=%s  reason=already-connected",
                            packet,
                        )
                else:
                    raise Exception("Invariant: unknown packet type")

    async def _send_packet(self,
                           packet_type: PacketType,
                           data: Optional[bytes] = None,
                           ) -> None:
        if not data and packet_type is PacketType.DATA:
            raise TypeError("DATA packets must contain a data payload")
        elif data is not None and packet_type is not PacketType.DATA:
            raise TypeError("Only DATA packets may contain a data payload")

        if data:
            self.seq_nr += 1

        if self.tracker.acked:
            raise NotImplementedError("Multi-ack not yet implemented")
        else:
            extensions = ()

        header = PacketHeader(
            type=packet_type,
            version=1,
            extensions=extensions,
            connection_id=self.send_id,
            timestamp_microseconds=1234,
            timestamp_difference_microseconds=4321,
            wnd_size=1024,
            seq_nr=self.seq_nr,
            ack_nr=self.ack_nr,
        )
        packet = Packet(header, data)
        self._unacked_packet_buffer[self.seq_nr] = packet
        await self._outbound_packet_send(packet)

    async def _handle_outbound_packets(self) -> None:
        buffer = io.BytesIO()
        async with self._outbound_packet_send as packet_send:
            async with self._outbound_data_receive as data_receive:
                while self.manager.is_running:
                    if not buffer.tell():
                        # block until there is data available
                        buffer.write(await data_receive.receive())
                    else:
                        # yield to the event loop at least once per iteration
                        await trio.lowlevel.checkpoint()

                    # if there is more data immediately available and there is
                    # room in the packet then we should include it.
                    while buffer.tell() < MAX_PACKET_DATA:
                        try:
                            buffer.write(data_receive.receive_nowait())
                        except trio.WouldBlock:
                            break

                    buffer.seek(0)
                    data = buffer.read(MAX_PACKET_DATA)

                    remainder = buffer.read()
                    buffer.seek(0)
                    buffer.write(remainder)
                    buffer.truncate()

                    header = PacketHeader(...)
                    packet = Packet(header=header, data=data)

                    # TODO: congestion control goes here...
                    await packet_send.send(packet)

    async def _handle_outbound_data(self) -> None:
        buffer = io.BytesIO()

        async with self._outbound_data_receive as data_receive:
            while self.manager.is_running:
                # If the outbound buffer is empty, block until there is data to
                # be sent
                if buffer.tell() == 0:
                    buffer.write(await data_receive.receive())

                # If the buffer is under the current maximum packet size,
                # attempt to fill it with data from the channel.
                while buffer.tell() < MAX_PACKET_DATA:
                    try:
                        buffer.write(data_receive.receive_nowait())
                    except trio.WouldBlock:
                        break

                # If we have data in the buffer, send
                while buffer.tell():
                    buffer.seek(0)
                    data = buffer.read(MAX_PACKET_DATA)

                    remainder = buffer.read()
                    buffer.seek(0)
                    buffer.write(remainder)
                    buffer.truncate()

                    await self._send_packet(PacketType.DATA, data=data)

                    # Breaking from the send loop here gives us a chance to
                    # send a full packet when we only have a partial packet
                    # buffered.  If there is more data ready to be sent it will
                    # be gathered.  Otherwise, the incomplete packet will be
                    # sent on the next pass through the full loop.
                    if buffer.tell() < MAX_PACKET_DATA:
                        break


class UTP(Service, UTPAPI):
    protocol_id = b'utp'

    def __init__(self, network: NetworkAPI) -> None:
        self.network = network
        self._connections: Dict[Tuple[NodeID, ConnectionID], Connection] = {}

    @asynccontextmanager
    async def open_connection(self,
                              connection_id: int,
                              ) -> AsyncIterator[Connection]:
        ...

    @asynccontextmanager
    async def receive_connection(self,
                                 connection_id: int,
                                 ) -> AsyncIterator[Connection]:
        ...

    async def run(self) -> None:
        async with self.network.client.dispatcher.subscribe(
            TalkRequestMessage
        ) as subscription:
            connection: Connection

            async for request in subscription:
                if request.message.protocol != self.protocol_id:
                    continue

                try:
                    packet = Packet.decode(request.message.payload)
                except DecodingError:
                    self.logger.debug(
                        "Discarding Message: message=%s  reason=malformed",
                        request,
                    )
                    continue

                key = (request.sender_node_id, packet.header.connection_id)

                try:
                    connection = self._connections[key]
                except KeyError:
                    if packet.header.type is Packet.SYN:
                        connection = Connection(
                            node_id=request.sender_node_id,
                            send_id=packet.header.connection_id,
                            receive_id=packet.header.connection_id + 1,
                        )
                        self._connections[key] = connection
                        self.manager.run_task(self._manage_connection, connection)
                    else:
                        self.logger.debug(
                            "Discarding Packet: packet=%s  reason=no-connection",
                            packet,
                        )
                        continue

                try:
                    connection.inbound_packet_send.send_nowait(packet)
                except trio.WouldBlock:
                    self.logger.debug(
                        "Discarding Packet: packet=%s  reason=buffer-full",
                        packet,
                    )

    async def _manage_connection(self, connection: Connection) -> None:
        async with background_trio_service(connection):
            async with connection.outbound_packet_receive as outbound_packet_receive:
                async for packet in outbound_packet_receive:
                    payload = packet.encode()
                    endpoint = self.network.endpoint_for_node_id(connection.node_id)

                    await self.network.client.send_talk_request(
                        connection.node_id,
                        endpoint,
                        protocol_id=self.protocol_id,
                        payload=payload,
                    )
