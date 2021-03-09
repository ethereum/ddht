import collections
import enum
import io
from typing import Dict, Optional

from async_service import Service
from eth_typing import NodeID
from eth_utils import get_extended_debug_logger
import trio

from ddht.v5_1.utp.ack import AckTracker
from ddht.v5_1.utp.collator import DataCollator, Segment
from ddht.v5_1.utp.data_buffer import DataBuffer
from ddht.v5_1.utp.extensions import SelectiveAck
from ddht.v5_1.utp.typing import ConnectionID
from ddht.v5_1.utp.packets import Packet, PacketHeader, MAX_PACKET_DATA, PacketType


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

    _status: ConnectionStatus

    max_seq_nr: int = MAX_SEQ_NR

    def __init__(self,
                 node_id: NodeID,
                 send_id: ConnectionID,
                 receive_id: ConnectionID,
                 ) -> None:
        self.logger = get_extended_debug_logger('ddht.utp.Connection')

        if abs(send_id - receive_id) != 1:
            raise ValueError("Invalid connection ids")

        self.send_id = send_id
        self.receive_id = receive_id

        self._status = EMBRIO

        (
            self._outbound_packet_send,
            self.outbound_packet_receive,
        ) = trio.open_memory_channel[Packet](256)
        (
            self.inbound_packet_send,
            self._inbound_packet_receive,
        ) = trio.open_memory_channel[Packet](256)

        self.seq_nr = 0
        self.acker = AckTracker()

        self._unacked_packet_buffer: Dict[int, Packet] = {}

        (
            self._outbound_data_send,
            self._outbound_data_receive,
        ) = trio.open_memory_channel[Packet](0)

        self._data_buffer = DataBuffer()
        self._receive_buffer = collections.deque()

        self._send_lock = trio.Lock()

        self._send_ready = trio.Event()

    def __str__(self) -> str:
        return (
            f"Connection("
            f"id={self.send_id}|{self.receive_id} "
            f"seq_nr={self.seq_nr} "
            f"ack_nr={self.ack_nr} "
            f"dial={'in' if self.is_inbound else 'out'}"
            f")"
        )

    @property
    def is_outbound(self) -> bool:
        return self.send_id == self.receive_id + 1

    @property
    def is_inbound(self) -> bool:
        return not self.is_outbound

    @property
    def ack_nr(self) -> Optional[int]:
        try:
            return self.acker.ack_nr
        except AttributeError:
            # TODO: ugly
            return None

    @property
    def status(self) -> ConnectionStatus:
        return self._status

    @status.setter
    def status(self, value: ConnectionStatus) -> None:
        self.logger.debug("[%s] status: %s -> %s", self, self._status.name, value.name)
        self._status = value

    async def finalize(self) -> None:
        """
        Send the `FIN` packet to signal this connection is closing.
        """
        if self.status in {CLOSING, CLOSED}:
            raise TypeError("Cannot send data")

        async with self._send_lock:
            self.status = CLOSING
            await self._send_packet(PacketType.FIN)

    async def reset(self) -> None:
        """
        Send the `RESET` packet and force the connection closed.
        """
        await self._send_packet(PacketType.RESET)
        self.status = CLOSED
        self.manager.cancel()

    async def receive_some(self, max_bytes: int) -> bytes:
        return await self._data_buffer.receive_some(max_bytes)

    async def send_all(self, data: bytes) -> None:
        await self._send_ready.wait()

        async with self._send_lock:
            if self.status in {CLOSING, CLOSED, EMBRIO}:
                raise TypeError("Cannot send data")

            data_view = memoryview(data)
            for idx in range(0, len(data), MAX_PACKET_DATA):
                await self._outbound_data_send.send(data_view[idx: idx + MAX_PACKET_DATA])

    async def run(self) -> None:
        async with self._outbound_data_send:
            self.manager.run_daemon_task(self._handle_inbound_packets)
            self.manager.run_daemon_task(self._handle_outbound_packets)

            self.manager.run_daemon_task(self._handle_outbound_data)

            if self.is_outbound:
                self.status = ConnectionStatus.SYN_SENT
                await self._send_packet(PacketType.SYN)

            await self.manager.wait_finished()

    #
    # Long lived packet handlers
    #
    async def _handle_inbound_packets(self) -> None:
        async with self._inbound_packet_receive as packet_receive:
            while self.manager.is_running:
                packet = await packet_receive.receive()

                if self.is_inbound:
                    if self.status is EMBRIO and packet.header.type is PacketType.SYN:
                        self.acker.ack(packet.header.seq_nr)
                        self._collator = DataCollator(packet.header.seq_nr + 1)
                        await self._handle_packet(packet)
                        self._send_ready.set()
                        break
                    else:
                        self.logger.info(
                            "[%s] Ignoring Packet: packet=%s  reason=invalid-first-packet",
                            self,
                            packet,
                        )
                elif self.is_outbound:
                    if self.status is SYN_SENT and packet.header.type is PacketType.STATE:
                        self.acker.ack(packet.header.seq_nr)
                        self._collator = DataCollator(packet.header.seq_nr + 1)
                        await self._handle_packet(packet)
                        self._send_ready.set()
                        break
                    else:
                        self.logger.info(
                            "[%s] Ignoring Packet: packet=%s  reason=invalid-first-packet",
                            self,
                            packet,
                        )
                else:
                    raise Exception("Invariant")

            async for packet in packet_receive:
                # Local packet `ack` tracking
                self.acker.ack(packet.header.seq_nr)

                # Process remote acks
                for seq_nr in packet.header.acks:
                    self._unacked_packet_buffer.pop(seq_nr, None)

                await self._handle_packet(packet)

                if self.acker.missing_seq_nr:
                    self.logger.info(
                        "[%s] Missing Packets: seq_nrs=%s",
                        self,
                        self.acker.missing_seq_nr,
                    )

                if packet.header.extensions:
                    for seq_nr in packet.header.extensions[0].get_lost(packet.header.ack_nr):
                        packet_to_resend = self._unacked_packet_buffer[seq_nr]
                        self.logger.info("[%s] Resending: packet=%s", self, packet_to_resend)
                        await self._outbound_packet_send.send(packet_to_resend)
                        break

    async def _handle_packet(self, packet: Packet) -> None:
        self.logger.debug("[%s] Handling Packet: packet=%s", self, packet)

        if packet.header.type is PacketType.DATA:
            # data packet
            if self.status in {SYN_RECEIVED, SYN_SENT}:
                self.status = CONNECTED

            if self.status in {CONNECTED, CLOSING}:
                if packet.header.seq_nr > self.max_seq_nr:
                    self.logger.debug(
                        "[%s] Ignoring Packet: packet=%s  "
                        "reason=sequent-number-out-of-bounds  max-seq=%d  "
                        "packet-seq=%d",
                        self,
                        packet,
                        self.max_seq_nr,
                        packet.header.seq_nr,
                    )
                else:
                    segment = Segment(packet.header.seq_nr, packet.data)
                    chunks = self._collator.collate(segment)

                    if chunks:
                        self.logger.debug(
                            "[%s] New Data: num_chunks=%d",
                            self,
                            len(chunks),
                        )

                    await self._data_buffer.write(chunks)

                    await self._send_packet(PacketType.STATE)
            else:
                self.logger.debug(
                    "[%s] Ignoring Packet: packet=%s  reason=invalid-state  state=%s",
                    self,
                    packet,
                    self.status,
                )
        elif packet.header.type is PacketType.FIN:
            # last packet
            self.logger.debug(
                "[%s] Closing Connection: reason=FIN",
                self,
            )
            self.status = CLOSING
            self.max_seq_nr = packet.header.seq_nr
            await self._send_packet(PacketType.STATE)
        elif packet.header.type is PacketType.STATE:
            # ack packet
            if self.status in {SYN_SENT, SYN_RECEIVED}:
                self.status = CONNECTED
        elif packet.header.type is PacketType.RESET:
            # hard termination
            self.logger.debug(
                "[%s] Closing Connection: reason=RESET",
                self,
            )
            self.status = CLOSED
            self.manager.cancel()
        elif packet.header.type is PacketType.SYN:
            # first packet over the connection
            if self.status is EMBRIO:
                self.status = SYN_RECEIVED
                await self._send_packet(PacketType.STATE)
            else:
                self.logger.debug(
                    "[%s] Ignoring Packet: packet=%s  reason=already-connected",
                    self,
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

        if self.acker.selective_acks:
            extensions = (
                SelectiveAck.from_acks(self.acker.ack_nr, self.acker.selective_acks),
            )
        else:
            extensions = ()

        if self.ack_nr is None:
            ack_nr = 0
        else:
            ack_nr = self.ack_nr

        header = PacketHeader(
            type=packet_type,
            version=1,
            extensions=extensions,
            connection_id=self.send_id,
            timestamp_microseconds=1234,
            timestamp_difference_microseconds=4321,
            wnd_size=1024,
            seq_nr=self.seq_nr,
            ack_nr=ack_nr,
        )
        packet = Packet(header, data)
        self._unacked_packet_buffer[self.seq_nr] = packet
        await self._outbound_packet_send.send(packet)

    async def _handle_outbound_packets(self) -> None:
        buffer = io.BytesIO()
        try:
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

                    await self._send_packet(PacketType.DATA, data)
        except trio.ClosedResourceError:
            pass

    async def _handle_outbound_data(self) -> None:
        buffer = io.BytesIO()

        try:
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
        except trio.ClosedResourceError:
            pass
