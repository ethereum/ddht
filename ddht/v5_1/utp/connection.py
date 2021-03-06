import enum
import io
from typing import Dict, Optional

from async_service import Service
from eth_typing import NodeID
from eth_utils import get_extended_debug_logger
import trio

from ddht.v5_1.utp.ack import AckTracker
from ddht.v5_1.utp.collator import DataCollator, Segment
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

    status: ConnectionStatus

    max_seq_nr: int = MAX_SEQ_NR

    def __init__(self,
                 node_id: NodeID,
                 send_id: ConnectionID,
                 receive_id: ConnectionID,
                 ) -> None:
        self.logger = get_extended_debug_logger('ddht.utp.Connection')

        self.state = EMBRIO

        if abs(send_id - receive_id) != 1:
            raise ValueError("Invalid connection ids")

        self.send_id = send_id
        self.receive_id = receive_id

        self.status = EMBRIO

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

        (
            self._inbound_data_send,
            self._inbound_data_receive,
        ) = trio.open_memory_channel[bytes](256)
        self._inbound_data_buffer = io.BytesIO()

        self._receive_buffer = io.BytesIO()

        self._send_lock = trio.Lock()
        self._receive_lock = trio.Lock()

    def __str__(self) -> str:
        return (
            f"Connection(send_id={self.send_id} recv_id={self.receive_id} "
            f"seq_nr={self.seq_nr} ack_nr={self.ack_nr})"
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

    async def receive_some(self, max_bytes: int) -> bytes:
        async with self._receive_lock:
            self.logger.debug2(
                "[%s] reading data: max-bytes=%d  buffered=%d",
                self,
                max_bytes,
                len(self._receive_buffer.getvalue()),
            )
            self._receive_buffer.seek(0)

            data = self._receive_buffer.read(max_bytes)

            if not data:
                data = await self._inbound_data_receive.receive()

            while True:
                try:
                    data += self._inbound_data_receive.receive_nowait()
                except trio.WouldBlock:
                    break

            if len(data) > max_bytes:
                remainder = data[max_bytes:]
                data = data[:max_bytes]

                # The only way we can end up with `remainder` data is if we read
                # new data in over the stream.  In this case, we can know that
                # we've read all information from the buffer and that any extra
                # information should be written to the front of the buffer, and the
                # buffer truncated down to the new size of however much remainder
                # data was left.
                self._receive_buffer.seek(0)
                self._receive_buffer.write(remainder)
                self._receive_buffer.truncate(len(remainder))
                self._receive_buffer.seek(0)

            return data

    async def send_all(self, data: bytes) -> None:
        async with self._send_lock:
            data_view = memoryview(data)
            for idx in range(0, len(data), MAX_PACKET_DATA):
                await self._outbound_data_send.send(data_view[idx: idx + MAX_PACKET_DATA])

    async def run(self) -> None:
        async with self._outbound_data_send:
            self.manager.run_daemon_task(self._handle_inbound_packets)
            self.manager.run_daemon_task(self._handle_outbound_packets)

            self.manager.run_daemon_task(self._handle_outbound_data)

            if self.is_outbound:
                await self._send_packet(PacketType.SYN)

            await self.manager.wait_finished()

    #
    # Long lived packet handlers
    #
    async def _handle_inbound_packets(self) -> None:
        async with self._inbound_packet_receive as packet_receive:
            async for packet in packet_receive:
                ack_nr_was_none = self.ack_nr is None
                acked = self.acker.ack(packet.header.seq_nr)

                if self.ack_nr is not None and ack_nr_was_none:
                    self.logger.debug("[%s] setting collator: packet=%s", self, packet)
                    self._collator = DataCollator(self.ack_nr + 1)

                for seq_nr in acked:
                    self._unacked_packet_buffer.pop(seq_nr, None)

                if packet.header.type is PacketType.DATA:
                    # data packet
                    if self.status is SYN_RECEIVED:
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
                            self.logger.debug(
                                "[%s] Feeding Data: packet=%s  seq_nr=%d",
                                self,
                                packet,
                                self._collator.seq_nr,
                            )
                            segment = Segment(packet.header.seq_nr, packet.data)
                            data_chunks = self._collator.collate(segment)

                            for chunk in data_chunks:
                                await self._inbound_data_send.send(segment.data)

                            await self._send_packet(PacketType.STATE)
                    else:
                        self.logger.debug(
                            "[%s] Ignoring Packet: packet=%s  reason=invalid-state  state=%s",
                            self,
                            packet,
                            self.state,
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
                    if self.state is SYN_SENT:
                        self.state = CONNECTED
                    # ack packet
                    self.logger.debug(
                        "[%s] Acking Packet: packet=%s",
                        self,
                        packet,
                    )
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

        if self.acker.acked:
            extensions = (
                SelectiveAck.from_unacked(self.ack_nr, self.acker.acked),
            )
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
            ack_nr=self.ack_nr or 0,
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
