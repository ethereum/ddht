import secrets

from eth_utils import int_to_big_endian
import pytest
from rlp.exceptions import DecodingError, DeserializationError
from rlp.sedes import binary
import trio

from ddht.base_message import BaseMessage
from ddht.tools.factories.v5_1 import PacketFactory
from ddht.v5_1.constants import SESSION_IDLE_TIMEOUT
from ddht.v5_1.messages import PingMessage


@pytest.mark.trio
async def test_session_handshake_process_steps(driver):
    initiator = driver.initiator.node
    recipient = driver.recipient.node

    assert driver.initiator.session.is_before_handshake
    assert driver.initiator.session.remote_node_id == recipient.node_id
    with pytest.raises(AttributeError, match="Session keys are not available"):
        driver.initiator.session.keys

    assert driver.recipient.session.is_before_handshake
    with pytest.raises(AttributeError, match="NodeID for remote not yet known"):
        driver.recipient.session.remote_node_id
    with pytest.raises(AttributeError, match="Session keys are not available"):
        driver.recipient.session.keys

    ping_message = await driver.initiator.send_ping()

    assert driver.initiator.session.is_during_handshake
    assert driver.recipient.session.is_before_handshake

    # transmit and process the queued messages from initiator -> recipient
    initiation_packet = await driver.transmit_one(driver.initiator)
    assert initiation_packet.packet.is_message

    assert driver.initiator.session.is_during_handshake
    assert driver.recipient.session.is_during_handshake
    assert driver.recipient.session.remote_node_id == initiator.node_id

    # let the receipient respond with the `WhoAreYouPacket`
    who_are_you = await driver.transmit_one(driver.recipient)
    assert who_are_you.packet.is_who_are_you

    assert driver.initiator.session.is_after_handshake
    assert driver.recipient.session.is_during_handshake

    handshake = await driver.transmit_one(driver.initiator)
    assert handshake.packet.is_handshake

    assert driver.initiator.session.is_after_handshake
    assert driver.recipient.session.is_after_handshake

    with trio.fail_after(1):
        message = await driver.recipient.next_message()

    assert message.message == ping_message


@pytest.mark.trio
async def test_session_message_sending_during_handshake(driver):
    assert driver.initiator.session.is_before_handshake
    assert driver.recipient.session.is_before_handshake

    # initiate the handshake
    await driver.initiator.send_ping(b"\x00")

    # send first message before initiation packet is transmitted
    # we cannot send a message from the recipient until they have the remote node id
    await driver.initiator.send_ping(b"\x01")

    assert driver.initiator.session.is_during_handshake
    assert driver.recipient.session.is_before_handshake

    # step the handshake forward
    await driver.transmit_one(driver.initiator)

    assert driver.initiator.session.is_during_handshake
    assert driver.recipient.session.is_during_handshake

    # send second message after initiation packet is transmitted
    await driver.initiator.send_ping(b"\x02")
    await driver.recipient.send_ping(b"\x03")

    # step the handshake forward
    await driver.transmit_one(driver.recipient)

    assert driver.initiator.session.is_after_handshake
    assert driver.recipient.session.is_during_handshake

    # send third message after initiation packet is transmitted
    await driver.initiator.send_ping(b"\x04")
    await driver.recipient.send_ping(b"\x05")

    # step the handshake forward
    await driver.transmit_one(driver.initiator)

    # handshake should be complete now
    assert driver.initiator.session.is_after_handshake
    assert driver.recipient.session.is_after_handshake

    async with driver.transmit():
        ping_0 = await driver.recipient.next_message()
        ping_1 = await driver.recipient.next_message()
        ping_2 = await driver.recipient.next_message()
        ping_4 = await driver.recipient.next_message()

        ping_3 = await driver.initiator.next_message()
        ping_5 = await driver.initiator.next_message()

    assert ping_0.message.request_id == b"\x00"
    assert ping_1.message.request_id == b"\x01"
    assert ping_2.message.request_id == b"\x02"
    assert ping_4.message.request_id == b"\x04"

    assert ping_3.message.request_id == b"\x03"
    assert ping_5.message.request_id == b"\x05"


@pytest.mark.trio
async def test_session_message_sending_after_handshake(driver):
    await driver.handshake()

    async with driver.transmit():
        await driver.initiator.send_ping(b"\x12")
        ping_message = await driver.recipient.next_message()
        assert ping_message.message.request_id == b"\x12"

        await driver.recipient.send_pong(b"\x12")
        pong_message = await driver.initiator.next_message()
        assert pong_message.message.request_id == b"\x12"


@pytest.mark.trio
async def test_session_unexpected_packets(driver):
    recipient = driver.recipient.node
    initiator = driver.initiator.node

    assert driver.initiator.session.is_before_handshake
    assert driver.recipient.session.is_before_handshake

    # initiate the handshake
    await driver.initiator.send_ping(b"\x12")

    assert driver.initiator.session.is_during_handshake
    assert driver.recipient.session.is_before_handshake

    with trio.fail_after(2):
        # The recipient should discard any non `MessagePacket` at this stage
        # since it is impossible for the initiator to have valid `SessionKeys`
        # since the recipient has not yet sent the `WhoAreYouPacket`
        async with driver.recipient.events.packet_discarded.subscribe_and_wait():
            await driver.send_packet(
                PacketFactory.who_are_you(dest_node_id=recipient.node_id,)
            )
        async with driver.recipient.events.packet_discarded.subscribe_and_wait():
            await driver.send_packet(
                PacketFactory.handshake(
                    source_node_id=initiator.node_id, dest_node_id=recipient.node_id,
                )
            )

        # The initiator should discard any non `WhoAreYouPacket` packet at this stage since it is
        # impossible for the recipient to have valid `SessionKeys` since the
        # initiator has not yet sent the `HandshakePacket`
        async with driver.initiator.events.packet_discarded.subscribe_and_wait():
            await driver.send_packet(
                PacketFactory.message(
                    source_node_id=recipient.node_id, dest_node_id=initiator.node_id,
                )
            )
        async with driver.initiator.events.packet_discarded.subscribe_and_wait():
            await driver.send_packet(
                PacketFactory.handshake(
                    source_node_id=recipient.node_id, dest_node_id=initiator.node_id,
                )
            )

    # Transmit the initation packet
    await driver.transmit_one(driver.initiator)

    assert driver.initiator.session.is_during_handshake
    assert driver.recipient.session.is_during_handshake

    with trio.fail_after(2):
        # The recipient should discard a WhoAreYouPacket since there is no
        # reason for the initiator to send such a packet.
        async with driver.recipient.events.packet_discarded.subscribe_and_wait():
            await driver.send_packet(
                PacketFactory.who_are_you(dest_node_id=recipient.node_id,)
            )

        # The initiator should discard a HandshakePacket since there is no
        # reason for the recipient to send such a packet
        async with driver.initiator.events.packet_discarded.subscribe_and_wait():
            await driver.send_packet(
                PacketFactory.handshake(
                    source_node_id=recipient.node_id, dest_node_id=initiator.node_id,
                )
            )

    # Transmit the who-are-you packet
    await driver.transmit_one(driver.recipient)

    assert driver.initiator.session.is_after_handshake
    assert driver.recipient.session.is_during_handshake

    with trio.fail_after(2):
        # The recipient should discard a WhoAreYouPacket since there is no
        # reason for the initiator to send such a packet.
        async with driver.recipient.events.packet_discarded.subscribe_and_wait():
            await driver.send_packet(
                PacketFactory.who_are_you(dest_node_id=recipient.node_id,)
            )

        # The recipient should buffer any message packets it receives at this
        # stage since they could be valid packets recieved out of order since
        # the initiator can now have valid session keys.
        await driver.send_packet(
            PacketFactory.message(
                aes_gcm_nonce=driver.initiator.session.get_encryption_nonce(),
                initiator_key=driver.initiator.session.keys.encryption_key,
                message=PingMessage(b"\x34", initiator.enr.sequence_number),
                source_node_id=initiator.node_id,
                dest_node_id=recipient.node_id,
            )
        )

        # The initiator should discard a HandshakePacket since there is no
        # reason for the recipient to send such a packet
        async with driver.initiator.events.packet_discarded.subscribe_and_wait():
            await driver.send_packet(
                PacketFactory.handshake(
                    source_node_id=recipient.node_id, dest_node_id=initiator.node_id,
                )
            )

    # Transmit the handshake packet
    await driver.transmit_one(driver.initiator)

    # handshake should be complete now
    assert driver.initiator.session.is_after_handshake
    assert driver.recipient.session.is_after_handshake

    async with driver.transmit():
        initiation_ping = await driver.recipient.next_message()
        out_of_order_ping = await driver.recipient.next_message()

    assert initiation_ping.message.request_id == b"\x12"
    assert out_of_order_ping.message.request_id == b"\x34"


class BadMessage(BaseMessage):
    fields = (("request_id", binary),)

    def __init__(self, message_type, request_id=None):
        if request_id is None:
            request_id = int_to_big_endian(secrets.randbits(32))
        self.message_type = message_type
        super().__init__(request_id=request_id)


@pytest.mark.trio
async def test_session_message_mismatched_rlp(driver):
    await driver.handshake()

    # Send a message that *looks* like a PingMessage
    with pytest.raises(DeserializationError):
        await driver.send_packet(
            PacketFactory.message(
                aes_gcm_nonce=driver.initiator.session.get_encryption_nonce(),
                initiator_key=driver.initiator.session.keys.encryption_key,
                message=BadMessage(1),
                source_node_id=driver.initiator.node.node_id,
                dest_node_id=driver.recipient.node.node_id,
            )
        )


@pytest.mark.trio
async def test_session_message_unknown_message_type(driver):
    await driver.handshake()

    # Send a message that *looks* like a PingMessage
    with pytest.raises(KeyError):
        await driver.send_packet(
            PacketFactory.message(
                aes_gcm_nonce=driver.initiator.session.get_encryption_nonce(),
                initiator_key=driver.initiator.session.keys.encryption_key,
                message=BadMessage(255),
                source_node_id=driver.initiator.node.node_id,
                dest_node_id=driver.recipient.node.node_id,
            )
        )


class GarbledMessage(BaseMessage):
    fields = (("request_id", binary),)

    def __init__(self, message_type, message_bytes):
        self.message_type = message_type
        self.message_bytes = message_bytes

    def to_bytes(self):
        return b"".join((int_to_big_endian(self.message_type), self.message_bytes))


@pytest.mark.trio
async def test_session_invalid_rlp(driver):
    await driver.handshake()

    # Send a message that *looks* like a PingMessage
    with pytest.raises(DecodingError):
        await driver.send_packet(
            PacketFactory.message(
                aes_gcm_nonce=driver.initiator.session.get_encryption_nonce(),
                initiator_key=driver.initiator.session.keys.encryption_key,
                message=GarbledMessage(1, b"\xff\xff\xff"),
                source_node_id=driver.initiator.node.node_id,
                dest_node_id=driver.recipient.node.node_id,
            )
        )


@pytest.mark.trio
async def test_session_is_valid_indefinitely_after_handhake(driver, autojump_clock):
    initiator = driver.initiator.session
    recipient = driver.recipient.session

    # test sessions timeout if handshake not completed
    assert initiator.is_before_handshake
    assert recipient.is_before_handshake
    assert not initiator.is_timed_out
    assert not recipient.is_timed_out

    await trio.sleep(SESSION_IDLE_TIMEOUT + 1)

    assert initiator.is_before_handshake
    assert recipient.is_before_handshake
    assert initiator.is_timed_out
    assert recipient.is_timed_out

    await driver.handshake()

    # test sessions won't timeout if handshake completed
    assert initiator.is_after_handshake
    assert recipient.is_after_handshake
    assert not initiator.is_timed_out
    assert not recipient.is_timed_out

    # let the clock advance a little
    await trio.sleep(1)

    assert not initiator.is_timed_out
    assert not recipient.is_timed_out

    # let the clock advance past the timeout
    await trio.sleep(SESSION_IDLE_TIMEOUT + 1)

    assert not initiator.is_timed_out
    assert not recipient.is_timed_out
