import pytest
import trio

from ddht.tools.driver import Network
from ddht.v5_1.messages import PingMessage

INITIATOR_PRIVATE_KEY = b"\x01" * 32
RECIPIENT_PRIVATE_KEY = b"\x02" * 32


@pytest.mark.trio
async def test_session_handshake_process():
    network = Network()
    initiator = network.node()
    recipient = network.node()

    driver = network.session_pair(initiator, recipient)

    assert driver.initiator.session.is_before_handshake
    assert driver.initiator.session.remote_node_id == recipient.node_id
    with pytest.raises(AttributeError, match="Session keys are not available"):
        driver.initiator.session.keys

    assert driver.recipient.session.is_before_handshake
    with pytest.raises(AttributeError, match="NodeID for remote not yet known"):
        driver.recipient.session.remote_node_id
    with pytest.raises(AttributeError, match="Session keys are not available"):
        driver.recipient.session.keys

    ping_message = PingMessage(1234, initiator.enr.sequence_number)

    await driver.initiator.send_message(ping_message),

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
