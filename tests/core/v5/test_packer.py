from async_service import background_trio_service
from eth.db.backends.memory import MemoryDB
import pytest
import pytest_trio
import trio

from ddht.base_message import OutboundMessage
from ddht.identity_schemes import default_identity_scheme_registry
from ddht.node_db import NodeDB
from ddht.tools.factories.discovery import (
    AuthTagPacketFactory,
    EndpointFactory,
    ENRFactory,
    HandshakeRecipientFactory,
    PingMessageFactory,
)
from ddht.tools.factories.keys import PrivateKeyFactory
from ddht.v5.channel_services import InboundPacket
from ddht.v5.messages import v5_registry
from ddht.v5.packer import Packer, PeerPacker
from ddht.v5.packets import AuthHeaderPacket, AuthTagPacket, WhoAreYouPacket
from ddht.v5.tags import compute_tag


@pytest.fixture
def private_key():
    return PrivateKeyFactory().to_bytes()


@pytest.fixture
def remote_private_key():
    return PrivateKeyFactory().to_bytes()


@pytest.fixture
def enr(private_key):
    return ENRFactory(private_key=private_key)


@pytest.fixture
def remote_enr(remote_private_key):
    return ENRFactory(private_key=remote_private_key)


@pytest.fixture
def endpoint():
    return EndpointFactory()


@pytest.fixture
def remote_endpoint():
    return EndpointFactory()


@pytest_trio.trio_fixture
async def node_db(enr, remote_enr):
    db = NodeDB(default_identity_scheme_registry, MemoryDB())
    db.set_enr(enr)
    db.set_enr(remote_enr)
    return db


@pytest.fixture
def inbound_packet_channels():
    return trio.open_memory_channel(0)


@pytest.fixture
def inbound_message_channels():
    return trio.open_memory_channel(1)


@pytest.fixture
def outbound_packet_channels():
    return trio.open_memory_channel(1)


@pytest.fixture
def outbound_message_channels():
    return trio.open_memory_channel(1)


@pytest.fixture
def remote_inbound_packet_channels():
    return trio.open_memory_channel(1)


@pytest.fixture
def remote_inbound_message_channels():
    return trio.open_memory_channel(1)


@pytest.fixture
def remote_outbound_packet_channels():
    return trio.open_memory_channel(1)


@pytest.fixture
def remote_outbound_message_channels():
    return trio.open_memory_channel(1)


@pytest_trio.trio_fixture
async def bridged_channels(
    nursery,
    endpoint,
    remote_endpoint,
    outbound_packet_channels,
    remote_outbound_packet_channels,
    inbound_packet_channels,
    remote_inbound_packet_channels,
):
    async def bridge_channels(
        outbound_packet_receive_channel, inbound_packet_send_channel
    ):
        async for outbound_packet in outbound_packet_receive_channel:
            receiver = outbound_packet.receiver_endpoint
            sender = endpoint if receiver == remote_endpoint else remote_endpoint
            inbound_packet = InboundPacket(
                packet=outbound_packet.packet, sender_endpoint=sender,
            )
            await inbound_packet_send_channel.send(inbound_packet)

    nursery.start_soon(
        bridge_channels, outbound_packet_channels[1], remote_inbound_packet_channels[0],
    )
    nursery.start_soon(
        bridge_channels, remote_outbound_packet_channels[1], inbound_packet_channels[0],
    )


@pytest_trio.trio_fixture
async def peer_packer(
    node_db,
    private_key,
    enr,
    remote_enr,
    inbound_packet_channels,
    inbound_message_channels,
    outbound_message_channels,
    outbound_packet_channels,
):
    peer_packer = PeerPacker(
        local_private_key=private_key,
        local_node_id=enr.node_id,
        remote_node_id=remote_enr.node_id,
        node_db=node_db,
        message_type_registry=v5_registry,
        inbound_packet_receive_channel=inbound_packet_channels[1],
        inbound_message_send_channel=inbound_message_channels[0],
        outbound_message_receive_channel=outbound_message_channels[1],
        outbound_packet_send_channel=outbound_packet_channels[0],
    )
    async with background_trio_service(peer_packer):
        yield peer_packer


@pytest_trio.trio_fixture
async def remote_peer_packer(
    node_db,
    remote_private_key,
    enr,
    remote_enr,
    remote_inbound_packet_channels,
    remote_inbound_message_channels,
    remote_outbound_message_channels,
    remote_outbound_packet_channels,
):
    peer_packer = PeerPacker(
        local_private_key=remote_private_key,
        local_node_id=remote_enr.node_id,
        remote_node_id=enr.node_id,
        node_db=node_db,
        message_type_registry=v5_registry,
        inbound_packet_receive_channel=remote_inbound_packet_channels[1],
        inbound_message_send_channel=remote_inbound_message_channels[0],
        outbound_message_receive_channel=remote_outbound_message_channels[1],
        outbound_packet_send_channel=remote_outbound_packet_channels[0],
    )
    async with background_trio_service(peer_packer):
        yield peer_packer


@pytest_trio.trio_fixture
async def packer(
    node_db,
    private_key,
    enr,
    inbound_packet_channels,
    inbound_message_channels,
    outbound_message_channels,
    outbound_packet_channels,
):
    packer = Packer(
        local_private_key=private_key,
        local_node_id=enr.node_id,
        node_db=node_db,
        message_type_registry=v5_registry,
        inbound_packet_receive_channel=inbound_packet_channels[1],
        inbound_message_send_channel=inbound_message_channels[0],
        outbound_message_receive_channel=outbound_message_channels[1],
        outbound_packet_send_channel=outbound_packet_channels[0],
    )
    async with background_trio_service(packer):
        yield packer


@pytest_trio.trio_fixture
async def remote_packer(
    node_db,
    remote_private_key,
    remote_enr,
    remote_inbound_packet_channels,
    remote_inbound_message_channels,
    remote_outbound_message_channels,
    remote_outbound_packet_channels,
    bridged_channels,
):
    remote_packer = Packer(
        local_private_key=remote_private_key,
        local_node_id=remote_enr.node_id,
        node_db=node_db,
        message_type_registry=v5_registry,
        inbound_packet_receive_channel=remote_inbound_packet_channels[1],
        inbound_message_send_channel=remote_inbound_message_channels[0],
        outbound_message_receive_channel=remote_outbound_message_channels[1],
        outbound_packet_send_channel=remote_outbound_packet_channels[0],
    )
    async with background_trio_service(remote_packer):
        yield packer


#
# Peer packer tests
#
@pytest.mark.trio
async def test_peer_packer_initiates_handshake(
    peer_packer, outbound_message_channels, outbound_packet_channels, nursery
):
    outbound_message = OutboundMessage(
        PingMessageFactory(), EndpointFactory(), peer_packer.remote_node_id,
    )

    outbound_message_channels[0].send_nowait(outbound_message)
    with trio.fail_after(0.5):
        outbound_packet = await outbound_packet_channels[1].receive()

    assert peer_packer.is_during_handshake
    assert outbound_packet.receiver_endpoint == outbound_message.receiver_endpoint
    assert isinstance(outbound_packet.packet, AuthTagPacket)


@pytest.mark.trio
async def test_peer_packer_sends_who_are_you(
    peer_packer, inbound_packet_channels, outbound_packet_channels, nursery
):
    inbound_packet = InboundPacket(AuthTagPacketFactory(), EndpointFactory(),)

    inbound_packet_channels[0].send_nowait(inbound_packet)
    with trio.fail_after(0.5):
        outbound_packet = await outbound_packet_channels[1].receive()

    assert peer_packer.is_during_handshake
    assert outbound_packet.receiver_endpoint == inbound_packet.sender_endpoint
    assert isinstance(outbound_packet.packet, WhoAreYouPacket)
    assert outbound_packet.packet.token == inbound_packet.packet.auth_tag


@pytest.mark.trio
async def test_peer_packer_sends_auth_header(
    peer_packer,
    enr,
    remote_private_key,
    remote_enr,
    remote_endpoint,
    inbound_packet_channels,
    outbound_packet_channels,
    outbound_message_channels,
    nursery,
):
    outbound_message = OutboundMessage(
        PingMessageFactory(), remote_endpoint, peer_packer.remote_node_id,
    )
    outbound_message_channels[0].send_nowait(outbound_message)
    with trio.fail_after(0.5):
        outbound_auth_tag_packet = await outbound_packet_channels[1].receive()

    handshake_recipient = HandshakeRecipientFactory(
        local_private_key=remote_private_key,
        local_enr=remote_enr,
        remote_private_key=peer_packer.local_private_key,
        remote_enr=enr,
        remote_node_id=peer_packer.local_node_id,
        initiating_packet_auth_tag=outbound_auth_tag_packet.packet.auth_tag,
    )
    inbound_packet = InboundPacket(
        handshake_recipient.first_packet_to_send, sender_endpoint=remote_endpoint,
    )
    inbound_packet_channels[0].send_nowait(inbound_packet)
    with trio.fail_after(0.5):
        outbound_auth_header_packet = await outbound_packet_channels[1].receive()

    assert peer_packer.is_post_handshake
    assert isinstance(outbound_auth_header_packet.packet, AuthHeaderPacket)
    assert outbound_auth_header_packet.receiver_endpoint == remote_endpoint

    handshake_result = handshake_recipient.complete_handshake(
        outbound_auth_header_packet.packet,
    )
    initiator_keys = peer_packer.session_keys
    recipient_keys = handshake_result.session_keys
    assert initiator_keys.auth_response_key == recipient_keys.auth_response_key
    assert initiator_keys.encryption_key == recipient_keys.decryption_key
    assert initiator_keys.decryption_key == recipient_keys.encryption_key


@pytest.mark.trio
async def test_full_peer_packer_handshake(
    peer_packer,
    remote_peer_packer,
    endpoint,
    remote_endpoint,
    enr,
    remote_enr,
    outbound_message_channels,
    remote_outbound_message_channels,
    inbound_message_channels,
    remote_inbound_message_channels,
    bridged_channels,
    nursery,
):
    # to remote
    outbound_message = OutboundMessage(
        message=PingMessageFactory(),
        receiver_endpoint=remote_endpoint,
        receiver_node_id=remote_enr.node_id,
    )
    outbound_message_channels[0].send_nowait(outbound_message)

    with trio.fail_after(0.5):
        inbound_message = await remote_inbound_message_channels[1].receive()

    assert inbound_message.message == outbound_message.message
    assert inbound_message.sender_endpoint == endpoint
    assert inbound_message.sender_node_id == enr.node_id

    # from remote
    outbound_message = OutboundMessage(
        message=PingMessageFactory(),
        receiver_endpoint=endpoint,
        receiver_node_id=enr.node_id,
    )
    remote_outbound_message_channels[0].send_nowait(outbound_message)

    with trio.fail_after(0.5):
        inbound_message = await inbound_message_channels[1].receive()

    assert inbound_message.message == outbound_message.message
    assert inbound_message.sender_endpoint == remote_endpoint
    assert inbound_message.sender_node_id == remote_enr.node_id


#
# Packer tests
#
@pytest.mark.trio
async def test_packer_sends_packets(
    nursery,
    packer,
    remote_enr,
    remote_endpoint,
    outbound_message_channels,
    outbound_packet_channels,
):
    assert not packer.is_peer_packer_registered(remote_enr.node_id)

    # send message
    outbound_message = OutboundMessage(
        message=PingMessageFactory(),
        receiver_endpoint=remote_endpoint,
        receiver_node_id=remote_enr.node_id,
    )
    outbound_message_channels[0].send_nowait(outbound_message)

    with trio.fail_after(0.5):
        outbound_packet = await outbound_packet_channels[1].receive()

    assert packer.is_peer_packer_registered(remote_enr.node_id)

    assert isinstance(outbound_packet.packet, AuthTagPacket)
    assert outbound_packet.receiver_endpoint == remote_endpoint


@pytest.mark.trio
async def test_packer_processes_handshake_initiation(
    nursery, packer, enr, remote_enr, remote_endpoint, inbound_packet_channels
):
    assert not packer.is_peer_packer_registered(remote_enr.node_id)

    # receive packet
    tag = compute_tag(
        source_node_id=remote_enr.node_id, destination_node_id=enr.node_id
    )
    inbound_packet = InboundPacket(
        packet=AuthTagPacketFactory(tag=tag), sender_endpoint=remote_endpoint,
    )
    await inbound_packet_channels[0].send(inbound_packet)
    await trio.sleep(0)
    assert packer.is_peer_packer_registered(remote_enr.node_id)


@pytest.mark.trio
async def test_packer_full_handshake(
    nursery,
    packer,
    remote_packer,
    enr,
    remote_enr,
    endpoint,
    remote_endpoint,
    outbound_message_channels,
    remote_outbound_message_channels,
    inbound_message_channels,
    remote_inbound_message_channels,
):
    # to remote
    outbound_message = OutboundMessage(
        message=PingMessageFactory(),
        receiver_endpoint=remote_endpoint,
        receiver_node_id=remote_enr.node_id,
    )
    outbound_message_channels[0].send_nowait(outbound_message)

    with trio.fail_after(0.5):
        inbound_message = await remote_inbound_message_channels[1].receive()

    assert inbound_message.message == outbound_message.message
    assert inbound_message.sender_endpoint == endpoint
    assert inbound_message.sender_node_id == enr.node_id

    # from remote
    outbound_message = OutboundMessage(
        message=PingMessageFactory(),
        receiver_endpoint=endpoint,
        receiver_node_id=enr.node_id,
    )
    remote_outbound_message_channels[0].send_nowait(outbound_message)

    with trio.fail_after(0.5):
        inbound_message = await inbound_message_channels[1].receive()

    assert inbound_message.message == outbound_message.message
    assert inbound_message.sender_endpoint == remote_endpoint
    assert inbound_message.sender_node_id == remote_enr.node_id
