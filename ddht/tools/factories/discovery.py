from eth_enr.identity_schemes import V4IdentityScheme
from eth_enr.tools.factories import ENRFactory
import factory

from ddht.base_message import InboundMessage
from ddht.v5.channel_services import InboundPacket
from ddht.v5.constants import (
    AUTH_SCHEME_NAME,
    ID_NONCE_SIZE,
    MAGIC_SIZE,
    NONCE_SIZE,
    TAG_SIZE,
)
from ddht.v5.endpoint_tracker import EndpointVote
from ddht.v5.handshake import HandshakeInitiator, HandshakeRecipient
from ddht.v5.messages import FindNodeMessage, PingMessage
from ddht.v5.packets import AuthHeader, AuthHeaderPacket, AuthTagPacket, WhoAreYouPacket
from ddht.v5.typing import Topic

from .endpoint import EndpointFactory
from .node_id import NodeIDFactory


class AuthTagPacketFactory(factory.Factory):  # type: ignore
    class Meta:
        model = AuthTagPacket

    tag = b"\x00" * TAG_SIZE
    auth_tag = b"\x00" * NONCE_SIZE
    encrypted_message = b"\x00" * 10


class AuthHeaderFactory(factory.Factory):  # type: ignore
    class Meta:
        model = AuthHeader

    auth_tag = b"\x00" * NONCE_SIZE
    id_nonce = b"\x00" * ID_NONCE_SIZE
    auth_scheme_name = AUTH_SCHEME_NAME
    ephemeral_public_key = b"\x00" * 32
    encrypted_auth_response = b"\x00" * 10


class AuthHeaderPacketFactory(factory.Factory):  # type: ignore
    class Meta:
        model = AuthHeaderPacket

    tag = b"\x00" * TAG_SIZE
    auth_header = factory.SubFactory(AuthHeaderFactory)
    encrypted_message = b"\x00" * 10


class WhoAreYouPacketFactory(factory.Factory):  # type: ignore
    class Meta:
        model = WhoAreYouPacket

    magic = b"\x00" * MAGIC_SIZE
    token = b"\x00" * NONCE_SIZE
    id_nonce = b"\x00" * ID_NONCE_SIZE
    enr_sequence_number = 0


class EndpointVoteFactory(factory.Factory):  # type: ignore
    class Meta:
        model = EndpointVote

    endpoint = factory.SubFactory(EndpointFactory)
    node_id = factory.LazyFunction(lambda: ENRFactory().node_id)
    timestamp = factory.Faker("unix_time")


class InboundPacketFactory(factory.Factory):  # type: ignore
    class Meta:
        model = InboundPacket

    packet = factory.SubFactory(AuthTagPacketFactory)
    sender_endpoint = factory.SubFactory(EndpointFactory)


class PingMessageFactory(factory.Factory):  # type: ignore
    class Meta:
        model = PingMessage

    request_id = factory.Faker("pyint", min_value=0, max_value=100)
    enr_seq = factory.Faker("pyint", min_value=0, max_value=100)


class FindNodeMessageFactory(factory.Factory):  # type: ignore
    class Meta:
        model = FindNodeMessage

    request_id = factory.Faker("pyint", min_value=0, max_value=100)
    distance = factory.Faker("pyint", min_value=0, max_value=32)


class InboundMessageFactory(factory.Factory):  # type: ignore
    class Meta:
        model = InboundMessage

    message = factory.SubFactory(PingMessageFactory)
    sender_endpoint = factory.SubFactory(EndpointFactory)
    sender_node_id = factory.SubFactory(NodeIDFactory)


class HandshakeInitiatorFactory(factory.Factory):  # type: ignore
    class Meta:
        model = HandshakeInitiator

    local_private_key = factory.Faker(
        "binary", length=V4IdentityScheme.private_key_size
    )
    local_enr = factory.LazyAttribute(
        lambda o: ENRFactory(private_key=o.local_private_key)
    )
    remote_enr = factory.LazyAttribute(
        lambda o: ENRFactory(private_key=o.remote_private_key)
    )
    initial_message = factory.SubFactory(PingMessageFactory)

    class Params:
        remote_private_key = factory.Faker(
            "binary", length=V4IdentityScheme.private_key_size
        )


class HandshakeRecipientFactory(factory.Factory):  # type: ignore
    class Meta:
        model = HandshakeRecipient

    local_private_key = factory.Faker(
        "binary", length=V4IdentityScheme.private_key_size
    )
    local_enr = factory.LazyAttribute(
        lambda o: ENRFactory(private_key=o.local_private_key)
    )
    remote_enr = factory.LazyAttribute(
        lambda o: ENRFactory(private_key=o.remote_private_key)
    )
    remote_node_id = factory.LazyAttribute(lambda o: o.remote_enr.node_id)
    initiating_packet_auth_tag = factory.Faker("binary", length=NONCE_SIZE)

    class Params:
        remote_private_key = factory.Faker(
            "binary", length=V4IdentityScheme.private_key_size
        )


class TopicFactory(factory.Factory):  # type: ignore
    class Meta:
        model = Topic
        inline_args = ("topic",)

    topic = factory.Faker("binary", length=32)
