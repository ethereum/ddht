from typing import Tuple

from ddht.abc import EventAPI
from ddht.base_message import InboundMessage, OutboundMessage
from ddht.endpoint import Endpoint
from ddht.event import Event
from ddht.v5_1.abc import EventsAPI, SessionAPI
from ddht.v5_1.envelope import InboundEnvelope, OutboundEnvelope
from ddht.v5_1.messages import (
    FindNodeMessage,
    FoundNodesMessage,
    PingMessage,
    PongMessage,
    RegisterTopicMessage,
    RegistrationConfirmationMessage,
    TalkRequestMessage,
    TalkResponseMessage,
    TicketMessage,
    TopicQueryMessage,
)


class Events(EventsAPI):
    def __init__(self) -> None:
        self.session_created: EventAPI[SessionAPI] = Event("session.created")
        self.session_handshake_complete: EventAPI[SessionAPI] = Event(
            "session.handshake.complete"
        )
        self.session_timeout: EventAPI[SessionAPI] = Event("session.timeout")

        self.packet_sent: EventAPI[Tuple[SessionAPI, OutboundEnvelope]] = Event(
            "session.packet.sent"
        )
        self.packet_received: EventAPI[Tuple[SessionAPI, InboundEnvelope]] = Event(
            "session.packet.received"
        )
        self.packet_discarded: EventAPI[Tuple[SessionAPI, InboundEnvelope]] = Event(
            "session.packet.discarded"
        )

        self.listening: EventAPI[Endpoint] = Event("listening")

        self.external_endpoint_updated: EventAPI[Endpoint] = Event(
            "external-endpoint.updated"
        )

        self.ping_sent: EventAPI[OutboundMessage[PingMessage]] = Event(
            "messages.ping.sent"
        )
        self.ping_received: EventAPI[InboundMessage[PingMessage]] = Event(
            "messages.ping.received"
        )

        self.pong_sent: EventAPI[OutboundMessage[PongMessage]] = Event(
            "messages.pong.sent"
        )
        self.pong_received: EventAPI[InboundMessage[PongMessage]] = Event(
            "messages.pong.received"
        )

        self.find_nodes_sent: EventAPI[OutboundMessage[FindNodeMessage]] = Event(
            "messages.find_nodes.sent"
        )
        self.find_nodes_received: EventAPI[InboundMessage[FindNodeMessage]] = Event(
            "messages.find_nodes.received"
        )

        self.found_nodes_sent: EventAPI[OutboundMessage[FoundNodesMessage]] = Event(
            "messages.found_nodes.sent"
        )
        self.found_nodes_received: EventAPI[InboundMessage[FoundNodesMessage]] = Event(
            "messages.found_nodes.received"
        )

        self.talk_request_sent: EventAPI[OutboundMessage[TalkRequestMessage]] = Event(
            "messages.talk_request.sent"
        )
        self.talk_request_received: EventAPI[
            InboundMessage[TalkRequestMessage]
        ] = Event("messages.talk_request.received")

        self.talk_response_sent: EventAPI[OutboundMessage[TalkResponseMessage]] = Event(
            "messages.talk_response.sent"
        )
        self.talk_response_received: EventAPI[
            InboundMessage[TalkResponseMessage]
        ] = Event("messages.talk_response.received")

        self.register_topic_sent: EventAPI[
            OutboundMessage[RegisterTopicMessage]
        ] = Event("messages.register_topic.sent")
        self.register_topic_received: EventAPI[
            InboundMessage[RegisterTopicMessage]
        ] = Event("messages.register_topic.received")

        self.ticket_sent: EventAPI[OutboundMessage[TicketMessage]] = Event(
            "messages.ticket.sent"
        )
        self.ticket_received: EventAPI[InboundMessage[TicketMessage]] = Event(
            "messages.ticket.received"
        )

        self.registration_confirmation_sent: EventAPI[
            OutboundMessage[RegistrationConfirmationMessage]
        ] = Event("messages.registration_confirmation.sent")
        self.registration_confirmation_received: EventAPI[
            InboundMessage[RegistrationConfirmationMessage]
        ] = Event("messages.registration_confirmation.received")

        self.topic_query_sent: EventAPI[OutboundMessage[TopicQueryMessage]] = Event(
            "messages.topic_query.sent"
        )
        self.topic_query_received: EventAPI[InboundMessage[TopicQueryMessage]] = Event(
            "messages.topic_query.received"
        )
