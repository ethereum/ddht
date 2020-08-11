from typing import Tuple

from ddht.abc import EventAPI
from ddht.base_message import InboundMessage, OutboundMessage
from ddht.endpoint import Endpoint
from ddht.event import Event
from ddht.v5_1.abc import EventsAPI, SessionAPI
from ddht.v5_1.envelope import InboundEnvelope
from ddht.v5_1.messages import PingMessage, PongMessage


class Events(EventsAPI):
    def __init__(self) -> None:
        self.session_created: EventAPI[SessionAPI] = Event("session.created")
        self.session_handshake_complete: EventAPI[SessionAPI] = Event(
            "session.handshake.complete"
        )
        self.session_timeout: EventAPI[SessionAPI] = Event("session.timeout")

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
