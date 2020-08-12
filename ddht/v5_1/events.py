from ddht.abc import EventAPI
from ddht.event import Event
from ddht.v5_1.abc import EventsAPI, SessionAPI


class Events(EventsAPI):
    def __init__(self) -> None:
        self.session_created: EventAPI[SessionAPI] = Event("session.created")
        self.session_handshake_complete: EventAPI[SessionAPI] = Event(
            "session.handshake.complete"
        )
