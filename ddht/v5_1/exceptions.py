from ddht.exceptions import BaseDDHTError


class SessionNotFound(BaseDDHTError):
    pass


class ProtocolNotSupported(BaseDDHTError):
    pass
