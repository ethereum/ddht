class BaseDDHTError(Exception):
    """
    The base class for all Discovery-DHT errors.
    """

    pass


class DecodingError(BaseDDHTError):
    """
    Raised when a datagram could not be decoded.
    """

    pass


class DecryptionError(BaseDDHTError):
    """
    Raised when a message could not be decrypted.
    """

    pass


class HandshakeFailure(BaseDDHTError):
    """
    Raised when the protocol handshake was unsuccessful.
    """

    pass


class UnexpectedMessage(BaseDDHTError):
    """
    Raised when the received message was unexpected.
    """

    pass


class DuplicateProtocol(BaseDDHTError):
    """
    Raised when attempting to register a TALK protocol when one is already registered.
    """

    pass


class EmptyFindNodesResponse(BaseDDHTError):
    """
    Raised when we ask a remote node for its ENR and it returns nothing.
    """

    pass
