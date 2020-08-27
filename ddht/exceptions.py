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


class OldSequenceNumber(Exception):
    """
    Raised when trying to update an ENR record with a sequence number that is
    older than the latest sequence number we have seen
    """

    pass


class UnknownIdentityScheme(Exception):
    """
    Raised when trying to instantiate an ENR with an unknown identity scheme
    """

    pass
