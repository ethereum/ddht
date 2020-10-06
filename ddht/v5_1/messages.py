from typing import cast

from eth_enr.sedes import ENRSedes
import rlp
from rlp.sedes import Binary, CountableList, big_endian_int, binary

from ddht.base_message import BaseMessage
from ddht.encryption import aesgcm_decrypt
from ddht.message_registry import MessageTypeRegistry
from ddht.sedes import ip_address_sedes
from ddht.typing import AES128Key, Nonce

topic_sedes = Binary.fixed_length(32)


v51_registry = MessageTypeRegistry()


#
# Message types
#
@v51_registry.register
class PingMessage(BaseMessage):
    message_type = 1

    fields = (("request_id", binary), ("enr_seq", big_endian_int))


@v51_registry.register
class PongMessage(BaseMessage):
    message_type = 2

    fields = (
        ("request_id", binary),
        ("enr_seq", big_endian_int),
        ("packet_ip", ip_address_sedes),
        ("packet_port", big_endian_int),
    )


@v51_registry.register
class FindNodeMessage(BaseMessage):
    message_type = 3

    fields = (
        ("request_id", binary),
        ("distances", CountableList(big_endian_int)),
    )


@v51_registry.register
class FoundNodesMessage(BaseMessage):
    message_type = 4

    fields = (
        ("request_id", binary),
        ("total", big_endian_int),
        ("enrs", CountableList(ENRSedes)),
    )


@v51_registry.register
class TalkRequestMessage(BaseMessage):
    message_type = 5

    protocol: bytes
    payload: bytes

    fields = (("request_id", binary), ("protocol", binary), ("payload", binary))


@v51_registry.register
class TalkResponseMessage(BaseMessage):
    message_type = 6

    payload: bytes

    fields = (("request_id", binary), ("payload", binary))


@v51_registry.register
class RegisterTopicMessage(BaseMessage):
    message_type = 7

    fields = (
        ("request_id", binary),
        ("topic", topic_sedes),
        ("enr", ENRSedes),
        ("ticket", binary),
    )


@v51_registry.register
class TicketMessage(BaseMessage):
    message_type = 8

    fields = (
        ("request_id", binary),
        ("ticket", binary),
        ("wait_time", big_endian_int),
    )


@v51_registry.register
class RegistrationConfirmationMessage(BaseMessage):
    message_type = 9

    fields = (("request_id", binary), ("topic", binary))


@v51_registry.register
class TopicQueryMessage(BaseMessage):
    message_type = 10

    fields = (("request_id", binary), ("topic", topic_sedes))


def decode_message(
    decryption_key: AES128Key,
    aes_gcm_nonce: Nonce,
    message_cipher_text: bytes,
    authenticated_data: bytes,
    message_type_registry: MessageTypeRegistry = v51_registry,
) -> BaseMessage:
    message_plain_text = aesgcm_decrypt(
        key=decryption_key,
        nonce=aes_gcm_nonce,
        cipher_text=message_cipher_text,
        authenticated_data=authenticated_data,
    )
    message_type = message_plain_text[0]
    message_sedes = message_type_registry[message_type]
    message = rlp.decode(message_plain_text[1:], sedes=message_sedes)

    return cast(BaseMessage, message)
