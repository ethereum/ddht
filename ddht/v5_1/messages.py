from eth_enr.sedes import ENRSedes
from rlp.sedes import Binary, CountableList, big_endian_int, binary

from ddht.base_message import BaseMessage
from ddht.message_registry import MessageTypeRegistry
from ddht.sedes import ip_address_sedes
from ddht.v5.constants import TOPIC_HASH_SIZE

topic_sedes = Binary.fixed_length(TOPIC_HASH_SIZE)


v51_registry = MessageTypeRegistry()


#
# Message types
#
@v51_registry.register
class PingMessage(BaseMessage):
    message_type = 1

    fields = (("request_id", big_endian_int), ("enr_seq", big_endian_int))


@v51_registry.register
class PongMessage(BaseMessage):
    message_type = 2

    fields = (
        ("request_id", big_endian_int),
        ("enr_seq", big_endian_int),
        ("packet_ip", ip_address_sedes),
        ("packet_port", big_endian_int),
    )


@v51_registry.register
class FindNodeMessage(BaseMessage):
    message_type = 3

    fields = (
        ("request_id", big_endian_int),
        ("distances", CountableList(big_endian_int)),
    )


@v51_registry.register
class FoundNodesMessage(BaseMessage):
    message_type = 4

    fields = (
        ("request_id", big_endian_int),
        ("total", big_endian_int),
        ("enrs", CountableList(ENRSedes)),
    )


@v51_registry.register
class TalkRequestMessage(BaseMessage):
    message_type = 5

    fields = (("request_id", big_endian_int), ("protocol", binary), ("request", binary))


@v51_registry.register
class TalkResponseMessage(BaseMessage):
    message_type = 6

    fields = (("request_id", big_endian_int), ("response", binary))


@v51_registry.register
class RegisterTopicMessage(BaseMessage):
    message_type = 7

    fields = (
        ("request_id", big_endian_int),
        ("topic", topic_sedes),
        ("enr", ENRSedes),
        ("ticket", binary),
    )


@v51_registry.register
class TicketMessage(BaseMessage):
    message_type = 8

    fields = (
        ("request_id", big_endian_int),
        ("ticket", binary),
        ("wait_time", big_endian_int),
    )


@v51_registry.register
class RegistrationConfirmationMessage(BaseMessage):
    message_type = 9

    fields = (("request_id", big_endian_int), ("topic", binary))


@v51_registry.register
class TopicQueryMessage(BaseMessage):
    message_type = 10

    fields = (("request_id", big_endian_int), ("topic", topic_sedes))
