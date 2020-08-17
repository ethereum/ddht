from typing import Type

from ddht.abc import MessageTypeRegistryAPI
from ddht.base_message import BaseMessage


class MessageTypeRegistry(MessageTypeRegistryAPI):
    def register(self, message_data_class: Type[BaseMessage]) -> Type[BaseMessage]:
        """Class Decorator to register BaseMessage classes."""
        message_type = message_data_class.message_type
        if message_type is None:
            raise ValueError("Message type must be defined")

        if message_type in self:
            raise ValueError(f"Message with type {message_type} is already defined")

        if not self:
            expected_message_type = 1
        else:
            expected_message_type = max(self.keys()) + 1

        if not message_type == expected_message_type:
            raise ValueError(
                f"Expected message type {expected_message_type}, but got {message_type}"
            )

        self[message_type] = message_data_class

        return message_data_class

    def get_message_id(self, message_data_class: Type[BaseMessage]) -> int:
        for message_id, message_type in self.items():
            if message_data_class is message_type:
                return message_id
        else:
            raise KeyError(message_data_class)
