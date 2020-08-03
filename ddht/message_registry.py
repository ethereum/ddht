from collections import UserDict
from typing import TYPE_CHECKING, Type

from ddht.base_message import BaseMessage

# https://github.com/python/mypy/issues/5264#issuecomment-399407428
if TYPE_CHECKING:
    MessageTypeRegistryBaseType = UserDict[int, Type[BaseMessage]]
else:
    MessageTypeRegistryBaseType = UserDict


class MessageTypeRegistry(MessageTypeRegistryBaseType):
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
