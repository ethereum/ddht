from typing import Any, Collection

from eth_utils import ValidationError


def validate_length(value: Collection[Any], length: int, title: str = "Value") -> None:
    if not len(value) == length:
        raise ValidationError(
            "{title} must be of length {0}.  Got {1} of length {2}".format(
                length, value, len(value), title=title,
            )
        )


def validate_length_lte(
    value: Collection[Any], maximum_length: int, title: str = "Value"
) -> None:
    if len(value) > maximum_length:
        raise ValidationError(
            "{title} must be of length less than or equal to {0}.  "
            "Got {1} of length {2}".format(
                maximum_length, value, len(value), title=title,
            )
        )
