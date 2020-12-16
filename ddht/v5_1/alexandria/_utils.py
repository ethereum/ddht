from .constants import MAX_RADIUS


def humanize_advertisement_radius(radius: int, max_radius: int = MAX_RADIUS) -> float:
    if radius == MAX_RADIUS:
        return 1.0 * MAX_RADIUS.bit_length()

    display_whole = radius.bit_length() - 1

    remainder = radius % 2 ** display_whole
    remainder_max = 2 ** (display_whole + 1) - 2 ** display_whole

    display_fraction = remainder / remainder_max

    display_value = display_whole + display_fraction
    return display_value  # type: ignore
