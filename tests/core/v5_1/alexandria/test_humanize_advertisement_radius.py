import pytest

from ddht.v5_1.alexandria._utils import humanize_advertisement_radius


@pytest.mark.parametrize(
    "radius,expected", ((16, 4.00), (14, 3.75), (12, 3.50), (10, 3.25), (8, 3.00),),
)
def test_humanize_max_radius(radius, expected):
    actual = humanize_advertisement_radius(radius, 16)
    assert actual == expected
