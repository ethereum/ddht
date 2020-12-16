import pytest

from ddht._utils import humanize_bytes

KB = 1024
MB = 1024 * 1024
GB = 1024 * 1024 * 1024
TB = 1024 * 1024 * 1024 * 1024
PB = 1024 * 1024 * 1024 * 1024 * 1024


@pytest.mark.parametrize(
    "num_bytes,expected",
    (
        (0, "0B"),
        (1, "1B"),
        (10, "10B"),
        (125, "125B"),
        (1023, "1023B"),
        (KB, "1KB"),
        (MB, "1MB"),
        (GB, "1GB"),
        (TB, "1TB"),
        (PB, "1PB"),
        (int(KB * 1.25), "1.25KB"),
        (int(MB * 1.25), "1.25MB"),
        (int(GB * 1.25), "1.25GB"),
        (int(TB * 1.25), "1.25TB"),
        (int(PB * 1.25), "1.25PB"),
        (int(KB * 1.2), "1.2KB"),
        (int(MB * 1.2), "1.2MB"),
        (int(GB * 1.2), "1.2GB"),
        (int(TB * 1.2), "1.2TB"),
        (int(PB * 1.2), "1.2PB"),
        (int(KB * 125), "125KB"),
        (int(MB * 125), "125MB"),
        (int(GB * 125), "125GB"),
        (int(TB * 125), "125TB"),
        (int(PB * 125), "125PB"),
    ),
)
def test_humanize_bytes(num_bytes, expected):
    actual = humanize_bytes(num_bytes)
    assert actual == expected
