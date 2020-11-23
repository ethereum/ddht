import collections

from ddht._utils import weighted_choice


def test_weighted_choice():
    for _ in range(100):
        options = ("a", "b", "c")
        results = tuple(weighted_choice(options) for _ in range(10000))
        counts = collections.Counter(results)
        count_a = counts["a"]
        count_b = counts["b"]
        count_c = counts["c"]

        assert count_a < count_b < count_c

        # `c` should be piced 3x as often as `a`
        # `b` should be piced 2x as often as `a`
        assert abs(3 - count_c / count_a) < 1.50
        assert abs(2 - count_b / count_a) < 1.50
