import random

from ddht.kademlia import at_log_distance, compute_log_distance
from ddht.tools.factories.node_id import NodeIDFactory


def test_at_log_distance():
    for i in range(10000):
        node = NodeIDFactory()
        distance = random.randint(1, 256)
        other = at_log_distance(node, distance)
        actual = compute_log_distance(node, other)
        assert actual == distance
