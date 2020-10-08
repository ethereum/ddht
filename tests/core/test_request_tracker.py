from ddht.request_tracker import RequestTracker
from ddht.tools.factories.node_id import NodeIDFactory


def test_request_tracker_reserve_request_id_generated():
    tracker = RequestTracker()

    node_id = NodeIDFactory()

    with tracker.reserve_request_id(node_id) as request_id:
        assert tracker.is_request_id_active(node_id, request_id)
    assert not tracker.is_request_id_active(node_id, request_id)


def test_request_tracker_reserve_request_id_provided():
    tracker = RequestTracker()

    node_id = NodeIDFactory()

    request_id = b"\x01\x02\x03\04"

    assert not tracker.is_request_id_active(node_id, request_id)

    with tracker.reserve_request_id(node_id, request_id) as actual_request_id:
        assert actual_request_id == request_id
        assert tracker.is_request_id_active(node_id, request_id)
    assert not tracker.is_request_id_active(node_id, request_id)
