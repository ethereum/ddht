import pytest
from ddht.v5_1.utp.ack import AckTracker


def test_ack_tracker():
    tracker = AckTracker()

    with pytest.raises(AttributeError):
        tracker.ack_nr

    tracker.ack(10)

    assert tracker.ack_nr == 10
    assert tracker.missing_seq_nr == ()
    assert tracker.selective_acks == ()

    tracker.ack(11)

    assert tracker.ack_nr == 11
    assert tracker.missing_seq_nr == ()
    assert tracker.selective_acks == ()

    tracker.ack(10)

    # no change
    assert tracker.ack_nr == 11
    assert tracker.missing_seq_nr == ()
    assert tracker.selective_acks == ()

    tracker.ack(13)

    assert tracker.ack_nr == 11
    assert tracker.missing_seq_nr == (12,)
    assert tracker.selective_acks == (13,)

    tracker.ack(14)

    assert tracker.ack_nr == 11
    assert tracker.missing_seq_nr == (12,)
    assert tracker.selective_acks == (13, 14)

    tracker.ack(12)

    assert tracker.ack_nr == 14
    assert tracker.missing_seq_nr == ()
    assert tracker.selective_acks == ()
