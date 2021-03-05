from ddht.v5_1.utp.ack import AckTracker


def test_ack_tracker():
    tracker = AckTracker()

    assert tracker.ack_nr == 0
    assert not tracker.acked

    res_a = tracker.ack(0)
    assert res_a == (0,)
    assert tracker.ack_nr == 1

    res_b = tracker.ack(1)
    assert res_b == (1,)
    assert tracker.ack_nr == 2

    assert not tracker.ack(0)

    res_c = tracker.ack(3)
    assert res_c == (3,)

    res_d = tracker.ack(4)
    assert res_d == (3, 4)

    res_e = tracker.ack(6)
    assert res_e == (3, 4, 6)

    assert tracker.ack_nr == 2

    res_f = tracker.ack(2)
    assert res_f == (2, 3, 4, 6)

    assert tracker.ack_nr == 5
    assert tracker.acked == (6,)

    res_g = tracker.ack(7)
    assert res_g == (6, 7)

    res_h = tracker.ack(5)
    assert res_h == (5, 6, 7)

    assert tracker.ack_nr == 8
    assert tracker.acked == ()

    assert not tracker.ack(0)
