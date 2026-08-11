from datetime import datetime, timedelta, timezone

from fast_tasks.clock import EPOCH, as_utc, naive_utc, now


def test_now_is_always_aware_utc():
    moment = now()

    assert moment.tzinfo is timezone.utc


def test_a_naive_value_is_read_as_utc():
    assert as_utc(datetime(2026, 1, 1, 12)) == datetime(2026, 1, 1, 12, tzinfo=timezone.utc)


def test_a_value_of_another_zone_is_converted_and_not_relabelled():
    tokyo = datetime(2026, 1, 1, 21, tzinfo=timezone(timedelta(hours=9)))

    assert as_utc(tokyo) == datetime(2026, 1, 1, 12, tzinfo=timezone.utc)


def test_nothing_stays_nothing():
    assert as_utc(None) is None
    assert naive_utc(None) is None


def test_what_reaches_a_driver_carries_no_offset():
    written = naive_utc(datetime(2026, 1, 1, 12, tzinfo=timezone.utc))

    assert written.tzinfo is None
    assert written == datetime(2026, 1, 1, 12)


def test_the_epoch_is_the_shared_origin_every_machine_counts_from():
    assert EPOCH == datetime(1970, 1, 1, tzinfo=timezone.utc)
