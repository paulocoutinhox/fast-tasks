from datetime import datetime, timedelta, timezone

import pytest

from fast_tasks.errors import CronError, QueueError
from fast_tasks.trigger import Cron, Interval, Trigger


def at(*parts) -> datetime:
    return datetime(*parts, tzinfo=timezone.utc)


def test_an_interval_names_a_slot_and_not_an_offset_from_whoever_asked():
    """two workers asking at two instants of the same slot have to get the same answer, or they write two rows"""
    every_thirty = Interval(30)

    assert every_thirty.next_after(at(2026, 1, 1, 10, 0, 5)) == every_thirty.next_after(at(2026, 1, 1, 10, 0, 29))


def test_an_interval_advances_past_the_slot_it_just_named():
    assert Interval(30).next_after(at(2026, 1, 1, 10, 0, 30)) == at(2026, 1, 1, 10, 1)


def test_an_interval_accepts_a_timedelta_worth_of_seconds():
    assert Interval(timedelta(minutes=1).total_seconds()).next_after(at(2026, 1, 1, 10, 0, 1)) == at(2026, 1, 1, 10, 1)


@pytest.mark.parametrize("seconds", [0, -1])
def test_an_interval_that_never_advances_is_refused(seconds):
    with pytest.raises(QueueError, match="never advances"):
        Interval(seconds)


def test_a_cron_is_parsed_when_it_is_declared_and_not_on_the_night_it_would_run():
    with pytest.raises(CronError):
        Cron("nonsense")


def test_a_cron_answers_the_next_matching_minute():
    assert Cron("0 4 * * *").next_after(at(2026, 1, 1, 10, 0)) == at(2026, 1, 2, 4, 0)


def test_the_base_trigger_states_nothing_and_says_so():
    with pytest.raises(NotImplementedError):
        Trigger().next_after(at(2026, 1, 1))
