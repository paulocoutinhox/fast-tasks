from datetime import datetime, timezone

import pytest

from fast_tasks.cron import next_after, parse
from fast_tasks.errors import CronError


def at(*parts) -> datetime:
    return datetime(*parts, tzinfo=timezone.utc)


def test_every_minute_is_the_next_minute():
    assert next_after("* * * * *", at(2026, 1, 1, 10, 0, 30)) == at(2026, 1, 1, 10, 1)


def test_the_answer_is_strictly_after_so_a_match_never_repeats_itself():
    assert next_after("* * * * *", at(2026, 1, 1, 10, 0)) == at(2026, 1, 1, 10, 1)


def test_a_daily_expression_crosses_into_tomorrow():
    assert next_after("0 4 * * *", at(2026, 1, 1, 10, 0)) == at(2026, 1, 2, 4, 0)


def test_a_list_takes_the_nearest_of_its_values():
    assert next_after("0,30 * * * *", at(2026, 1, 1, 10, 5)) == at(2026, 1, 1, 10, 30)


def test_a_step_walks_the_range():
    assert next_after("*/15 * * * *", at(2026, 1, 1, 10, 1)) == at(2026, 1, 1, 10, 15)


def test_a_range_is_bounded_at_both_ends():
    assert next_after("0 9-17 * * *", at(2026, 1, 1, 18, 0)) == at(2026, 1, 2, 9, 0)


def test_sunday_is_written_seven_and_read_zero():
    assert 0 in parse("* * * * 7")[4]
    assert 7 not in parse("* * * * 7")[4]


def test_a_weekday_expression_finds_the_weekday():
    # 2026-01-01 is a thursday, so the next monday is the fifth
    assert next_after("0 0 * * 1", at(2026, 1, 1, 10, 0)) == at(2026, 1, 5, 0, 0)


def test_day_of_month_and_day_of_week_are_joined_by_or_when_both_are_restricted():
    """posix says so, and a job that runs on the 1st or on any monday is the reason"""
    assert next_after("0 0 1 * 1", at(2026, 1, 1, 10, 0)) == at(2026, 1, 5, 0, 0)


def test_a_restricted_day_alone_is_an_and_with_the_open_weekday():
    assert next_after("0 0 15 * *", at(2026, 1, 1, 10, 0)) == at(2026, 1, 15, 0, 0)


def test_february_the_thirtieth_never_comes():
    with pytest.raises(CronError, match="none of the months it names ever has"):
        next_after("0 0 30 2 *", at(2026, 1, 1))


def test_the_twenty_ninth_of_february_is_the_next_leap_year_and_not_an_error():
    """it is further away than a year, and a search that only looked a year ahead refused an expression that is perfectly good"""
    assert next_after("0 0 29 2 *", at(2026, 3, 1)) == at(2028, 2, 29, 0, 0)


def test_the_century_that_is_not_a_leap_year_is_looked_past():
    """1900 and 2100 are divisible by four and are not leap years, which is what makes the longest gap between two leap days eight years and not four"""
    assert next_after("0 0 29 2 *", at(2096, 3, 1)) == at(2104, 2, 29, 0, 0)


def test_a_day_whose_minutes_are_all_behind_moves_on_to_the_next_hour():
    """the minute is past every one this hour offers, and the answer is the first one the hour after it offers — not the next day"""
    assert next_after("0 * * * *", at(2026, 1, 1, 12, 34)) == at(2026, 1, 1, 13, 0)


def test_the_search_gives_up_rather_than_running_for_ever(monkeypatch):
    """nothing that parses reaches this, and the bound is what says so: an expression the matcher could never satisfy has to end the call instead of the process"""
    monkeypatch.setattr("fast_tasks.cron.HORIZON", 0)

    with pytest.raises(CronError, match="matches nothing"):
        next_after("0 4 * * *", at(2026, 1, 1))


@pytest.mark.parametrize("expression,reason", [("* * * *", "5 fields"), ("* * * * * *", "5 fields"), ("bad * * * *", "not a number"), ("*/0 * * * *", "never advances"), ("*/x * * * *", "not a number"), ("99 * * * *", "outside"), ("30-10 * * * *", "outside"), ("0 0 0 * *", "outside")])
def test_an_expression_that_cannot_mean_anything_is_refused_where_it_is_written(expression, reason):
    with pytest.raises(CronError, match=reason):
        parse(expression)
