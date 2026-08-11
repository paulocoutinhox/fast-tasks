import pytest

from fast_tasks.retry import RetryPolicy, delay_for


def test_a_fixed_policy_waits_the_same_every_time():
    assert [delay_for(RetryPolicy.FIXED, 5, attempt) for attempt in (1, 2, 3)] == [5, 5, 5]


def test_a_linear_policy_grows_by_the_base():
    assert [delay_for(RetryPolicy.LINEAR, 5, attempt) for attempt in (1, 2, 3)] == [5, 10, 15]


def test_an_exponential_policy_doubles_and_starts_at_the_base():
    assert [delay_for(RetryPolicy.EXPONENTIAL, 5, attempt) for attempt in (1, 2, 3)] == [5, 10, 20]


def test_jitter_spreads_a_herd_that_all_failed_at_once():
    drawn = {delay_for(RetryPolicy.EXPONENTIAL_JITTER, 10, 2, jitter=0.5) for _ in range(50)}

    assert len(drawn) > 1, "a fixed multiplier hands the herd back whole, and every one of them works it out from the same numbers"
    assert min(drawn) >= 20, "and never sooner than the same policy without jitter"
    assert max(drawn) <= 30


@pytest.mark.parametrize("policy", list(RetryPolicy))
def test_no_policy_ever_asks_for_a_wait_in_the_past(policy):
    assert delay_for(policy, 5, 1) >= 0


def test_doubling_stops_at_the_ceiling():
    """five seconds and twenty attempts is a retry a month away, and nobody ever meant to ask for that"""
    assert delay_for(RetryPolicy.EXPONENTIAL, 5, 20, ceiling=3600) == 3600
    assert delay_for(RetryPolicy.LINEAR, 600, 10, ceiling=3600) == 3600


def test_a_delay_under_the_ceiling_is_left_where_it_is():
    assert delay_for(RetryPolicy.EXPONENTIAL, 5, 3, ceiling=3600) == 20
