import random
from enum import StrEnum

# doubling past this is a wait no ceiling ever allows and a number a float no longer holds, so an ambitious `max_attempts` is a retry that raises instead of one that waits
MAX_DOUBLINGS = 64


class RetryPolicy(StrEnum):
    FIXED = "fixed"
    LINEAR = "linear"
    EXPONENTIAL = "exponential"
    EXPONENTIAL_JITTER = "exponential_jitter"


def delay_for(policy: RetryPolicy, base: float, attempt: int, jitter: float = 0.0, ceiling: float = 3600.0) -> float:
    """how long the attempt that just failed waits before the next one, in seconds, and never longer than the ceiling"""
    return min(growth_for(policy, base, attempt, jitter), ceiling)


def growth_for(policy: RetryPolicy, base: float, attempt: int, jitter: float) -> float:
    if policy == RetryPolicy.FIXED:
        return base

    if policy == RetryPolicy.LINEAR:
        return base * attempt

    # the first attempt waits the base delay, and every one after it doubles
    exponential = base * (2 ** min(attempt - 1, MAX_DOUBLINGS))

    if policy == RetryPolicy.EXPONENTIAL:
        return exponential

    # spread by a drawn fraction and never by a fixed one: ten thousand runs that failed on the same dead dependency work out the same delay from the same numbers, and a multiplier they all share hands the herd back whole
    return exponential * (1 + random.uniform(0, jitter))
