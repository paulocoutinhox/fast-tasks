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
    if policy != RetryPolicy.EXPONENTIAL_JITTER:
        return min(growth_for(policy, base, attempt), ceiling)

    # the growth is held under the ceiling before the draw and never cut down to it afterwards: every run of a herd that has doubled its way past the ceiling works out the very same wait, so capping the drawn delay hands back whole the herd this policy exists to break up. drawn under the ceiling the wait still lands within it, and still lands somewhere different for each of them
    return min(growth_for(policy, base, attempt), ceiling / (1 + jitter)) * (1 + random.uniform(0, jitter))


def growth_for(policy: RetryPolicy, base: float, attempt: int) -> float:
    if policy == RetryPolicy.FIXED:
        return base

    if policy == RetryPolicy.LINEAR:
        return base * attempt

    # the first attempt waits the base delay, and every one after it doubles
    return base * (2 ** min(attempt - 1, MAX_DOUBLINGS))
