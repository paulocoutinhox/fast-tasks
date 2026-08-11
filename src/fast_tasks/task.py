from dataclasses import dataclass
from typing import Callable

from fast_tasks.retry import RetryPolicy
from fast_tasks.trigger import Trigger


@dataclass(frozen=True)
class Task:
    """what a name means: the code to run, the policy it runs under, the queue it belongs to, and the trigger that writes its next run when it has one"""

    name: str
    handler: Callable
    queue: str = "default"
    trigger: Trigger | None = None
    max_attempts: int = 1
    timeout: float | None = None
    retry_policy: RetryPolicy = RetryPolicy.EXPONENTIAL
    retry_delay: float = 5.0

    # doubling has no ceiling of its own: five seconds and twenty attempts is a retry a month away, which nobody meant to ask for
    max_retry_delay: float = 3600.0

    priority: int = 0
