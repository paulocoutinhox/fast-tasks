from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum

from fast_tasks.clock import now
from fast_tasks.retry import RetryPolicy


class RunStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    DONE = "done"
    FAILED = "failed"
    CANCELED = "canceled"


# a run in one of these is over, and nothing claims it again
SETTLED = frozenset({RunStatus.DONE, RunStatus.FAILED, RunStatus.CANCELED})


@dataclass
class Run:
    """one execution of one task, which is the only thing a worker ever claims — an interval and a cron differ in when the next of these is written, and in nothing else"""

    name: str
    queue: str = "default"
    payload: dict = field(default_factory=dict)

    # what makes a run unique: two callers writing the same key write one row, and that is what stops ten workers from making ten occurrences of the same minute
    key: str | None = None

    status: RunStatus = RunStatus.PENDING
    priority: int = 0
    due_at: datetime = field(default_factory=now)

    attempts: int = 0
    max_attempts: int = 1
    timeout: float | None = None
    retry_policy: RetryPolicy = RetryPolicy.EXPONENTIAL
    retry_delay: float = 5.0
    max_retry_delay: float = 3600.0

    worker: str | None = None
    lease_until: datetime | None = None

    created_at: datetime = field(default_factory=now)
    started_at: datetime | None = None
    finished_at: datetime | None = None

    result: dict | None = None
    error: str | None = None
    error_type: str | None = None

    id: int | str | None = None

    @property
    def exhausted(self) -> bool:
        return self.attempts >= self.max_attempts
