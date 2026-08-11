from dataclasses import dataclass
from datetime import datetime, timedelta

from fast_tasks import cron
from fast_tasks.clock import EPOCH
from fast_tasks.errors import QueueError


class Trigger:
    """what turns an instant into the next instant a recurring task is due"""

    def next_after(self, moment: datetime) -> datetime:
        raise NotImplementedError


# the finest an instant is ever kept, in every store and in the column each of them holds one in. anything under it names one slot twice and never two of them
RESOLUTION = timedelta(microseconds=1)


@dataclass(frozen=True)
class Interval(Trigger):
    """every n seconds, counted from the unix epoch so every worker of every machine names the same slot"""

    seconds: float

    def __post_init__(self) -> None:
        if self.seconds <= 0:
            raise QueueError(f"an interval of {self.seconds}s never advances")

        if self.span < RESOLUTION:
            raise QueueError(f"an interval of {self.seconds}s is finer than the microsecond a store keeps an instant to, so every slot of it is the slot before it under another name")

    @property
    def span(self) -> timedelta:
        return timedelta(seconds=self.seconds)

    def next_after(self, moment: datetime) -> datetime:
        """counted in whole slots and never in seconds: a float carries the answer back through a multiplication it cannot undo, and the slot that came out of it was the instant it was asked after — a task writing the occurrence it had already written, and never the one after"""
        return EPOCH + self.span * ((moment - EPOCH) // self.span + 1)


@dataclass(frozen=True)
class Cron(Trigger):
    """a posix expression, rounded to the minute like every cron is"""

    expression: str

    def __post_init__(self) -> None:
        cron.parse(self.expression)

    def next_after(self, moment: datetime) -> datetime:
        return cron.next_after(self.expression, moment)
