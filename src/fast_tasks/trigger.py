from dataclasses import dataclass
from datetime import datetime, timedelta

from fast_tasks import cron
from fast_tasks.clock import EPOCH
from fast_tasks.errors import QueueError


class Trigger:
    """what turns an instant into the next instant a recurring task is due"""

    def next_after(self, moment: datetime) -> datetime:
        raise NotImplementedError


@dataclass(frozen=True)
class Interval(Trigger):
    """every n seconds, counted from the unix epoch so every worker of every machine names the same slot"""

    seconds: float

    def __post_init__(self) -> None:
        if self.seconds <= 0:
            raise QueueError(f"an interval of {self.seconds}s never advances")

    def next_after(self, moment: datetime) -> datetime:
        elapsed = (moment - EPOCH).total_seconds()

        return EPOCH + timedelta(seconds=(int(elapsed // self.seconds) + 1) * self.seconds)


@dataclass(frozen=True)
class Cron(Trigger):
    """a posix expression, rounded to the minute like every cron is"""

    expression: str

    def __post_init__(self) -> None:
        cron.parse(self.expression)

    def next_after(self, moment: datetime) -> datetime:
        return cron.next_after(self.expression, moment)
