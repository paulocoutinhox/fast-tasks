from datetime import datetime, timedelta, timezone
from typing import Callable

from fast_tasks.clock import as_utc, now
from fast_tasks.errors import QueueError, UnknownTask
from fast_tasks.retry import RetryPolicy
from fast_tasks.run import Run, RunStatus
from fast_tasks.store.base import KEY_LIMIT, QUEUE_LIMIT, TASK_NAME_LIMIT, Store
from fast_tasks.task import Task
from fast_tasks.trigger import Cron, Interval, Trigger


def occurrence_key(name: str, moment: datetime) -> str:
    """what makes one slot of a recurring task a single run, computed the same way by every worker of every machine"""
    return f"{name}@{moment.isoformat()}"


# the widest instant a slot is ever named for, which is what a key is measured against. an interval of half a second lands on microseconds every other slot where one on the minute never does, so the slot a task wants next says nothing about the longest key it will come to write
WIDEST_SLOT = datetime(9999, 12, 31, 23, 59, 59, 999999, tzinfo=timezone.utc)


def within(value: str, limit: int, what: str) -> None:
    """every store sizes the column it keeps a name in, and there is nothing further down that can tell a value is too long for it. a database in strict mode refuses the write, and one that is not quietly cuts the value short — which for a key is two occurrences of two different minutes ending up as one run"""
    if len(value) > limit:
        raise QueueError(f"{what} is {len(value)} characters and a store keeps {limit} of them, so the write is one the database refuses or quietly cuts short")


def keyed(key: str) -> None:
    """what a key has to be for a store to tell one run from another by it. a key of no characters is the one value the stores cannot agree on: a column takes it as a name held once, and redis reads it as no key at all — so the same call folds every enqueue into one row on a database and writes a row every time on redis"""
    if not key:
        raise QueueError("a key of no characters names nothing, and what makes a run single is the name no other run may take")

    within(key, KEY_LIMIT, f"the key '{key}'")


def trigger_for(every: float | timedelta | None, cron: str | None) -> Trigger | None:
    if every is not None:
        return Interval(every.total_seconds() if isinstance(every, timedelta) else every)

    return Cron(cron) if cron is not None else None


class FastTasks:
    """what an application holds: the tasks it knows, and the store where their runs live"""

    def __init__(self, store: Store) -> None:
        self.store = store
        self.tasks: dict[str, Task] = {}

        # the slot each recurring task was last asked to write, so a poll every second is not an insert every second
        self.written: dict[str, datetime] = {}

    def register(self, task: Task) -> Task:
        if task.name in self.tasks:
            raise QueueError(f"'{task.name}' is registered twice, and a name has to mean one thing")

        within(task.name, TASK_NAME_LIMIT, f"the name of '{task.name}'")
        within(task.queue, QUEUE_LIMIT, f"the queue '{task.queue}' of '{task.name}'")

        # a recurring task writes its own key on every poll, so the longest one it will ever write is what says whether it fits. left to the worker, this is a task whose every pass raises before anything is claimed, on every worker of the fleet, for as long as the deployment lives
        if task.trigger is not None:
            within(occurrence_key(task.name, WIDEST_SLOT), KEY_LIMIT, f"the key each slot of '{task.name}' is written under")

        self.tasks[task.name] = task

        return task

    def task(self, name: str, *, queue: str = "default", every: float | timedelta | None = None, cron: str | None = None, max_attempts: int = 1, timeout: float | None = None, retry_policy: RetryPolicy = RetryPolicy.EXPONENTIAL, retry_delay: float = 5.0, max_retry_delay: float = 3600.0, priority: int = 0) -> Callable:
        """declares a task. with neither `every` nor `cron` it runs when somebody enqueues it, and with one of them it also runs on its own"""
        if every is not None and cron is not None:
            raise QueueError(f"'{name}' asks for an interval and a cron, and a task runs on one clock")

        if max_attempts < 1:
            raise QueueError(f"'{name}' allows {max_attempts} attempts, and a run has to be tried at least once")

        if timeout is not None and timeout <= 0:
            raise QueueError(f"'{name}' has a timeout of {timeout}s, which stops every run before it starts")

        if retry_delay < 0:
            raise QueueError(f"'{name}' waits {retry_delay}s before another attempt, and a wait is never negative")

        if max_retry_delay <= 0:
            raise QueueError(f"'{name}' waits at most {max_retry_delay}s before another attempt, which is a retry due before the one that failed and a queue that hammers instead of backing off")

        def declare(handler: Callable) -> Callable:
            self.register(Task(name=name, handler=handler, queue=queue, trigger=trigger_for(every, cron), max_attempts=max_attempts, timeout=timeout, retry_policy=retry_policy, retry_delay=retry_delay, max_retry_delay=max_retry_delay, priority=priority))

            return handler

        return declare

    def task_for(self, name: str) -> Task:
        if name not in self.tasks:
            raise UnknownTask(f"nothing here is called '{name}'")

        return self.tasks[name]

    async def setup(self) -> None:
        await self.store.setup()

    def build(self, task: Task, due_at: datetime, payload: dict, key: str | None, priority: int | None) -> Run:
        """the instant is settled here and never in a store: a datetime with no zone is the utc one it reads as, and three stores each deciding that for themselves is the same value naming three instants"""
        if key is not None:
            keyed(key)

        return Run(name=task.name, queue=task.queue, payload=payload, key=key, due_at=as_utc(due_at), max_attempts=task.max_attempts, timeout=task.timeout, retry_policy=task.retry_policy, retry_delay=task.retry_delay, max_retry_delay=task.max_retry_delay, priority=task.priority if priority is None else priority)

    async def store_once(self, run: Run) -> Run:
        """a key already taken is not a failure: the caller is handed the run that got there first"""
        written = await self.store.add(run)

        if written is not None:
            return written

        return await self.store.find(run.key)

    async def enqueue(self, name: str, /, *, key: str | None = None, priority: int | None = None, payload: dict | None = None, **shorthand) -> Run:
        """runs as soon as a worker is free, which is what an email leaving a request wants"""
        return await self.enqueue_at(name, now(), key=key, priority=priority, payload=payload, **shorthand)

    async def enqueue_at(self, name: str, when: datetime, /, *, key: str | None = None, priority: int | None = None, payload: dict | None = None, **shorthand) -> Run:
        """runs once, at a stated instant. a worker that was down when it came due picks it up as soon as it is back"""
        return await self.store_once(self.build(self.task_for(name), when, self.payload_of(payload, shorthand), key, priority))

    def payload_of(self, payload: dict | None, shorthand: dict) -> dict:
        """keywords are the short way to say it, and `payload` is the way to say anything — a task whose argument is called `key` has to be reachable too"""
        if payload is None:
            return shorthand

        if shorthand:
            raise QueueError("a payload was given whole and in pieces at the same time, and only one of them can be the arguments")

        return payload

    async def materialize(self, moment: datetime | None = None) -> list[Run]:
        """writes the next slot of every recurring task. every worker does this and the key is what leaves one run, so nothing here elects a leader"""
        moment = moment or now()
        written = []

        for task in self.tasks.values():
            if task.trigger is None:
                continue

            # the slot this worker wrote last is still ahead, so there is nothing to write and nothing to work out: asking the trigger first would walk a yearly expression minute by minute on every poll, which is a third of a core spent answering the same thing
            ahead = self.written.get(task.name)

            if ahead is not None and ahead > moment:
                continue

            due_at = task.trigger.next_after(moment)
            slot = await self.store.add(self.build(task, due_at, {}, occurrence_key(task.name, due_at), None))

            # remembered only once the store has it: marking it first would let a store that blinked drop this slot for good, and a daily task would lose a whole day in silence
            self.written[task.name] = due_at

            if slot is not None:
                written.append(slot)

        return written

    async def cancel(self, run_id) -> bool:
        return await self.store.cancel(run_id, now())

    async def get(self, run_id) -> Run | None:
        return await self.store.get(run_id)

    async def find(self, key: str) -> Run | None:
        return await self.store.find(key)

    async def count(self, status: RunStatus | None = None, queue: str | None = None) -> int:
        return await self.store.count(status, queue)
