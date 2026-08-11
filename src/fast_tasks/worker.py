import asyncio
import inspect
import logging
import os
import random
import socket
from datetime import datetime, timedelta
from time import monotonic
from typing import Callable
from uuid import uuid4

from fast_tasks.app import FastTasks
from fast_tasks.clock import now
from fast_tasks.errors import PermanentError, QueueError, UnknownTask
from fast_tasks.retry import delay_for
from fast_tasks.run import Run
from fast_tasks.store.base import WORKER_NAME_LIMIT

logger = logging.getLogger(__name__)

# the lease is pushed well before it runs out, so a slow run never has its run taken from under it
HEARTBEAT_SHARE = 3

# pruning is a housekeeping write and not the work, so it happens on the hour and never on the poll
PURGE_EVERY = 3600.0

# how many settled runs one pruning drops, so a year that was never pruned is caught up over some passes instead of in one statement that holds the table
PURGE_LIMIT = 1000


def worker_name() -> str:
    """a worker is told apart from every other one anywhere: the host names the machine, the pid names the process, and the draw covers a pid the system handed out again"""
    tail = f":{os.getpid()}:{uuid4().hex[:8]}"

    # a pod is named after its deployment, its namespace and its cluster, which is long past what a store keeps a worker name in — and the draw is what tells two machines apart once the domain is cut off the end
    return socket.gethostname()[: WORKER_NAME_LIMIT - len(tail)] + tail


class Worker:
    """claims what is due and runs it. any number of these may run against one store, on one machine or on twenty"""

    def __init__(self, app: FastTasks, *, name: str | None = None, queues: tuple[str, ...] = ("default",), concurrency: int = 8, poll: float = 1.0, lease: timedelta = timedelta(seconds=60), jitter: float = 0.25, grace: float = 30.0, keep: timedelta | None = timedelta(days=7)) -> None:
        self.app = app
        self.name = name or worker_name()
        self.queues = queues
        self.concurrency = concurrency
        self.poll = poll
        self.lease = lease
        self.jitter = jitter
        self.grace = grace
        self.keep = keep

        # each of these fails silently rather than loudly when it is wrong: a poll of zero spins the process, and a concurrency of zero is a worker that claims nothing and says nothing
        if lease.total_seconds() <= 0:
            raise QueueError(f"a lease of {lease.total_seconds()}s is one that has already run out, and a worker cannot hold a run for it")

        if poll <= 0:
            raise QueueError(f"a poll of {poll}s is not a wait, and a worker that does not wait spends a core asking")

        if concurrency < 1:
            raise QueueError(f"a concurrency of {concurrency} is a worker that never takes anything")

        if grace < 0:
            raise QueueError(f"a grace of {grace}s is not a span to wait for what is in flight")

        if jitter < 0:
            raise QueueError(f"a jitter of {jitter} draws a delay shorter than the policy asked for, and one far enough below it is a retry due in the past")

        if keep is not None and keep.total_seconds() < 0:
            raise QueueError(f"keeping a run for {keep.total_seconds()}s asks for what is over to be dropped before it is over, which is all of it")

        if len(self.name) > WORKER_NAME_LIMIT:
            raise QueueError(f"a name of {len(self.name)} characters does not fit the {WORKER_NAME_LIMIT} a store keeps for one, and a claim the database refuses is a worker that polls forever and takes nothing")

        # drawn, so ten workers coming up together do not all reach for the same rows in the same instant — the same reason a retry delay is drawn
        self.purged: float | None = monotonic() - random.uniform(0, PURGE_EVERY)
        self.running: set[asyncio.Task] = set()
        self.stopping = asyncio.Event()
        self.starting: list[Callable] = []
        self.finishing: list[Callable] = []
        self.failing: list[Callable] = []

    def on_start(self, listener: Callable) -> Callable:
        """called with the run, before its handler runs"""
        self.starting.append(listener)

        return listener

    def on_finish(self, listener: Callable) -> Callable:
        """called with the run, what the handler answered and how long it took"""
        self.finishing.append(listener)

        return listener

    def on_error(self, listener: Callable) -> Callable:
        """called with the run, what broke, how long it took, and whether it comes back for another attempt"""
        self.failing.append(listener)

        return listener

    async def announce(self, listeners: list[Callable], *arguments) -> None:
        """a listener that breaks breaks alone: an audit trail that fails must never take the outcome of the run with it"""
        for listener in listeners:
            try:
                outcome = listener(*arguments)

                # a plain function is a listener too, and the same ambiguity a handler already answers for: `await` on what it returned is the error nobody reads
                if inspect.isawaitable(outcome):
                    await outcome
            except Exception:
                logger.exception("[fast_tasks] a listener of %s failed", self.name)

    @property
    def free(self) -> int:
        return self.concurrency - len(self.running)

    async def run_once(self) -> list[Run]:
        """one pass: what a dead worker left goes back, what is due is written, and what this worker can hold is claimed and started"""
        moment = now()

        await self.app.store.reclaim(moment)
        await self.tidy(moment)
        await self.app.materialize(moment)

        if self.free <= 0:
            return []

        claimed = await self.app.store.claim(self.name, self.queues, self.free, self.lease, moment)

        for run in claimed:
            self.spawn(run)

        return claimed

    async def tidy(self, moment: datetime) -> int:
        """a queue nobody prunes grows for ever, and what it grows by is rows nothing reads again"""
        if self.keep is None:
            return 0

        if self.purged is not None and monotonic() - self.purged < PURGE_EVERY:
            return 0

        try:
            gone = await self.app.store.purge(moment - self.keep, PURGE_LIMIT)
        except Exception:
            # housekeeping is never the work: a pruning the store refused must not cost this pass the claim it was on its way to make
            logger.exception("[fast_tasks] %s could not prune what is over", self.name)
            self.purged = monotonic()

            return 0

        # a full batch means there is more of it, so the next pass takes the next one instead of waiting the hour out: a year that was never pruned would take a year of hours to catch up on
        self.purged = None if gone == PURGE_LIMIT else monotonic()

        return gone

    def spawn(self, run: Run) -> None:
        running = asyncio.create_task(self.execute(run))
        self.running.add(running)
        running.add_done_callback(self.running.discard)

    async def drain(self) -> None:
        """waits for what this worker started, and says what it gave up on. one bounded wait and never a loop over the set: a task is taken out of it by a callback the loop runs later, so asking again as soon as the wait returns spins, and ten workers spinning starve the very callbacks they are waiting for"""
        if not self.running:
            return

        done, pending = await asyncio.wait(tuple(self.running), timeout=self.grace)

        if pending:
            logger.warning("[fast_tasks] %s stopped with %s runs still in flight, and their leases are what bring them back", self.name, len(pending))

    async def run(self) -> None:
        """polls until `stop`, then lets what is in flight land"""
        while not self.stopping.is_set():
            try:
                await self.run_once()
            except Exception:
                # a store that blinked must not end the worker, or one bad minute stops every run forever
                logger.exception("[fast_tasks] the pass of %s failed", self.name)

            await self.wait()

        await self.drain()

    async def wait(self) -> None:
        try:
            await asyncio.wait_for(self.stopping.wait(), timeout=self.poll)
        except TimeoutError:
            return None

    def stop(self) -> None:
        self.stopping.set()

    async def execute(self, run: Run) -> None:
        resting = asyncio.Event()
        beat = asyncio.create_task(self.beat(run, resting))

        try:
            await self.attempt(run)
        except asyncio.CancelledError:
            raise
        except BaseException:
            # the outcome never reached the store, so nothing on either side knows what became of this run: the lease is what brings it back, and this line is the only place that says why it came
            logger.exception("[fast_tasks] %s could not record the outcome of %s", self.name, run.name)
        finally:
            # the beat is asked to stop and then waited for, never cancelled: a command interrupted halfway leaves the connection it was using with an answer nobody read, and whoever takes that connection next waits for a reply that already went somewhere else
            resting.set()
            await asyncio.gather(beat, return_exceptions=True)

    async def attempt(self, run: Run) -> None:
        started = monotonic()

        await self.announce(self.starting, run)

        try:
            result = await self.call(run)
        except asyncio.CancelledError:
            # cancellation is the shutdown asking, and swallowing it would leave a worker that cannot be stopped
            raise
        except UnknownTask as error:
            # nothing was attempted: the name belongs to the code beside this worker, which is what a rolling deploy is. failing it here destroys a run that is perfectly good, and `max_attempts` is one by default — so this one never counts against it
            await self.broke(run, error, started, await self.release(run, error))
        except PermanentError as error:
            await self.broke(run, error, started, await self.settle(run, error, retryable=False))
        except Exception as error:
            await self.broke(run, error, started, await self.settle(run, error, retryable=True))
        except BaseException as error:
            # asyncio never swallows SystemExit or KeyboardInterrupt inside a task: it hands them to the loop, which ends the whole worker and every run it was holding. one handler calling sys.exit must not do that, and it is not something another attempt fixes either
            logger.exception("[fast_tasks] %s asked the process to stop, and it was refused", run.name)
            await self.broke(run, error, started, await self.settle(run, error, retryable=False))
        else:
            answered = result if isinstance(result, dict) else None

            if self.recorded(run, await self.app.store.complete(run.id, self.name, now(), answered)):
                await self.announce(self.finishing, run, answered, monotonic() - started)

    async def broke(self, run: Run, error: BaseException, started: float, coming_back: bool | None) -> None:
        """tells the listeners how an attempt ended, unless the store would not take that ending"""
        if coming_back is None:
            return

        await self.announce(self.failing, run, error, monotonic() - started, coming_back)

    def recorded(self, run: Run, taken: bool) -> bool:
        """whether the store took the outcome. it refuses when the lease ran out and somebody else took the run over, and announcing an outcome the store threw away puts a run in an audit trail twice — once here and once under whoever holds it now"""
        if taken:
            return True

        logger.warning("[fast_tasks] %s no longer held %s when it was over, so the outcome was dropped and whoever holds it now answers for the run", self.name, run.name)

        return False

    async def call(self, run: Run):
        """a handler may be a coroutine or a plain function, and a plain one runs off the loop so it never blocks the others"""
        answer = self.settled(self.invoke(run))

        if run.timeout is None:
            return await answer

        return await asyncio.wait_for(answer, timeout=run.timeout)

    def invoke(self, run: Run):
        handler = self.app.task_for(run.name).handler

        return handler(**run.payload) if inspect.iscoroutinefunction(handler) else asyncio.to_thread(handler, **run.payload)

    async def settled(self, answer):
        """a callable object with an async `__call__`, or a decorator whose wrapper is plain, does not look like a coroutine function and still answers a coroutine — awaiting only the thread would close the run as done with the work never started"""
        outcome = await answer

        return await outcome if inspect.isawaitable(outcome) else outcome

    async def release(self, run: Run, error: Exception) -> bool | None:
        """hands a run back to the queue with the attempt given back, because this worker never had anything to try — and a spent attempt is what a reclaim reads as a run with nothing left"""
        taken = await self.app.store.release(run.id, self.name, now() + timedelta(seconds=self.waiting(run)), str(error), type(error).__name__)

        return True if self.recorded(run, taken) else None

    def waiting(self, run: Run) -> float:
        return delay_for(run.retry_policy, run.retry_delay, run.attempts, self.jitter, run.max_retry_delay)

    async def settle(self, run: Run, error: BaseException, retryable: bool) -> bool | None:
        """what a failed attempt becomes: another attempt while the policy allows one, and the end of the run when it does not. answers whether it comes back, and nothing at all when the store would not take the outcome"""
        description = str(error) or type(error).__name__
        kind = type(error).__name__
        coming_back = retryable and not run.exhausted

        if coming_back:
            taken = await self.app.store.retry_later(run.id, self.name, now() + timedelta(seconds=self.waiting(run)), description, kind)
        else:
            taken = await self.app.store.fail(run.id, self.name, now(), description, kind)

        return coming_back if self.recorded(run, taken) else None

    async def beat(self, run: Run, resting: asyncio.Event) -> None:
        """the lease belongs to the worker for as long as it is working, and this is what keeps saying so — it only ever stops between two of them, and never inside one"""
        period = self.lease.total_seconds() / HEARTBEAT_SHARE

        while not await self.resting_within(resting, period):
            await self.push(run)

    async def resting_within(self, resting: asyncio.Event, period: float) -> bool:
        """the event is read before the wait, because `wait_for` with nothing left to wait cancels without ever looking at it"""
        if resting.is_set():
            return True

        try:
            return await asyncio.wait_for(resting.wait(), timeout=period)
        except TimeoutError:
            return False

    async def push(self, run: Run) -> None:
        """a beat that could not reach the store is not fatal, and it must never become the outcome of the run: the lease is what says this worker is still here, and losing it is exactly how the run comes back"""
        try:
            await self.app.store.heartbeat(run.id, self.name, self.lease, now())
        except Exception:
            logger.exception("[fast_tasks] the beat of %s could not reach the store", run.name)
