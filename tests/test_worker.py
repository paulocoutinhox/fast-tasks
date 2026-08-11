import asyncio
import logging
from datetime import timedelta

import pytest

from fast_tasks.clock import now
from fast_tasks.errors import PermanentError, QueueError
from fast_tasks.retry import RetryPolicy
from fast_tasks.run import RunStatus
from fast_tasks.worker import Worker, worker_name
from tests.conftest import wait_until


async def settle(worker) -> None:
    await worker.run_once()
    await worker.drain()


async def test_a_worker_runs_what_it_claims_and_closes_it_as_done(app):
    seen = []

    @app.task("greet")
    async def greet(who):
        seen.append(who)

    written = await app.enqueue("greet", who="paulo")
    await settle(Worker(app))

    assert seen == ["paulo"]
    assert (await app.get(written.id)).status == RunStatus.DONE


async def test_a_dictionary_a_handler_answers_is_kept_and_anything_else_is_not(app):
    @app.task("mapped")
    async def mapped():
        return {"sent": 1}

    @app.task("other")
    async def other():
        return "not a mapping"

    mapped_run = await app.enqueue("mapped")
    other_run = await app.enqueue("other")
    await settle(Worker(app))

    assert (await app.get(mapped_run.id)).result == {"sent": 1}
    assert (await app.get(other_run.id)).result is None


async def test_a_plain_function_runs_off_the_loop_so_it_never_blocks_the_others(app):
    seen = []

    @app.task("blocking")
    def blocking(who):
        seen.append(who)

    written = await app.enqueue("blocking", who="paulo")
    await settle(Worker(app))

    assert seen == ["paulo"]
    assert (await app.get(written.id)).status == RunStatus.DONE


async def test_a_failure_comes_back_for_another_attempt_while_the_policy_allows_one(app):
    attempts = []

    @app.task("flaky", max_attempts=3, retry_policy=RetryPolicy.FIXED, retry_delay=0)
    async def flaky():
        attempts.append(1)

        if len(attempts) < 3:
            raise RuntimeError("not yet")

    written = await app.enqueue("flaky")

    for _ in range(3):
        await settle(Worker(app))

    settled = await app.get(written.id)

    assert len(attempts) == 3
    assert settled.status == RunStatus.DONE
    assert settled.attempts == 3
    assert settled.error is None, "a pass that worked clears what the one before it wrote"


async def test_a_failure_that_runs_out_of_attempts_ends_the_run_with_what_broke(app):
    @app.task("broken", max_attempts=2, retry_policy=RetryPolicy.FIXED, retry_delay=0)
    async def broken():
        raise RuntimeError("smtp refused")

    written = await app.enqueue("broken")

    for _ in range(2):
        await settle(Worker(app))

    settled = await app.get(written.id)

    assert settled.status == RunStatus.FAILED
    assert (settled.error, settled.error_type) == ("smtp refused", "RuntimeError")


async def test_a_permanent_error_is_the_last_attempt_however_many_were_allowed(app):
    attempts = []

    @app.task("malformed", max_attempts=5, retry_delay=0)
    async def malformed():
        attempts.append(1)

        raise PermanentError("the payload names no account")

    written = await app.enqueue("malformed")
    await settle(Worker(app))

    assert len(attempts) == 1
    assert (await app.get(written.id)).status == RunStatus.FAILED


async def test_an_error_with_nothing_to_say_is_recorded_by_its_kind(app):
    @app.task("silent")
    async def silent():
        raise RuntimeError()

    written = await app.enqueue("silent")
    await settle(Worker(app))

    assert (await app.get(written.id)).error == "RuntimeError"


async def test_a_job_that_runs_past_its_timeout_is_stopped_and_treated_as_a_failure(app):
    @app.task("slow", timeout=0.05, max_attempts=1)
    async def slow():
        await asyncio.sleep(5)

    written = await app.enqueue("slow")
    await settle(Worker(app))

    settled = await app.get(written.id)

    assert settled.status == RunStatus.FAILED
    assert settled.error_type == "TimeoutError"


async def test_a_worker_never_holds_more_than_it_was_told_to(app):
    started = asyncio.Event()

    @app.task("held")
    async def held():
        started.set()
        await asyncio.sleep(0.2)

    for _ in range(5):
        await app.enqueue("held")

    worker = Worker(app, concurrency=2)

    assert len(await worker.run_once()) == 2
    await started.wait()
    assert await worker.run_once() == [], "a full worker asks for nothing"

    await worker.drain()


async def test_the_lease_is_pushed_while_a_long_job_is_still_working(app):
    @app.task("long", timeout=5)
    async def long():
        await asyncio.sleep(0.25)

    written = await app.enqueue("long")
    worker = Worker(app, lease=timedelta(seconds=0.3))

    claimed = (await worker.run_once())[0]
    await worker.drain()

    assert claimed.lease_until is not None
    assert (await app.get(written.id)).status == RunStatus.DONE, "the run was never taken from under it"


async def test_a_lease_that_ran_out_is_picked_up_by_the_next_pass(app):
    """what a worker that died leaves behind, reached through the worker instead of the store"""

    @app.task("orphan", max_attempts=3)
    async def orphan():
        return None

    written = await app.enqueue("orphan")
    await app.store.claim("a-worker-that-died", ("default",), 1, timedelta(seconds=-1), now())

    assert (await app.get(written.id)).status == RunStatus.RUNNING

    await settle(Worker(app))

    assert (await app.get(written.id)).status == RunStatus.DONE


async def test_the_loop_runs_until_it_is_told_to_stop_and_then_lets_the_flight_land(app):
    seen = []

    @app.task("ticking")
    async def ticking():
        seen.append(1)

    await app.enqueue("ticking")

    worker = Worker(app, poll=0.01)
    polling = asyncio.create_task(worker.run())

    await wait_until(lambda: seen)

    worker.stop()
    await polling

    assert seen == [1]
    assert worker.running == set()


async def test_a_store_that_blinked_does_not_end_the_worker(app, monkeypatch):
    broken = []

    async def refuse(*args, **kwargs):
        broken.append(1)

        raise RuntimeError("the database went away")

    monkeypatch.setattr(type(app.store), "reclaim", refuse)

    worker = Worker(app, poll=0.01)
    polling = asyncio.create_task(worker.run())

    await wait_until(lambda: len(broken) >= 2)

    worker.stop()
    await polling

    assert len(broken) >= 2, "it kept polling after the failure"


def test_a_worker_is_told_apart_from_every_other_one_anywhere():
    first, second = worker_name(), worker_name()

    assert first != second
    assert len(first.split(":")) == 3, "host, process and a draw for a pid that came back"


async def test_a_worker_only_looks_at_the_queues_it_was_given(app):
    seen = []

    @app.task("mail", queue="email")
    async def mail():
        seen.append(1)

    await app.enqueue("mail")
    await settle(Worker(app, queues=("default",)))

    assert seen == []

    await settle(Worker(app, queues=("email",)))

    assert seen == [1]


async def test_the_heartbeat_is_never_cut_off_in_the_middle_of_talking_to_the_store(app):
    """a command interrupted halfway leaves the connection with an answer nobody read, and the next user of it waits forever"""
    inside = []

    original = type(app.store).heartbeat

    async def slow_heartbeat(self, *arguments, **options):
        inside.append(1)
        await asyncio.sleep(0.05)
        inside.pop()

        return await original(self, *arguments, **options)

    type(app.store).heartbeat = slow_heartbeat

    @app.task("quick", timeout=5)
    async def quick():
        await asyncio.sleep(0.12)

    try:
        await app.enqueue("quick")
        worker = Worker(app, lease=timedelta(seconds=0.15))

        await worker.run_once()
        await worker.drain()
    finally:
        type(app.store).heartbeat = original

    assert inside == [], "the worker waited for the beat to finish instead of cutting it off"


async def test_a_beat_that_cannot_reach_the_store_never_becomes_the_outcome_of_the_run(app):
    """the lease is what says a worker is still here, and losing it is how a run comes back — it is not how a run fails"""
    original = type(app.store).heartbeat

    async def refuse(self, *arguments, **options):
        raise ConnectionError("the store went away")

    type(app.store).heartbeat = refuse

    @app.task("quick", timeout=5)
    async def quick():
        await asyncio.sleep(0.12)

    try:
        written = await app.enqueue("quick")
        worker = Worker(app, lease=timedelta(seconds=0.15))

        await worker.run_once()
        await worker.drain()
    finally:
        type(app.store).heartbeat = original

    assert (await app.get(written.id)).status == RunStatus.DONE


async def test_an_outcome_that_never_reached_the_store_is_said_out_loud(app, caplog):
    """otherwise a store that blinked shows up as a run executed twice, with nothing anywhere saying why"""
    original = type(app.store).complete

    async def refuse(self, *arguments, **options):
        raise ConnectionError("the store went away")

    type(app.store).complete = refuse

    @app.task("greet")
    async def greet():
        return None

    try:
        written = await app.enqueue("greet")

        with caplog.at_level(logging.ERROR):
            worker = Worker(app)
            await worker.run_once()
            await worker.drain()
    finally:
        type(app.store).complete = original

    assert "could not record the outcome" in caplog.text
    assert (await app.get(written.id)).status == RunStatus.RUNNING, "the lease is what brings it back"


async def test_a_shutdown_ends_even_when_a_run_will_not(app, caplog):
    """a drain that waits forever is a deploy that never finishes, and the lease is what brings the run back anyway"""
    holding = asyncio.Event()

    @app.task("stuck")
    async def stuck():
        await holding.wait()

    await app.enqueue("stuck")
    worker = Worker(app, grace=0.1)

    await worker.run_once()

    with caplog.at_level(logging.WARNING):
        await worker.drain()

    assert "still in flight" in caplog.text

    holding.set()
    await asyncio.sleep(0)


async def test_a_drain_with_nothing_in_flight_returns_at_once(app):
    assert await Worker(app).drain() is None


def test_a_lease_that_has_already_run_out_is_refused_where_it_is_written(app):
    """it would leave the beat with nothing to wait for, and every run held until the grace ran out"""
    for lease in (timedelta(seconds=0), timedelta(seconds=-1)):
        with pytest.raises(QueueError, match="already run out"):
            Worker(app, lease=lease)


async def test_cancelling_a_run_is_never_swallowed(app):
    """a shutdown cancels what it is waiting on, and a worker that turned that into a failure would be one nobody can stop"""
    started = asyncio.Event()

    @app.task("forever")
    async def forever():
        started.set()
        await asyncio.sleep(3600)

    written = await app.enqueue("forever")
    worker = Worker(app)

    await worker.run_once()
    await started.wait()

    running = next(iter(worker.running))
    running.cancel()

    with pytest.raises(asyncio.CancelledError):
        await running

    assert running.cancelled(), "the cancellation was passed on and never turned into an outcome"
    assert (await app.get(written.id)).status == RunStatus.RUNNING, "and the lease is what brings it back"
