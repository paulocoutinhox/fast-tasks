"""the days nothing goes to plan: a store that is gone, a handler that takes the process with it, a deploy where two versions of the code are running at once"""

import asyncio
import logging
from datetime import timedelta

import pytest

from fast_tasks.run import Run, RunStatus
from fast_tasks.worker import Worker
from tests.conftest import wait_until


def breaks(store, name: str, rounds: int, error: Exception):
    """the store answers with a failure the first `rounds` times it is asked, and normally after that"""
    original = getattr(type(store), name)
    asked = []

    async def refusing(self, *arguments, **options):
        asked.append(1)

        if len(asked) <= rounds:
            raise error

        return await original(self, *arguments, **options)

    setattr(type(store), name, refusing)

    return original, asked


async def test_a_store_that_is_down_when_the_worker_starts_never_ends_it(app, caplog):
    """the database is not up yet, or the network is not there — the worker keeps asking and takes the work when it can"""
    ran = []

    @app.task("work")
    async def work():
        ran.append(1)

    original, asked = breaks(app.store, "reclaim", 3, ConnectionError("the store is not there"))

    try:
        await app.enqueue("work")

        with caplog.at_level(logging.ERROR):
            worker = Worker(app, poll=0.01)
            polling = asyncio.create_task(worker.run())

            await wait_until(lambda: ran)
            worker.stop()
            await polling
    finally:
        setattr(type(app.store), "reclaim", original)

    assert ran == [1], "it recovered on its own, without anybody restarting it"
    assert "the pass of" in caplog.text, "and it said so while it was down"


async def test_a_store_that_dies_after_the_work_is_done_costs_the_run_a_second_go(app, caplog):
    """the outcome never reached the store, so nothing knows the work happened — the lease is what brings it back, and this is the at-least-once boundary"""
    ran = []

    @app.task("work", max_attempts=3)
    async def work():
        ran.append(1)

    original, asked = breaks(app.store, "complete", 1, ConnectionError("the store went away"))

    try:
        written = await app.enqueue("work")

        with caplog.at_level(logging.ERROR):
            first = Worker(app, lease=timedelta(milliseconds=50))
            await first.run_once()
            await first.drain()

        # the lease of the worker that could not record anything runs out, which is the only thing that brings the run back
        await asyncio.sleep(0.08)

        assert ran == [1]
        assert "could not record the outcome" in caplog.text

        second = Worker(app)
        await second.run_once()
        await second.drain()
    finally:
        setattr(type(app.store), "complete", original)

    assert ran == [1, 1], "it ran twice, which is what at-least-once means and why a handler has to be idempotent"
    assert (await app.get(written.id)).status == RunStatus.DONE


async def test_a_handler_that_takes_the_whole_process_down_does_not_take_the_worker(app):
    """SystemExit and KeyboardInterrupt are not Exception, and a worker that let them through would stop polling"""

    @app.task("suicidal")
    async def suicidal():
        raise SystemExit(1)

    @app.task("ordinary")
    async def ordinary():
        return None

    written = await app.enqueue("suicidal")
    ordinary_run = await app.enqueue("ordinary")

    worker = Worker(app, concurrency=1, poll=0.01)
    polling = asyncio.create_task(worker.run())

    await wait_until(lambda: True, 1)
    await wait_until(lambda: True, 1)

    async def both_settled() -> bool:
        return (await app.get(written.id)).status != RunStatus.RUNNING and (await app.get(ordinary_run.id)).status == RunStatus.DONE

    await wait_until(both_settled)

    worker.stop()
    await polling

    assert (await app.get(written.id)).status == RunStatus.FAILED, "it ended as a failure instead of ending the worker"
    assert (await app.get(written.id)).error_type == "SystemExit"
    assert (await app.get(ordinary_run.id)).status == RunStatus.DONE, "the run beside it was never touched"


async def test_a_result_the_store_cannot_write_does_not_run_forever(app):
    """a handler answering something the store will not take is a bug, and it has to end as one instead of repeating the side effect for ever"""
    from fast_tasks.store.memory import MemoryStore

    if isinstance(app.store, MemoryStore):
        pytest.skip("a store that keeps the object itself has nothing to refuse")

    ran = []

    @app.task("odd", max_attempts=2)
    async def odd():
        ran.append(1)

        return {"when": object()}

    written = await app.enqueue("odd")

    for _ in range(5):
        worker = Worker(app, lease=timedelta(milliseconds=50))
        await worker.run_once()
        await worker.drain()
        await asyncio.sleep(0.08)

    settled = await app.get(written.id)

    assert settled.status == RunStatus.FAILED, "the attempts ran out and it stopped"
    assert len(ran) <= 3, "and it did not repeat the side effect for ever"


async def test_a_run_whose_task_this_worker_does_not_know(app, caplog):
    """a rolling deploy has two versions of the code running at once, and the older one meets a name it never heard of"""
    written = await app.store.add(Run(name="from_the_future", max_attempts=3))

    with caplog.at_level(logging.ERROR):
        worker = Worker(app)
        await worker.run_once()
        await worker.drain()

    settled = await app.get(written.id)

    assert settled.status == RunStatus.PENDING, "it comes back, because the worker beside this one may know it"
    assert settled.error_type == "UnknownTask"


async def test_every_attempt_spent_ends_the_run_and_stops_the_sweeping(app):
    ran = []

    @app.task("broken", max_attempts=2, retry_delay=0)
    async def broken():
        ran.append(1)

        raise RuntimeError("it never works")

    written = await app.enqueue("broken")

    for _ in range(5):
        worker = Worker(app)
        await worker.run_once()
        await worker.drain()

    assert len(ran) == 2, "it tried what it was allowed and stopped"
    assert (await app.get(written.id)).status == RunStatus.FAILED


async def test_a_worker_whose_clock_runs_ahead_does_not_take_a_run_that_is_still_working(app):
    """two machines never agree to the millisecond, and a lease is what keeps that from becoming a double execution"""
    ran = []

    @app.task("slow", max_attempts=3)
    async def slow():
        ran.append(1)
        await asyncio.sleep(0.2)

    await app.enqueue("slow")

    working = Worker(app, name="on-time", lease=timedelta(seconds=60))
    await working.run_once()

    ahead = Worker(app, name="running-ahead")

    assert await ahead.run_once() == [], "a lease of a minute is not something a few seconds of skew takes away"

    await working.drain()

    assert ran == [1]


async def test_the_store_losing_everything_does_not_end_the_workers(app):
    """somebody flushed redis, or dropped the table — the worker finds nothing and carries on"""
    ran = []

    @app.task("work")
    async def work():
        ran.append(1)

    await app.enqueue("work")

    worker = Worker(app, poll=0.01)
    polling = asyncio.create_task(worker.run())

    await wait_until(lambda: ran)

    await forget_everything(app.store)
    await asyncio.sleep(0.1)

    assert not polling.done(), "it is still polling an empty store"

    await app.enqueue("work")
    await wait_until(lambda: len(ran) == 2)

    worker.stop()
    await polling


async def forget_everything(store) -> None:
    from fast_tasks.store.memory import MemoryStore
    from fast_tasks.store.redis import RedisStore

    if isinstance(store, MemoryStore):
        store.runs.clear()

        return

    if isinstance(store, RedisStore):
        await store.client.flushdb()

        return

    from fast_tasks.store.sqlalchemy import runs

    async with store.sessions() as session:
        await session.execute(runs.delete())
        await session.commit()


@pytest.mark.parametrize("attempts", [1, 5])
async def test_a_run_is_never_taken_by_two_workers_however_many_are_reaching(app, attempts):
    """the one promise everything else rests on, asked again with a worker starting between every claim"""
    ran = []

    @app.task("once", max_attempts=attempts)
    async def once(index):
        ran.append(index)
        await asyncio.sleep(0.02)

    for index in range(12):
        await app.enqueue("once", index=index)

    workers = [Worker(app, concurrency=3, poll=0.02) for _ in range(4)]
    polling = [asyncio.create_task(worker.run()) for worker in workers]

    await wait_until(lambda: len(ran) >= 12)

    for worker in workers:
        worker.stop()

    await asyncio.gather(*polling)

    assert sorted(ran) == list(range(12))
