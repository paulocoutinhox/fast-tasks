"""ten workers in one process reaching for the same runs, which is the race the conditional write exists to settle"""

import asyncio
from datetime import timedelta

from fast_tasks.app import FastTasks
from fast_tasks.clock import now
from fast_tasks.run import RunStatus
from fast_tasks.worker import Worker
from tests.conftest import wait_until

RUNS = 40
WORKERS = 10


async def test_every_run_is_executed_exactly_once_however_many_workers_reach_for_it(app):
    executed = []

    @app.task("record")
    async def record(marker):
        executed.append(marker)

    for marker in range(RUNS):
        await app.enqueue("record", marker=marker)

    workers = [Worker(app, concurrency=4, poll=0.01) for _ in range(WORKERS)]
    polling = [asyncio.create_task(worker.run()) for worker in workers]

    await wait_until(lambda: len(executed) >= RUNS)

    for worker in workers:
        worker.stop()

    await asyncio.gather(*polling)

    assert sorted(executed) == list(range(RUNS)), "each run happened, and none of them twice"
    assert await app.count(status=RunStatus.DONE) == RUNS


async def test_ten_workers_beating_at_once_write_one_run_a_slot(store):
    """each worker builds the same app from the same code, which is what ten processes of one deployment are"""

    async def tick():
        return None

    workers = []

    for _ in range(WORKERS):
        # an hour between slots, so what is counted is the writing and never how long the test took
        built = FastTasks(store)
        built.task("tick", every=3600)(tick)
        workers.append(Worker(built, poll=0.05))

    for _ in range(20):
        await asyncio.gather(*[worker.run_once() for worker in workers])

    for worker in workers:
        await worker.drain()

    assert await store.count() == 1, "one slot, one run, however many of them wrote it"


async def test_a_worker_that_dies_holding_a_run_does_not_take_it_with_him(app):
    executed = []

    @app.task("record", max_attempts=3)
    async def record(marker):
        executed.append(marker)

    await app.enqueue("record", marker="one")

    # a worker claims it and is never heard from again, which is what a lease that runs out means
    dying = Worker(app)
    claimed = await dying.app.store.claim(dying.name, ("default",), 1, timedelta(seconds=-1), now())

    assert len(claimed) == 1
    assert executed == []

    survivor = Worker(app)
    await survivor.run_once()
    await survivor.drain()

    assert executed == ["one"]
