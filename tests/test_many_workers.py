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

# a batch abandoned every round, so every pass of every worker has expired leases to take back while the others are claiming. the numbers are its own, and the poll is slower than everywhere else here, because every one of these is a round trip and a traced run is an order of magnitude slower than the run that chose them
CONTENDERS = 6
ROUNDS = 5
BATCH = 12


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


async def test_leases_running_out_under_a_fleet_never_hand_one_run_to_two_workers(app):
    """the reclaim of one worker and the claim of another meet on the same rows, which is where a run gets handed out twice. it is the one interleaving nothing else in the suite puts under load: a claim racing a claim is settled by one conditional write, and a claim racing a reclaim is settled by two"""
    executed = []

    @app.task("record", max_attempts=5, retry_delay=0)
    async def record(marker):
        executed.append(marker)

    workers = [Worker(app, concurrency=3, poll=0.05) for _ in range(CONTENDERS)]
    polling = [asyncio.create_task(worker.run()) for worker in workers]
    written = 0

    for _ in range(ROUNDS):
        for marker in range(written, written + BATCH):
            await app.enqueue("record", marker=marker)

        # taken by a machine that is already gone, so what every worker meets is a lease that ran out the moment it was written
        await app.store.claim("a-machine-that-died", ("default",), BATCH, timedelta(seconds=-5), now())
        written += BATCH

        await asyncio.sleep(0.1)

    await wait_until(lambda: len(executed) >= written)

    for worker in workers:
        worker.stop()

    await asyncio.gather(*polling)

    assert sorted(executed) == list(range(written)), "every run happened, and not one of them twice"
    assert await app.count(status=RunStatus.FAILED) == 0
    assert await app.count(status=RunStatus.DONE) == written


async def test_many_workers_serving_many_queues_hand_out_every_run_exactly_once(app):
    """a claim spanning several queues reads one ordering out of several places at once, and a run named by two of them is a run two workers take. the queues are served in the order they are named and the priorities across them are not, so this is the interleaving that says the merging holds while leases run out underneath it"""
    executed = []

    for queue, priority in (("default", 0), ("email", 5), ("reports", 10)):

        @app.task(f"record_{queue}", queue=queue, priority=priority, max_attempts=5, retry_delay=0)
        async def record(marker, executed=executed):
            executed.append(marker)

    served = ("default", "email", "reports")
    workers = [Worker(app, queues=served, concurrency=3, poll=0.05) for _ in range(CONTENDERS)]
    polling = [asyncio.create_task(worker.run()) for worker in workers]
    written = 0

    for _ in range(ROUNDS):
        for marker in range(written, written + BATCH):
            await app.enqueue(f"record_{served[marker % len(served)]}", marker=marker)

        # taken by a machine that is already gone, so what every worker meets is a lease that ran out the moment it was written
        await app.store.claim("a-machine-that-died", served, BATCH, timedelta(seconds=-5), now())
        written += BATCH

        await asyncio.sleep(0.1)

    await wait_until(lambda: len(executed) >= written)

    for worker in workers:
        worker.stop()

    await asyncio.gather(*polling)

    assert sorted(executed) == list(range(written)), "every run happened, and not one of them twice"
    assert await app.count(status=RunStatus.FAILED) == 0
    assert await app.count(status=RunStatus.DONE) == written


async def test_the_urgent_run_of_any_queue_is_served_before_the_ordinary_ones(app):
    """priority is what orders a claim and a queue is only where a run waits, so a worker holding a deep backlog on the queue it was told about first still takes the urgent one waiting behind it"""
    order = []

    @app.task("ordinary")
    async def ordinary(marker):
        order.append(marker)

    @app.task("urgent", queue="urgent", priority=10)
    async def urgent(marker):
        order.append(marker)

    for marker in range(20):
        await app.enqueue("ordinary", marker=f"ordinary-{marker:02d}")

    await app.enqueue("urgent", marker="urgent")

    # one at a time, so what is read is the order the store hands them out and never how fast a handler was
    worker = Worker(app, queues=("default", "urgent"), concurrency=1, poll=0.01)

    while len(order) < 21:
        await worker.run_once()
        await worker.drain()

    assert order[0] == "urgent", f"the urgent run went first, and it was served {order.index('urgent')} runs in"


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
