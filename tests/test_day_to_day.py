"""what a deployment actually does on an ordinary week, driven end to end through workers"""

import asyncio
from datetime import datetime, timedelta, timezone

from fast_tasks.run import RunStatus
from fast_tasks.worker import Worker
from tests.conftest import wait_until


async def test_a_deploy_lands_what_is_in_flight_and_loses_nothing(app):
    """stop, wait, come back: the runs that were going finished, and the ones that were waiting are still waiting"""
    done = []

    @app.task("work")
    async def work(index):
        await asyncio.sleep(0.05)
        done.append(index)

    for index in range(6):
        await app.enqueue("work", index=index)

    leaving = Worker(app, concurrency=2, poll=0.01)
    polling = asyncio.create_task(leaving.run())

    await wait_until(lambda: len(leaving.running) == 2)
    leaving.stop()
    await polling

    assert len(done) == 2, "what it had taken, it finished"
    assert await app.count(status=RunStatus.PENDING) == 4, "what it had not taken is untouched"

    arriving = Worker(app, concurrency=4, poll=0.01)
    coming = asyncio.create_task(arriving.run())

    await wait_until(lambda: len(done) == 6)
    arriving.stop()
    await coming

    assert sorted(done) == list(range(6)), "every run happened, and none of them twice"


async def test_a_worker_added_in_the_middle_picks_up_what_is_waiting(app):
    done = []

    @app.task("work")
    async def work(index):
        await asyncio.sleep(0.02)
        done.append(index)

    for index in range(12):
        await app.enqueue("work", index=index)

    workers = [Worker(app, concurrency=2, poll=0.01)]
    polling = [asyncio.create_task(workers[0].run())]

    await wait_until(lambda: len(done) >= 2)

    workers.append(Worker(app, concurrency=2, poll=0.01))
    polling.append(asyncio.create_task(workers[1].run()))

    await wait_until(lambda: len(done) == 12)

    for worker in workers:
        worker.stop()

    await asyncio.gather(*polling)

    assert sorted(done) == list(range(12))


async def test_a_slow_queue_never_holds_up_a_fast_one(app):
    """which is the whole reason a task names a queue"""
    order = []

    @app.task("heavy", queue="heavy")
    async def heavy():
        await asyncio.sleep(0.3)
        order.append("heavy")

    @app.task("light", queue="light")
    async def light():
        order.append("light")

    await app.enqueue("heavy")
    await app.enqueue("light")

    slow = Worker(app, queues=("heavy",), poll=0.01)
    quick = Worker(app, queues=("light",), poll=0.01)
    polling = [asyncio.create_task(slow.run()), asyncio.create_task(quick.run())]

    await wait_until(lambda: order)

    assert order == ["light"], "the light one went first, with the heavy one still going"

    await wait_until(lambda: len(order) == 2)

    for worker in (slow, quick):
        worker.stop()

    await asyncio.gather(*polling)


async def test_a_full_worker_takes_nothing_more_until_a_slot_frees(app):
    started = []
    holding = asyncio.Event()

    @app.task("held")
    async def held():
        started.append(1)
        await holding.wait()

    for _ in range(4):
        await app.enqueue("held")

    worker = Worker(app, concurrency=2, poll=0.01)
    polling = asyncio.create_task(worker.run())

    await wait_until(lambda: len(started) == 2)
    await asyncio.sleep(0.05)

    assert len(started) == 2, "it never reached for a third with two in hand"
    assert await app.count(status=RunStatus.PENDING) == 2

    holding.set()

    await wait_until(lambda: len(started) == 4)
    worker.stop()
    await polling


async def test_a_time_already_past_runs_at_once_and_one_ahead_waits(app):
    ran = []

    @app.task("later")
    async def later(when):
        ran.append(when)

    await app.enqueue_at("later", datetime.now(timezone.utc) - timedelta(hours=1), payload={"when": "past"})
    await app.enqueue_at("later", datetime.now(timezone.utc) + timedelta(hours=1), payload={"when": "future"})

    worker = Worker(app)
    await worker.run_once()
    await worker.drain()

    assert ran == ["past"]
    assert await app.count(status=RunStatus.PENDING) == 1


async def test_a_payload_arrives_the_way_it_left(app):
    """it makes a trip through json, and a queue that quietly changes an argument is worse than one that refuses it"""
    seen = []

    @app.task("carry")
    async def carry(**payload):
        seen.append(payload)

    sent = {"text": "café, naïve, 日本語", "nested": {"a": [1, 2, {"b": None}]}, "empty": {}, "zero": 0, "flag": False}
    await app.enqueue("carry", **sent)

    worker = Worker(app)
    await worker.run_once()
    await worker.drain()

    assert seen == [sent]


async def test_a_task_with_no_arguments_at_all_runs(app):
    ran = []

    @app.task("bare")
    async def bare():
        ran.append(1)

    await app.enqueue("bare")

    worker = Worker(app)
    await worker.run_once()
    await worker.drain()

    assert ran == [1]


async def test_priority_is_served_before_age_through_a_worker(app):
    order = []

    @app.task("work")
    async def work(who):
        order.append(who)

    for index in range(5):
        await app.enqueue("work", who=f"ordinary-{index}")

    await app.enqueue("work", priority=10, who="urgent")

    worker = Worker(app, concurrency=1)
    await worker.run_once()
    await worker.drain()

    assert order == ["urgent"]


async def test_a_task_may_ask_for_another_one(app):
    """the everyday shape of a pipeline, and the second one is claimed like anything else"""
    done = []

    @app.task("first")
    async def first():
        await app.enqueue("second")
        done.append("first")

    @app.task("second")
    async def second():
        done.append("second")

    await app.enqueue("first")

    worker = Worker(app, poll=0.01)
    polling = asyncio.create_task(worker.run())

    await wait_until(lambda: len(done) == 2)
    worker.stop()
    await polling

    assert done == ["first", "second"]


async def test_two_callers_asking_for_the_same_key_at_once_leave_one_run(app):
    @app.task("welcome")
    async def welcome(account):
        return None

    written = await asyncio.gather(*[app.enqueue("welcome", key="welcome:7", account=7) for _ in range(10)])

    assert len({run.id for run in written}) == 1
    assert await app.count() == 1
