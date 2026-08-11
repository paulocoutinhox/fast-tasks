"""what a worker tells the application about every run, which is how an audit trail is kept without the app knowing what an audit trail is"""

import asyncio

from fast_tasks.errors import PermanentError
from fast_tasks.retry import RetryPolicy
from fast_tasks.worker import Worker


async def settle(worker) -> None:
    await worker.run_once()
    await worker.drain()


async def test_a_run_that_worked_is_announced_from_start_to_finish(app):
    told = []
    worker = Worker(app)

    @app.task("greet")
    async def greet(who):
        return {"greeted": who}

    @worker.on_start
    async def started(task):
        told.append(("start", task.name))

    @worker.on_finish
    async def finished(task, result, seconds):
        told.append(("finish", task.name, result, seconds >= 0))

    await app.enqueue("greet", who="paulo")
    await settle(worker)

    assert told == [("start", "greet"), ("finish", "greet", {"greeted": "paulo"}, True)]


async def test_a_run_that_broke_says_what_broke_and_whether_it_comes_back(app):
    told = []
    worker = Worker(app)

    @app.task("flaky", max_attempts=2, retry_policy=RetryPolicy.FIXED, retry_delay=0)
    async def flaky():
        raise RuntimeError("not yet")

    @worker.on_error
    async def failed(task, error, seconds, retrying):
        told.append((str(error), retrying))

    await app.enqueue("flaky")
    await settle(worker)
    await settle(worker)

    assert told == [("not yet", True), ("not yet", False)]


async def test_a_permanent_error_is_announced_as_one_that_never_comes_back(app):
    told = []
    worker = Worker(app)

    @app.task("malformed", max_attempts=9)
    async def malformed():
        raise PermanentError("the payload names no account")

    @worker.on_error
    async def failed(task, error, seconds, retrying):
        told.append(retrying)

    await app.enqueue("malformed")
    await settle(worker)

    assert told == [False]


async def test_a_listener_that_breaks_never_takes_the_run_with_it(app):
    """an audit trail failing is a thing to fix, and never a reason to lose the outcome of the work"""
    from fast_tasks.run import RunStatus

    worker = Worker(app)
    done = []

    @app.task("greet")
    async def greet():
        done.append(1)

    @worker.on_start
    async def broken_start(task):
        raise RuntimeError("the log refused")

    @worker.on_finish
    async def broken_finish(task, result, seconds):
        raise RuntimeError("the log refused again")

    written = await app.enqueue("greet")
    await settle(worker)

    assert done == [1]
    assert (await app.get(written.id)).status == RunStatus.DONE


async def test_a_worker_with_nobody_listening_runs_exactly_the_same(app):
    @app.task("greet")
    async def greet():
        return None

    worker = Worker(app)

    assert (worker.starting, worker.finishing, worker.failing) == ([], [], [])

    await app.enqueue("greet")
    await settle(worker)


async def test_every_listener_of_a_stage_is_told(app):
    told = []
    worker = Worker(app)

    @app.task("greet")
    async def greet():
        return None

    for index in range(3):
        worker.on_start(lambda task, index=index: asyncio.sleep(0) if told.append(index) is None else None)

    await app.enqueue("greet")
    await settle(worker)

    assert told == [0, 1, 2]


async def test_a_listener_is_handed_back_so_it_may_be_used_as_a_decorator(app):
    worker = Worker(app)

    async def listener(task):
        return None

    assert worker.on_start(listener) is listener
    assert worker.on_finish(listener) is listener
    assert worker.on_error(listener) is listener
