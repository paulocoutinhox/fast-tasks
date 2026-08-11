"""the worker beside the api. nothing here imports fastapi, because the lifespan protocol is all this needs to honour"""

from fast_tasks.fastapi import lifespan_for
from fast_tasks.run import RunStatus
from fast_tasks.worker import Worker
from tests.conftest import wait_until


async def test_the_worker_comes_up_with_the_process_and_lands_the_flight_before_it_goes(app):
    seen = []

    @app.task("greet")
    async def greet(who):
        seen.append(who)

    worker = Worker(app, poll=0.01)

    async with lifespan_for(worker)(object()):
        written = await app.enqueue("greet", who="paulo")

        await wait_until(lambda: seen)

    assert seen == ["paulo"]
    assert (await app.get(written.id)).status == RunStatus.DONE
    assert worker.stopping.is_set(), "leaving the block is what tells the worker to stop"


async def test_the_store_is_built_before_the_api_serves_a_request(app):
    built = []

    async def remember():
        built.append(1)

    app.setup = remember

    async with lifespan_for(Worker(app, poll=0.01))(object()):
        assert built == [1]
