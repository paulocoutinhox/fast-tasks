"""many machines against one real server, with leases running out under them the whole time. this is the load nothing else in the suite puts on a store: a claim racing a claim is settled by one conditional write, and a claim racing a reclaim is settled by two, on rows both of them are already holding"""

import asyncio
import json
import os
import subprocess
import sys
from collections import Counter
from datetime import timedelta
from pathlib import Path

import pytest
import pytest_asyncio

from fast_tasks.clock import now
from fast_tasks.run import RunStatus
from fast_tasks.worker import Worker
from tests.conftest import PORTS, SERVERS, reachable, wait_until
from tests.fleet import QUEUES, TASKS, build_queue, empty

pytestmark = pytest.mark.stress

MACHINES = 6
BATCH = 40
ROUNDS = 10
SECONDS = 15.0

# workers sharing one process, which is the cheaper half of this file and the one that carries the most pressure per second. its numbers are smaller than the fleet's on purpose: past this much sustained contention innodb rolls transactions back faster than any bounded retry budget can absorb, and what a store refuses there is a lease handing the run back — at least once, which is what this library promises and not what this asks
PRESSING = 10
PRESSED_BATCH = 20
PRESSED_ROUNDS = 6

# short, so a machine that stops answering is one the others take over from inside the run and not after it
LEASE = 4.0

# the claim of the machine that is already gone spends one, and what is left is what the fleet needs to finish the run
ATTEMPTS = 5

# generous enough for a loaded runner, and short enough that a machine that never starts is a failure and not a hang
PATIENCE = 180.0

ROOT = Path(__file__).resolve().parent.parent

# a real server only: the point is many processes against one store, which a file and a dictionary are not
STRESSED = [name for name in ("redis", "mysql", "postgres") if reachable(SERVERS[name], PORTS[name])]


@pytest_asyncio.fixture(params=STRESSED)
async def fleet(request, tmp_path):
    url = SERVERS[request.param]
    await empty(url)

    output = tmp_path / "done"
    output.mkdir()

    app, closing = build_queue(url, str(output), ATTEMPTS)
    await app.setup()

    yield app, url, output

    await closing()


def start_machines(url: str, output: Path, count: int) -> list:
    """the output of every machine is kept, so one that never starts says why instead of failing in silence"""
    environment = os.environ | {"PYTHONPATH": str(ROOT)}
    logs = [(output.parent / f"machine-{index}.log").open("w") for index in range(count)]
    settings = {"url": url, "output": str(output), "seconds": SECONDS, "attempts": ATTEMPTS, "lease": LEASE, "concurrency": 4, "queues": list(QUEUES)}

    return [(subprocess.Popen([sys.executable, "-m", "tests.machine", json.dumps(settings)], cwd=ROOT, env=environment, stdout=log, stderr=subprocess.STDOUT), log) for log in logs]


def told(output: Path) -> str:
    return "\n".join(path.read_text(errors="ignore") for path in sorted(output.parent.glob("machine-*.log")))


def executions(output: Path) -> list[str]:
    return [path.name for path in output.iterdir() if path.name.startswith("run-")]


async def abandon(app, written: int, batch: int, queues: tuple[str, ...] = ("default",)) -> int:
    """a batch written and taken by a machine that is already gone, so every pass of every machine has expired leases to take back while the others are claiming"""
    for marker in range(written, written + batch):
        await app.enqueue(TASKS[marker % len(queues)], marker=f"run-{marker:05d}")

    await app.store.claim("a-machine-that-died", queues, batch, timedelta(seconds=-LEASE), now())

    return written + batch


async def test_a_fleet_under_leases_running_out_hands_every_run_to_exactly_one_machine(fleet):
    app, url, output = fleet
    machines = start_machines(url, output, MACHINES)
    written = 0

    for _ in range(ROUNDS):
        written = await abandon(app, written, BATCH)

        await asyncio.sleep(0.4)

    for process, log in machines:
        code = process.wait(timeout=PATIENCE)
        log.close()

        assert code == 0, f"a machine ended with {code}:\n{told(output)}"

    # the machines are gone, and whatever their leases still hold is taken back and finished by one survivor
    survivor = Worker(app, concurrency=8, poll=0.05, lease=timedelta(seconds=30))
    polling = asyncio.create_task(survivor.run())

    await wait_until(lambda: len(executions(output)) >= written)

    survivor.stop()
    await polling

    done = Counter(name.rsplit(".", 2)[0] for name in executions(output))

    assert sorted(done) == [f"run-{marker:05d}" for marker in range(written)], f"every run was executed:\n{told(output)}"
    assert [marker for marker, count in done.items() if count > 1] == [], "and not one of them twice"
    assert await app.count(status=RunStatus.FAILED) == 0
    assert await app.count(status=RunStatus.RUNNING) == 0

    # the work really was shared, and did not all land on whoever started first
    assert len({name.rsplit(".", 2)[1] for name in executions(output)}) > 1


async def test_a_fleet_serving_many_queues_still_hands_every_run_to_exactly_one_machine(fleet):
    """a claim spans every queue a machine serves and every priority inside them, which is one ordering read out of several places at once. it is the path the fleet above never walks: one queue at one priority asks nothing of the merging, and a run named by two lanes at once is a run two machines run"""
    app, url, output = fleet
    machines = start_machines(url, output, MACHINES)
    written = 0

    for _ in range(ROUNDS):
        written = await abandon(app, written, BATCH, QUEUES)

        await asyncio.sleep(0.4)

    for process, log in machines:
        code = process.wait(timeout=PATIENCE)
        log.close()

        assert code == 0, f"a machine ended with {code}:\n{told(output)}"

    survivor = Worker(app, queues=QUEUES, concurrency=8, poll=0.05, lease=timedelta(seconds=30))
    polling = asyncio.create_task(survivor.run())

    await wait_until(lambda: len(executions(output)) >= written)

    survivor.stop()
    await polling

    done = Counter(name.rsplit(".", 2)[0] for name in executions(output))

    assert sorted(done) == [f"run-{marker:05d}" for marker in range(written)], f"every run of every queue was executed:\n{told(output)}"
    assert [marker for marker, count in done.items() if count > 1] == [], "and not one of them twice"
    assert await app.count(status=RunStatus.FAILED) == 0
    assert await app.count(status=RunStatus.RUNNING) == 0

    # every queue really carried some of it, because a run that all landed in one lane asks the merging nothing
    for queue in QUEUES:
        assert await app.count(queue=queue) > 0, f"'{queue}' carried none of the work"


async def test_workers_sharing_one_process_under_leases_running_out_settle_every_run(fleet):
    """the same interleaving without the process boundary, at a pressure the ordinary suite cannot carry: tracing costs an order of magnitude, and it was this load untraced that turned up a contention budget giving up while the burst was still going. a store that refuses an outcome leaves the run claimed and the work quietly done again a lease later, so what this asks is not only that nothing ran twice but that everything was written down"""
    app, url, output = fleet

    workers = [Worker(app, concurrency=4, poll=0.01) for _ in range(PRESSING)]
    polling = [asyncio.create_task(worker.run()) for worker in workers]
    written = 0

    for _ in range(PRESSED_ROUNDS):
        written = await abandon(app, written, PRESSED_BATCH)

        await asyncio.sleep(0.05)

    await wait_until(lambda: len(executions(output)) >= written)

    for worker in workers:
        worker.stop()

    await asyncio.gather(*polling)

    done = Counter(name.rsplit(".", 2)[0] for name in executions(output))

    assert sorted(done) == [f"run-{marker:05d}" for marker in range(written)], "every run was executed"
    assert [marker for marker, count in done.items() if count > 1] == [], "and not one of them twice"

    # the interval task settles slots of its own beside all this, and every one of them is a run the store closed too
    ticks = len([path for path in output.iterdir() if path.name.startswith("tick.")])

    assert await app.count(status=RunStatus.DONE) == written + ticks, "and every outcome reached the store, instead of a run left claimed for a lease to hand back"
    assert await app.count(status=RunStatus.RUNNING) == 0
