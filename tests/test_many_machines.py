"""the fleet, for real: separate processes with separate interpreters against one database, which is what ten containers are"""

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest_asyncio

from fast_tasks.run import RunStatus
from tests.conftest import wait_until
from tests.fleet import QUEUES, build_queue, prepare

MACHINES = 4
RUNS = 30
SECONDS = 3.0

# generous enough for a loaded runner, and short enough that a machine that never starts is a failure and not a hang
PATIENCE = 60.0

ROOT = Path(__file__).resolve().parent.parent


@pytest_asyncio.fixture
async def fleet(tmp_path):
    url, output = f"sqlite+aiosqlite:///{tmp_path / 'fleet.db'}", tmp_path / "done"
    output.mkdir()

    await prepare(url)
    app, closing = build_queue(url, str(output))

    yield app, url, output

    await closing()


def start_machines(url: str, output: Path, count: int, seconds: float = SECONDS) -> list:
    """the output of every machine is kept, so one that never starts says why instead of failing in silence"""
    environment = os.environ | {"PYTHONPATH": str(ROOT)}
    logs = [(output.parent / f"machine-{index}.log").open("w") for index in range(count)]
    settings = {"url": url, "output": str(output), "seconds": seconds, "attempts": 1, "lease": 60.0, "concurrency": 4, "queues": list(QUEUES)}

    return [(subprocess.Popen([sys.executable, "-m", "tests.machine", json.dumps(settings)], cwd=ROOT, env=environment, stdout=log, stderr=subprocess.STDOUT), log) for log in logs]


def told(output: Path) -> str:
    return "\n".join(path.read_text(errors="ignore") for path in sorted(output.parent.glob("machine-*.log")))


def wait_for_all(machines: list, output: Path) -> None:
    for process, log in machines:
        code = process.wait(timeout=PATIENCE)
        log.close()

        assert code == 0, f"a machine ended with {code}:\n{told(output)}"


def executions(output: Path, prefix: str) -> list[str]:
    return [path.name for path in output.iterdir() if path.name.startswith(prefix)]


async def test_four_machines_share_the_work_and_none_of_them_repeats_it(fleet):
    app, url, output = fleet

    for marker in range(RUNS):
        await app.enqueue("record", marker=f"run-{marker:03d}")

    wait_for_all(start_machines(url, output, MACHINES), output)

    done = executions(output, "run-")
    markers = {name.split(".")[0] for name in done}

    assert len(markers) == RUNS, f"every run was executed:\n{told(output)}"
    assert len(done) == RUNS, "and not one of them twice"
    assert await app.count(status=RunStatus.FAILED) == 0

    # the work really was shared, and did not all land on whoever started first
    assert len({name.split(".")[1] for name in done}) > 1


async def test_a_recurring_job_fires_once_a_slot_across_machines(fleet):
    app, url, output = fleet

    wait_for_all(start_machines(url, output, MACHINES), output)

    ticks = executions(output, "tick.")

    assert ticks, f"the interval job fired on its own, with nobody enqueueing anything:\n{told(output)}"
    assert len(ticks) == await app.count(status=RunStatus.DONE), "one execution per slot, and four machines did not make four of each"


async def test_a_machine_that_dies_mid_job_leaves_the_run_for_the_next_one(fleet):
    app, url, output = fleet

    await app.enqueue("record", marker="interrupted")

    machines = start_machines(url, output, 1, seconds=PATIENCE)
    dying, log = machines[0]

    await wait_until(lambda: executions(output, "interrupted"))

    dying.kill()
    dying.wait(timeout=PATIENCE)
    log.close()

    assert len(executions(output, "interrupted")) == 1
