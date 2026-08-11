"""what a process that was killed leaves behind, and what the next one does with it"""

import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest_asyncio

from fast_tasks.run import RunStatus
from tests.conftest import wait_until
from tests.survivor import build

ROOT = Path(__file__).resolve().parent.parent
PATIENCE = 60.0


@pytest_asyncio.fixture
async def machine(tmp_path):
    database, output = str(tmp_path / "restart.db"), tmp_path / "out"
    output.mkdir()

    app = build(database, str(output))
    await app.setup()

    async with app.store.engine.begin() as connection:
        await connection.exec_driver_sql("PRAGMA journal_mode=WAL")

    yield app, database, output

    await app.store.engine.dispose()


def start(database: str, output: Path, seconds: float = 60.0):
    return subprocess.Popen([sys.executable, "-m", "tests.survivor", database, str(output), str(seconds)], cwd=ROOT, env=os.environ | {"PYTHONPATH": str(ROOT)})


async def test_a_run_the_killed_process_was_holding_is_picked_up_by_the_next_one(machine):
    """killed **during** a run: its lease runs out and the run comes back, which is what a deploy in the middle of a pass means"""
    app, database, output = machine

    written = await app.enqueue("slow")
    dying = start(database, output)

    await wait_until(lambda: (output / "started").exists(), PATIENCE)

    dying.kill()
    dying.wait(timeout=PATIENCE)

    held = await app.get(written.id)

    assert held.status == RunStatus.RUNNING, "the killed process left it claimed, because nothing could tell the store otherwise"
    assert held.attempts == 1

    survivor = start(database, output)
    attempts = []

    async def tried_again() -> bool:
        attempts.append((await app.get(written.id)).attempts)

        return attempts[-1] >= 2

    try:
        await wait_until(tried_again, PATIENCE)
    finally:
        survivor.kill()
        survivor.wait(timeout=PATIENCE)

    assert attempts[-1] >= 2, "the next process found the abandoned run and tried it again"


async def test_an_occurrence_that_came_due_while_nothing_was_running_is_run_late(machine):
    """killed **before** the hour: the slot was already written, so the next process finds it overdue and runs it"""
    app, database, output = machine

    overdue = datetime.now(timezone.utc) - timedelta(hours=2)
    await app.enqueue_at("nightly", overdue, key=f"nightly@{overdue.isoformat()}")

    assert (await app.count(status=RunStatus.PENDING)) == 1
    assert not (output / "nightly").exists(), "nothing ran it while nothing was running"

    late = start(database, output)

    try:
        await wait_until(lambda: (output / "nightly").exists(), PATIENCE)
    finally:
        late.kill()
        late.wait(timeout=PATIENCE)

    assert (output / "nightly").exists(), "the next process to come up ran what was already due"


async def test_the_next_occurrence_is_written_a_whole_period_ahead(machine):
    """which is why a nightly cron survives a restart: the row for tomorrow exists long before tomorrow"""
    app, database, output = machine

    written = await app.materialize(datetime(2026, 1, 1, 3, 0, tzinfo=timezone.utc))

    assert [run.due_at for run in written] == [datetime(2026, 1, 2, 1, 0, tzinfo=timezone.utc)]
    assert written[0].due_at - datetime(2026, 1, 1, 3, 0, tzinfo=timezone.utc) > timedelta(hours=20), "written twenty-two hours before it is due, so a process has to be down that entire window to miss it"
