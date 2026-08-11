"""a process that is killed on purpose, so the suite can ask what the next one finds"""

import asyncio
import sys
from pathlib import Path

from fast_tasks.app import FastTasks
from fast_tasks.store.sqlalchemy import SqlAlchemyStore
from fast_tasks.worker import Worker
from tests.fleet import build_engine


def build(database: str, output: str):
    app = FastTasks(SqlAlchemyStore(build_engine(database)))

    @app.task("slow", max_attempts=3, timeout=300)
    async def slow():
        Path(output).joinpath("started").write_text("started")
        await asyncio.sleep(300)

    @app.task("nightly", cron="0 1 * * *", max_attempts=3)
    async def nightly():
        Path(output).joinpath("nightly").write_text("ran")

    return app


async def main(database: str, output: str, seconds: float) -> None:
    app = build(database, output)
    worker = Worker(app, poll=0.02, lease=__import__("datetime").timedelta(seconds=1), grace=0.5)
    polling = asyncio.create_task(worker.run())

    await asyncio.sleep(seconds)
    worker.stop()
    await polling
    await app.store.engine.dispose()


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1], sys.argv[2], float(sys.argv[3])))
