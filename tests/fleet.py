"""the app a fleet shares: every process builds the same one from the same code, which is the setup the whole design is for"""

import os
from pathlib import Path
from uuid import uuid4

from sqlalchemy.ext.asyncio import create_async_engine

from fast_tasks.app import FastTasks
from fast_tasks.store.sqlalchemy import SqlAlchemyStore


def build_engine(database: str):
    # write-ahead logging is what lets more than one process hold this file at once, and the timeout is what makes them wait instead of fail
    return create_async_engine(f"sqlite+aiosqlite:///{database}", connect_args={"timeout": 30}, isolation_level="AUTOCOMMIT")


def build_queue(database: str, output: str) -> tuple[FastTasks, object]:
    engine = build_engine(database)
    app = FastTasks(SqlAlchemyStore(engine))

    @app.task("record", every=None, max_attempts=1)
    async def record(marker: str):
        # one file per execution, so a run that happened twice leaves two of them and cannot hide
        Path(output).joinpath(f"{marker}.{os.getpid()}.{uuid4().hex}").write_text(marker)

    @app.task("tick", every=1, max_attempts=1)
    async def tick():
        Path(output).joinpath(f"tick.{os.getpid()}.{uuid4().hex}").write_text("tick")

    return app, engine


async def prepare(database: str) -> None:
    app, engine = build_queue(database, ".")

    try:
        await app.setup()

        async with engine.begin() as connection:
            await connection.exec_driver_sql("PRAGMA journal_mode=WAL")
    finally:
        await engine.dispose()
