"""the app a fleet shares: every process builds the same one from the same code, which is the setup the whole design is for"""

import os
from pathlib import Path
from uuid import uuid4

from fast_tasks.app import FastTasks

# write-ahead logging is what lets more than one process hold one file at once, and the timeout is what makes them wait instead of fail
SQLITE = {"connect_args": {"timeout": 30}, "isolation_level": "AUTOCOMMIT"}

# a machine polls, claims and beats at the same time, so the pool has to hold more than one of it
SERVER = {"pool_pre_ping": True, "pool_size": 10, "max_overflow": 20}


def build_store(url: str):
    """whichever store the url names, and the call that lets go of it. a fleet is the same code against one store, and which one it is changes nothing above this line"""
    if url.startswith("redis"):
        from redis.asyncio import Redis

        from fast_tasks.store.redis import RedisStore

        client = Redis.from_url(url)

        return RedisStore(client), client.aclose

    from sqlalchemy.ext.asyncio import create_async_engine

    from fast_tasks.store.sqlalchemy import SqlAlchemyStore

    engine = create_async_engine(url, **(SQLITE if url.startswith("sqlite") else SERVER))

    return SqlAlchemyStore(engine), engine.dispose


# the queues a machine serves, and the task that writes into each. a claim merges the lanes of all of them, so a fleet spread this way is what says the merging holds under contention and not only in one process
QUEUES = ("default", "email", "reports")

TASKS = ("record", "record_email", "record_reports")


def build_queue(url: str, output: str, attempts: int = 1):
    store, closing = build_store(url)
    app = FastTasks(store)

    def executed(marker: str) -> None:
        # one file per execution, so a run that happened twice leaves two of them and cannot hide
        Path(output).joinpath(f"{marker}.{os.getpid()}.{uuid4().hex}").write_text(marker)

    @app.task("record", max_attempts=attempts)
    async def record(marker: str):
        executed(marker)

    @app.task("record_email", queue="email", priority=5, max_attempts=attempts)
    async def record_email(marker: str):
        executed(marker)

    @app.task("record_reports", queue="reports", priority=10, max_attempts=attempts)
    async def record_reports(marker: str):
        executed(marker)

    @app.task("tick", every=1, max_attempts=attempts)
    async def tick():
        executed("tick")

    return app, closing


async def empty(url: str) -> None:
    """the store as a fresh one would be, so what a run of this proves is about the code and not about what the run before it left"""
    store, closing = build_store(url)

    if url.startswith("redis"):
        await store.client.flushdb()
    else:
        from fast_tasks.store.sqlalchemy import metadata

        async with store.engine.begin() as connection:
            await connection.run_sync(metadata.drop_all)

    await closing()


async def prepare(url: str) -> None:
    """what one process does before the fleet starts, because a write-ahead log is a property of the file and not of a connection"""
    app, closing = build_queue(url, ".")

    try:
        await app.setup()

        async with app.store.engine.begin() as connection:
            await connection.exec_driver_sql("PRAGMA journal_mode=WAL")
    finally:
        await closing()
