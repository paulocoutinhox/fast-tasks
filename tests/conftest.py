import asyncio
import inspect
import os
import socket
from urllib.parse import urlsplit

import pytest
import pytest_asyncio
from redis.asyncio import Redis
from sqlalchemy import event
from sqlalchemy.ext.asyncio import create_async_engine

from fast_tasks.app import FastTasks
from fast_tasks.store.memory import MemoryStore
from fast_tasks.store.redis import RedisStore
from fast_tasks.store.sqlalchemy import SqlAlchemyStore
from fast_tasks.store.sqlalchemy import metadata as server_metadata

# a traced run is an order of magnitude slower than an untraced one, so the wait is generous — what makes it bounded is that it ends at all
PATIENCE = 90.0


def sqlite_engine(path):
    """a file and not a shared cache, because the point of these tests is more than one connection seeing the same rows"""
    engine = create_async_engine(f"sqlite+aiosqlite:///{path}", connect_args={"timeout": 30})

    # the setup the documentation tells everybody to use: without it a polling worker starves the writer beside it
    event.listen(engine.sync_engine, "connect", lambda connection, record: connection.execute("PRAGMA journal_mode=WAL"))

    return engine


async def wait_until(condition, patience: float = PATIENCE):
    """a condition that never comes is a failure anybody can read, and never a suite that spins. the check may be a plain call or a coroutine, because some of them have to ask the store"""
    async with asyncio.timeout(patience):
        while True:
            answer = condition()
            settled = await answer if inspect.isawaitable(answer) else answer

            if settled:
                return

            await asyncio.sleep(0.01)


@pytest_asyncio.fixture
async def memory_store():
    store = MemoryStore()
    await store.setup()

    return store


@pytest_asyncio.fixture
async def sql_store(tmp_path):
    engine = sqlite_engine(tmp_path / "app.db")
    store = SqlAlchemyStore(engine)
    await store.setup()

    yield store

    await engine.dispose()


# the servers a developer or a runner has. a store nobody can reach is not collected, and one that is answers the same contract as every other
SERVERS = {"redis": os.environ.get("FAST_TASKS_REDIS_URL", "redis://127.0.0.1:6399/0"), "mysql": os.environ.get("FAST_TASKS_MYSQL_URL", "mysql+aiomysql://root:root@127.0.0.1:3399/fast_tasks"), "postgres": os.environ.get("FAST_TASKS_POSTGRES_URL", "postgresql+asyncpg://postgres:postgres@127.0.0.1:5499/fast_tasks")}

PORTS = {"redis": 6399, "mysql": 3399, "postgres": 5499}


def reachable(url: str, fallback: int) -> bool:
    parts = urlsplit(url)

    try:
        with socket.socket() as probe:
            probe.settimeout(1.0)

            return probe.connect_ex((parts.hostname or "127.0.0.1", parts.port or fallback)) == 0
    except OSError:
        return False


REDIS_URL = SERVERS["redis"]

STORES = ["memory", "sqlalchemy"] + [name for name in ("redis", "mysql", "postgres") if reachable(SERVERS[name], PORTS[name])]


@pytest_asyncio.fixture(params=STORES)
async def store(request, tmp_path):
    """every store answers the same contract, and the way to keep that true is to run one suite against all of them"""
    if request.param == "memory":
        built = MemoryStore()
        await built.setup()

        yield built

        return

    if request.param == "redis":
        client = Redis.from_url(REDIS_URL)
        await client.flushdb()

        built = RedisStore(client)
        await built.setup()

        yield built

        await client.aclose()

        return

    if request.param in ("mysql", "postgres"):
        # the same schema a fresh database would get, so what runs here is what runs there
        engine = create_async_engine(SERVERS[request.param], pool_pre_ping=True)

        async with engine.begin() as connection:
            await connection.run_sync(server_metadata.drop_all)

        built = SqlAlchemyStore(engine)
        await built.setup()

        yield built

        await engine.dispose()

        return

    engine = sqlite_engine(tmp_path / "app.db")
    built = SqlAlchemyStore(engine)
    await built.setup()

    yield built

    await engine.dispose()


@pytest.fixture
def app(store):
    return FastTasks(store)


@pytest.fixture(scope="session")
def differential():
    return {}
