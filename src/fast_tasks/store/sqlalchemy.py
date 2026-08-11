import asyncio
import random
from datetime import datetime, timedelta

from sqlalchemy import JSON, BigInteger, Column, DateTime, Float, Index, Integer, MetaData, String, Table, Text, TypeDecorator, and_, delete, func, select, update
from sqlalchemy.dialects.mysql import DATETIME as MYSQL_DATETIME
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from fast_tasks.clock import as_utc, naive_utc
from fast_tasks.retry import RetryPolicy
from fast_tasks.run import SETTLED, Run, RunStatus
from fast_tasks.store.base import CLAIM_SPREAD, KEY_LIMIT, QUEUE_LIMIT, RECLAIM_BATCH, TASK_NAME_LIMIT, WORKER_NAME_LIMIT, Store

# the store keeps its own metadata, so building its table never touches a table of the application around it
metadata = MetaData()

# innodb answers contention by rolling one side back and asking for the transaction again, which is the handling mysql documents and not a workaround: 1213 is a deadlock and 1205 is a lock it waited too long for
CONTENDED = frozenset({1205, 1213})

TRIES = 8

# short to begin with, because the other side only needs to finish the statement it is already in — and doubling, because contention on a hot queue comes in bursts that outlast a schedule only ever adding milliseconds. measured against innodb with forty claims and a reclaim on the same rows: five tries growing by ten milliseconds left outcomes the store refused on every single run, and eight doubling ones left none
BACKOFF = 0.005

# and drawn, for the same reason a retry delay is drawn: every transaction innodb rolled back works the same wait out from the same numbers, so a schedule they all share brings the whole herd back in lockstep to deadlock on each other again and spend every try doing it
SPREAD = 1.0


def contended(error: DBAPIError) -> bool:
    codes = getattr(error.orig, "args", ())

    return bool(codes) and codes[0] in CONTENDED


async def under_contention(work):
    """runs a write again when the database asked for it, and lets anything else through untouched"""
    for attempt in range(TRIES - 1):
        try:
            return await work()
        except DBAPIError as error:
            if not contended(error):
                raise

            await asyncio.sleep(BACKOFF * (2**attempt) * (1 + random.uniform(0, SPREAD)))

    return await work()


class UtcDateTime(TypeDecorator):
    """the database holds naive utc and the application reads aware utc, because mysql keeps no offset and a store that guesses one runs everything an hour late"""

    impl = DateTime
    cache_ok = True

    def load_dialect_impl(self, dialect):
        """mysql **rounds** a datetime with no fractional precision, so a run due at 10:00:00.9 is stored due at 10:00:01 — a second in the future, where nothing claims it until the second turns"""
        if dialect.name == "mysql":
            return dialect.type_descriptor(MYSQL_DATETIME(fsp=6))

        return dialect.type_descriptor(DateTime())

    def process_bind_param(self, value, dialect):
        return naive_utc(value)

    def process_result_value(self, value, dialect):
        return as_utc(value)


runs = Table(
    "fast_tasks_run",
    metadata,
    Column("id", BigInteger().with_variant(Integer, "sqlite"), primary_key=True, autoincrement=True),
    Column("key", String(KEY_LIMIT), nullable=True, unique=True),
    Column("name", String(TASK_NAME_LIMIT), nullable=False),
    Column("queue", String(QUEUE_LIMIT), nullable=False, default="default"),
    Column("payload", JSON, nullable=False, default=dict),
    Column("status", String(16), nullable=False, default=RunStatus.PENDING.value),
    Column("priority", Integer, nullable=False, default=0),
    Column("due_at", UtcDateTime, nullable=False),
    Column("attempts", Integer, nullable=False, default=0),
    Column("max_attempts", Integer, nullable=False, default=1),
    Column("timeout", Float, nullable=True),
    Column("retry_policy", String(32), nullable=False, default=RetryPolicy.EXPONENTIAL.value),
    Column("retry_delay", Float, nullable=False, default=5.0),
    Column("max_retry_delay", Float, nullable=False, default=3600.0),
    Column("worker", String(WORKER_NAME_LIMIT), nullable=True),
    Column("lease_until", UtcDateTime, nullable=True),
    Column("created_at", UtcDateTime, nullable=False),
    Column("started_at", UtcDateTime, nullable=True),
    Column("finished_at", UtcDateTime, nullable=True),
    Column("result", JSON, nullable=True),
    Column("error", Text, nullable=True),
    Column("error_type", String(128), nullable=True),
    # the one query a worker runs on every poll, answered by the index instead of by a scan of everything ever queued
    Index("fast_tasks_run_ready", "queue", "status", "priority", "due_at"),
    Index("fast_tasks_run_lease", "status", "lease_until"),
    # what the pruning asks for, so dropping a week of settled runs is not a scan of everything ever queued
    Index("fast_tasks_run_settled", "status", "finished_at"),
    # sqlite hands out the highest id it can see plus one, so pruning the newest settled run gives its id to the next run written. a caller holding an id from before the pruning then reads, and cancels, somebody else's run — which the sequences of postgres and the persisted counter of innodb never do
    sqlite_autoincrement=True,
)


def limited(condition, size: int):
    """the rows a statement is allowed to touch in one go. the limit lives in a derived table because mysql refuses one directly inside an `in`"""
    return runs.c.id.in_(select(select(runs.c.id).where(condition).limit(size).subquery().c.id))


def to_run(row) -> Run:
    return Run(
        id=row.id,
        key=row.key,
        name=row.name,
        queue=row.queue,
        payload=row.payload or {},
        status=RunStatus(row.status),
        priority=row.priority,
        due_at=row.due_at,
        attempts=row.attempts,
        max_attempts=row.max_attempts,
        timeout=row.timeout,
        retry_policy=RetryPolicy(row.retry_policy),
        retry_delay=row.retry_delay,
        max_retry_delay=row.max_retry_delay,
        worker=row.worker,
        lease_until=row.lease_until,
        created_at=row.created_at,
        started_at=row.started_at,
        finished_at=row.finished_at,
        result=row.result,
        error=row.error,
        error_type=row.error_type,
    )


def to_values(run: Run) -> dict:
    return {
        "key": run.key,
        "name": run.name,
        "queue": run.queue,
        "payload": run.payload,
        "status": run.status.value,
        "priority": run.priority,
        "due_at": run.due_at,
        "attempts": run.attempts,
        "max_attempts": run.max_attempts,
        "timeout": run.timeout,
        "retry_policy": run.retry_policy.value,
        "retry_delay": run.retry_delay,
        "max_retry_delay": run.max_retry_delay,
        "created_at": run.created_at,
    }


class SqlAlchemyStore(Store):
    """runs held in the database the application already has, which is what lets ten workers on ten machines agree without any of them talking to the others"""

    def __init__(self, engine: AsyncEngine) -> None:
        self.engine = engine
        self.sessions = async_sessionmaker(bind=engine, expire_on_commit=False)

    async def setup(self) -> None:
        """ten replicas boot together and `create_all` asks whether the table is there and then creates it, which is a question and a statement with a gap between them — the losers are told the table already exists and the process ends before it ever polls"""
        try:
            async with self.engine.begin() as connection:
                await connection.run_sync(metadata.create_all)
        except DBAPIError:
            # asking again is what settles it, and nothing here reads an error message: with the table there this does nothing, and with it still missing this raises for whatever the real reason was
            async with self.engine.begin() as connection:
                await connection.run_sync(metadata.create_all)

    async def add(self, run: Run) -> Run | None:
        """the one write every worker aims at the same key at the same instant, and innodb answers a duplicate two transactions race for with a deadlock as often as with a duplicate-key error"""
        return await under_contention(lambda: self.insert(run))

    async def insert(self, run: Run) -> Run | None:
        async with self.sessions() as session:
            try:
                written = await session.execute(runs.insert().values(**to_values(run)))
                await session.commit()
            except IntegrityError:
                await session.rollback()

                # the key is taken, which is exactly what a second worker writing the same occurrence is supposed to hit. a run with no key never raced anybody for one, so what the database refused there is a real failure and swallowing it hands the caller somebody else's run
                if run.key is None:
                    raise

                return None

            run.id = written.inserted_primary_key[0]

            return run

    async def claim(self, worker: str, queues: tuple[str, ...], limit: int, lease: timedelta, moment: datetime) -> list[Run]:
        """the candidates are read without a lock and each one is taken with a conditional write, so the database decides the winner and nothing is held between the two"""
        async with self.sessions() as session:
            ready = select(runs.c.id).where(runs.c.queue.in_(queues), runs.c.status == RunStatus.PENDING.value, runs.c.due_at <= moment).order_by(runs.c.priority.desc(), runs.c.due_at.asc(), runs.c.id.asc()).limit(limit * CLAIM_SPREAD)
            candidates = list((await session.execute(ready)).scalars())
            taken = []

            for run_id in candidates:
                if len(taken) == limit:
                    break

                if await self.take(session, run_id, worker, lease, moment):
                    taken.append(to_run((await session.execute(select(runs).where(runs.c.id == run_id))).one()))

            return taken

    async def take(self, session, run_id, worker: str, lease: timedelta, moment: datetime) -> bool:
        """one row, taken with a write conditional on the state it was in, so the database is what decides the winner"""

        async def once() -> bool:
            mine = update(runs).where(runs.c.id == run_id, runs.c.status == RunStatus.PENDING.value, runs.c.due_at <= moment).values(status=RunStatus.RUNNING.value, worker=worker, attempts=runs.c.attempts + 1, started_at=moment, lease_until=moment + lease)

            try:
                won = (await session.execute(mine)).rowcount == 1
                await session.commit()
            except DBAPIError:
                # the rollback is what leaves this session usable for the candidate after it
                await session.rollback()

                raise

            return won

        return await under_contention(once)

    def holding(self, run_id, worker: str):
        return and_(runs.c.id == run_id, runs.c.worker == worker, runs.c.status == RunStatus.RUNNING.value)

    async def write(self, condition, values: dict) -> bool:
        async def once() -> bool:
            async with self.sessions() as session:
                changed = (await session.execute(update(runs).where(condition).values(**values))).rowcount == 1
                await session.commit()

                return changed

        return await under_contention(once)

    async def heartbeat(self, run_id, worker: str, lease: timedelta, moment: datetime) -> bool:
        return await self.write(self.holding(run_id, worker), {"lease_until": moment + lease})

    async def complete(self, run_id, worker: str, moment: datetime, result: dict | None) -> bool:
        return await self.write(self.holding(run_id, worker), {"status": RunStatus.DONE.value, "finished_at": moment, "result": result, "error": None, "error_type": None, "worker": None, "lease_until": None})

    async def fail(self, run_id, worker: str, moment: datetime, error: str, error_type: str) -> bool:
        return await self.write(self.holding(run_id, worker), {"status": RunStatus.FAILED.value, "finished_at": moment, "error": error, "error_type": error_type, "worker": None, "lease_until": None})

    def requeued(self, due_at: datetime, error: str, error_type: str) -> dict:
        return {"status": RunStatus.PENDING.value, "due_at": due_at, "error": error, "error_type": error_type, "worker": None, "lease_until": None, "started_at": None}

    async def retry_later(self, run_id, worker: str, due_at: datetime, error: str, error_type: str) -> bool:
        return await self.write(self.holding(run_id, worker), self.requeued(due_at, error, error_type))

    async def release(self, run_id, worker: str, due_at: datetime, error: str, error_type: str) -> bool:
        return await self.write(self.holding(run_id, worker), self.requeued(due_at, error, error_type) | {"attempts": runs.c.attempts - 1})

    async def reclaim(self, moment: datetime) -> int:
        """what a dead worker left behind: the run goes back to the queue, unless its attempts are spent and there is nothing left to try"""
        return await under_contention(lambda: self.sweep(moment))

    async def sweep(self, moment: datetime) -> int:
        expired = and_(runs.c.status == RunStatus.RUNNING.value, runs.c.lease_until.is_not(None), runs.c.lease_until < moment)
        released = {"worker": None, "lease_until": None, "started_at": None}

        async with self.sessions() as session:
            # the two statements share the batch, because what the constant bounds is how long one pass holds the store and not how much either half of it writes
            gave_up = await self.take_back(session, and_(expired, runs.c.attempts >= runs.c.max_attempts), RECLAIM_BATCH, {"status": RunStatus.FAILED.value, "finished_at": moment, "error": "the worker holding this run stopped answering", "error_type": "LeaseExpired", **released})
            requeued = await self.take_back(session, and_(expired, runs.c.attempts < runs.c.max_attempts), RECLAIM_BATCH - gave_up, {"status": RunStatus.PENDING.value, "due_at": moment, **released})

            await session.commit()

            return gave_up + requeued

    async def take_back(self, session, expired, size: int, values: dict) -> int:
        """the batch is named by a subselect the database reads once and then holds, so the state those rows were named for is asserted again by the update itself. without it, a row this statement waited on a lock for is written whatever it has since become — and a lease another worker took over in that moment is taken straight back out from under it"""
        return (await session.execute(update(runs).where(expired, limited(expired, size)).values(**values))).rowcount

    async def purge(self, before: datetime, limit: int) -> int:
        """a settled run is still being indexed, counted and backed up, and nothing ever reads it again"""

        async def once() -> int:
            async with self.sessions() as session:
                over = and_(runs.c.status.in_([status.value for status in SETTLED]), runs.c.finished_at.is_not(None), runs.c.finished_at < before)
                gone = (await session.execute(delete(runs).where(limited(over, limit)))).rowcount
                await session.commit()

                return gone

        return await under_contention(once)

    async def cancel(self, run_id, moment: datetime) -> bool:
        return await self.write(and_(runs.c.id == run_id, runs.c.status == RunStatus.PENDING.value), {"status": RunStatus.CANCELED.value, "finished_at": moment})

    async def one(self, condition) -> Run | None:
        async with self.sessions() as session:
            row = (await session.execute(select(runs).where(condition))).one_or_none()

            return to_run(row) if row is not None else None

    async def get(self, run_id) -> Run | None:
        return await self.one(runs.c.id == run_id)

    async def find(self, key: str) -> Run | None:
        return await self.one(runs.c.key == key)

    async def count(self, status: RunStatus | None = None, queue: str | None = None) -> int:
        wanted = [column == value for column, value in ((runs.c.status, status.value if status else None), (runs.c.queue, queue)) if value is not None]

        async with self.sessions() as session:
            return await session.scalar(select(func.count()).select_from(runs).where(*wanted))
