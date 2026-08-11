"""what a line by line reading found, each one pinned by the test that would have caught it"""

import asyncio
import time
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import create_async_engine

from fast_tasks.clock import now
from fast_tasks.errors import CronError, PermanentError, QueueError
from fast_tasks.retry import RetryPolicy, delay_for
from fast_tasks.run import Run, RunStatus
from fast_tasks.store.base import RECLAIM_BATCH
from fast_tasks.task import Task
from fast_tasks.trigger import Cron
from fast_tasks.worker import PURGE_LIMIT, Worker

from .conftest import wait_until


class Greeter:
    """a callable object with an async call, which `iscoroutinefunction` says nothing about"""

    def __init__(self) -> None:
        self.seen = []

    async def __call__(self, who: str) -> None:
        self.seen.append(who)


def wrapped(handler):
    """the shape every decorator that forgets `async` has: a plain function answering a coroutine"""

    def wrapper(**payload):
        return handler(**payload)

    return wrapper


async def test_a_handler_that_only_looks_synchronous_is_still_run(app):
    """awaiting the thread alone would close the run as done with the work never started, and nothing anywhere would say so"""
    greeter = Greeter()
    app.register(Task(name="greet", handler=greeter))

    written = await app.enqueue("greet", who="paulo")

    worker = Worker(app)
    await worker.run_once()
    await worker.drain()

    assert greeter.seen == ["paulo"], "the work actually happened"
    assert (await app.get(written.id)).status == RunStatus.DONE


async def test_a_decorated_handler_whose_wrapper_is_plain_is_still_run(app):
    seen = []

    async def greet(who):
        seen.append(who)

    app.register(Task(name="greet", handler=wrapped(greet)))

    await app.enqueue("greet", who="paulo")

    worker = Worker(app)
    await worker.run_once()
    await worker.drain()

    assert seen == ["paulo"]


async def test_a_store_that_blinked_never_costs_a_recurring_task_its_slot(app):
    """the slot was remembered before it was written, so one bad moment dropped that occurrence for good — for a daily task, a whole day"""
    moment = datetime(2026, 1, 1, 10, 0, 5, tzinfo=timezone.utc)
    original = type(app.store).add
    refused = []

    async def refusing(self, run):
        if not refused:
            refused.append(1)

            raise ConnectionError("the store went away")

        return await original(self, run)

    type(app.store).add = refusing

    try:

        @app.task("nightly", cron="0 4 * * *")
        async def nightly():
            return None

        with pytest.raises(ConnectionError):
            await app.materialize(moment)

        assert await app.count() == 0

        written = await app.materialize(moment)
    finally:
        type(app.store).add = original

    assert [run.due_at for run in written] == [datetime(2026, 1, 2, 4, 0, tzinfo=timezone.utc)], "the pass after it wrote the slot the failed one owed"


async def test_a_key_reserved_by_a_write_that_never_finished_is_not_lost_for_ever(app):
    """redis reserved the key and wrote the run in separate steps, so a process dying between them poisoned that key permanently"""
    from fast_tasks.store.redis import RedisStore

    if not isinstance(app.store, RedisStore):
        pytest.skip("the reservation and the write are one statement in every other store")

    @app.task("welcome")
    async def welcome(account):
        return None

    written = await app.enqueue("welcome", key="welcome:7", account=7)

    assert written.id is not None
    assert (await app.find("welcome:7")).id == written.id, "the key points at a run that exists"

    again = await app.enqueue("welcome", key="welcome:7", account=7)

    assert again.id == written.id


@pytest.mark.parametrize("options,reason", [({"poll": 0}, "does not wait"), ({"poll": -1}, "does not wait"), ({"concurrency": 0}, "never takes anything"), ({"grace": -1}, "not a span"), ({"lease": timedelta(seconds=0)}, "already run out")])
def test_a_worker_that_could_never_work_is_refused_where_it_is_written(app, options, reason):
    with pytest.raises(QueueError, match=reason):
        Worker(app, **options)


@pytest.mark.parametrize("options,reason", [({"max_attempts": 0}, "at least once"), ({"timeout": 0}, "before it starts"), ({"retry_delay": -1}, "never negative")])
def test_a_task_that_could_never_run_is_refused_where_it_is_declared(app, options, reason):
    with pytest.raises(QueueError, match=reason):
        app.task("broken", **options)(lambda: None)


async def test_a_worker_polling_is_not_a_worker_spinning(app):
    """the poll is a wait, and a worker that does not wait spends a core asking the store nothing"""
    asked = []
    original = type(app.store).reclaim

    async def counting(self, moment):
        asked.append(1)

        return await original(self, moment)

    type(app.store).reclaim = counting

    try:
        worker = Worker(app, poll=0.05)
        polling = asyncio.create_task(worker.run())

        await asyncio.sleep(0.25)
        worker.stop()
        await polling
    finally:
        type(app.store).reclaim = original

    assert len(asked) < 20, f"a quarter of a second asked the store {len(asked)} times, which is a spin and not a poll"


async def test_a_recurring_task_is_not_worked_out_again_on_every_poll(app):
    """a yearly expression is walked minute by minute, and asking the trigger before the cache spent a third of a core answering the same thing for ever"""
    asked = []

    @app.task("yearly", cron="0 0 1 1 *")
    async def yearly():
        return None

    original = type(app.tasks["yearly"].trigger).next_after

    def counting(self, moment):
        asked.append(1)

        return original(self, moment)

    type(app.tasks["yearly"].trigger).next_after = counting

    try:
        for _ in range(50):
            await app.materialize()
    finally:
        type(app.tasks["yearly"].trigger).next_after = original

    assert asked == [1], "worked out once for the slot it wrote, and never again while that slot is ahead"


async def test_a_reclaim_takes_a_batch_and_never_the_whole_backlog(app):
    """redis runs a script to the end before it answers anybody, so a backlog of expired leases would freeze the server"""
    from fast_tasks.store.redis import RECLAIM_BATCH, RedisStore

    if not isinstance(app.store, RedisStore):
        pytest.skip("only redis holds the whole server while a script runs")

    @app.task("work", max_attempts=3)
    async def work():
        return None

    for _ in range(RECLAIM_BATCH + 5):
        await app.enqueue("work")

    await app.store.claim("a-worker-that-died", ("default",), RECLAIM_BATCH + 5, timedelta(seconds=-1), datetime.now(timezone.utc))

    assert await app.store.reclaim(datetime.now(timezone.utc)) == RECLAIM_BATCH, "one pass took a batch"
    assert await app.store.reclaim(datetime.now(timezone.utc)) == 5, "and the pass after it took the rest"


async def test_a_run_somebody_deleted_by_hand_does_not_end_every_reclaim_for_ever(app):
    """one id left behind in the leases would make the script read a field that is gone, and every worker's pass would break on it"""
    from fast_tasks.store.redis import RedisStore

    if not isinstance(app.store, RedisStore):
        pytest.skip("only redis keeps the leases in a set of its own")

    @app.task("work", max_attempts=3)
    async def work():
        return None

    written = await app.enqueue("work")
    await app.store.claim("a-worker-that-died", ("default",), 1, timedelta(seconds=-1), datetime.now(timezone.utc))

    await app.store.client.delete(app.store.run_key(written.id))

    assert await app.store.reclaim(datetime.now(timezone.utc)) == 1, "it stepped over what was not there instead of breaking"
    assert await app.store.reclaim(datetime.now(timezone.utc)) == 0, "and the leases no longer name it"


async def test_a_run_names_the_same_instant_whichever_store_wrote_it(app):
    """`fromtimestamp` with no zone answers the wall clock of the machine, and calling that utc moved every instant the redis store read by the offset of wherever it ran"""
    when = datetime(2026, 8, 10, 12, 0, tzinfo=timezone.utc)

    @app.task("work")
    async def work():
        return None

    written = await app.enqueue_at("work", when)
    read = await app.get(written.id)

    assert read.due_at == when, "read back as the instant it was written, and not three hours off it"


async def test_a_datetime_with_no_zone_is_read_as_utc_by_every_store(app):
    """`timestamp()` on a naive value reads it as local time, and the same value would be one instant in one store and another instant in the other"""

    @app.task("work")
    async def work():
        return None

    written = await app.enqueue_at("work", datetime(2026, 8, 10, 12, 0))
    read = await app.get(written.id)

    assert read.due_at == datetime(2026, 8, 10, 12, 0, tzinfo=timezone.utc), "what a column without an offset means is what this means"


async def test_an_expression_that_asks_for_a_day_no_month_has_is_refused_where_it_is_written(app):
    """nothing but the search can tell february has no thirtieth, and the search walks a year of minutes to find that out — on every pass, for as long as the process lives"""
    with pytest.raises(CronError):
        Cron("0 0 30 2 *")

    assert Cron("0 0 30 2 5"), "with the weekday field restricted posix joins the two with an or, so a friday in february still matches"
    assert Cron("0 0 31 * *"), "and a thirty first is asked for by every month that has one"


async def test_a_run_the_store_lost_is_not_claimed_as_a_run_with_nothing_in_it(app):
    """a redis holding this under an eviction policy drops the hash and keeps the lane, and writing the fields of a claim over nothing builds a hash that holds those fields and no run"""
    from fast_tasks.store.redis import RedisStore

    if not isinstance(app.store, RedisStore):
        pytest.skip("only redis keeps the run and the lane it is queued in as two separate things")

    @app.task("work")
    async def work():
        return None

    written = await app.enqueue("work")
    await app.store.client.delete(app.store.run_key(written.id))

    assert await app.store.claim("a-worker", ("default",), 10, timedelta(seconds=60), datetime.now(timezone.utc)) == [], "nothing was handed out"
    assert await app.store.claim("a-worker", ("default",), 10, timedelta(seconds=60), datetime.now(timezone.utc)) == [], "and the lane no longer names it"


async def test_writing_a_run_is_tried_again_when_the_database_asks_for_it(app):
    """every worker writes the same occurrence key at the same instant, and innodb answers a duplicate two transactions race for with a deadlock as often as with a duplicate-key error"""
    from fast_tasks.store.sqlalchemy import SqlAlchemyStore

    if not isinstance(app.store, SqlAlchemyStore):
        pytest.skip("contention is what the database answers, and only this store speaks to one")

    @app.task("work")
    async def work():
        return None

    original = SqlAlchemyStore.insert
    refused = []

    async def deadlocking(self, run):
        if not refused:
            refused.append(run)

            raise DBAPIError("insert", {}, Exception(1213, "Deadlock found when trying to get lock"))

        return await original(self, run)

    SqlAlchemyStore.insert = deadlocking

    try:
        written = await app.enqueue("work")
    finally:
        SqlAlchemyStore.insert = original

    assert refused, "the database asked once"
    assert await app.get(written.id) is not None, "and the run was written on the try after it"


async def test_a_herd_that_failed_at_the_same_instant_does_not_come_back_as_a_herd(app):
    """a multiplier every one of them shares works out the same delay from the same numbers, and the herd is handed back whole"""
    drawn = {delay_for(RetryPolicy.EXPONENTIAL_JITTER, 10, 2, jitter=0.5) for _ in range(50)}

    assert len(drawn) > 1, "they were spread"
    assert min(drawn) >= 20, "and never sooner than the policy without jitter says"
    assert max(drawn) <= 30, "and never further out than the jitter allows"


async def test_a_run_that_is_over_is_dropped_once_it_is_older_than_the_worker_keeps(app):
    """nothing pruned this before, so the table and the keys grew for as long as the deployment lived"""

    @app.task("work")
    async def work():
        return None

    kept = await app.enqueue("work", key="only-once")
    worker = Worker(app, poll=0.05, keep=timedelta(days=7))

    await worker.run_once()
    await wait_until(lambda: worker.free == worker.concurrency)
    await worker.run_once()

    assert await app.get(kept.id) is not None, "what is over and recent stays, and a worker pass does not touch it"

    worker.keep = timedelta(0)
    worker.purged = None

    assert await worker.tidy(now()) == 1, "and the worker is what prunes it, on its own pass"
    assert await app.get(kept.id) is None
    assert await app.find("only-once") is None, "the reservation goes with the run it named, or what is left is a key nothing can ever write again"


async def test_pruning_is_housekeeping_and_never_the_work_of_a_pass(app):
    """a delete on every poll is a write per second per worker to drop what one an hour drops just as well"""

    @app.task("work")
    async def work():
        return None

    worker = Worker(app, poll=0.05, keep=timedelta(0))
    worker.purged = None
    asked = []

    async def counting(before, limit):
        asked.append(before)

        return 0

    worker.app.store.purge = counting

    await worker.tidy(now())
    await worker.tidy(now())
    await worker.tidy(now())

    assert len(asked) == 1, "the pass after it is inside the hour, and asks nothing"


async def test_a_worker_told_to_keep_everything_prunes_nothing(app):
    """dropping what somebody wanted kept is the one thing this must never do on its own"""
    worker = Worker(app, poll=0.05, keep=None)

    assert await worker.tidy(now()) == 0


async def test_every_way_a_run_ends_is_a_way_it_can_be_pruned(app):
    """three paths close a run — the handler, the policy running out and a lease nobody renewed — and one of them writing the state without saying it is over leaves that run behind for ever"""

    @app.task("breaks", max_attempts=1)
    async def breaks():
        raise RuntimeError("no")

    @app.task("works")
    async def works():
        return None

    await app.enqueue("works")
    await app.enqueue("breaks")

    worker = Worker(app, poll=0.05)
    await worker.run_once()
    await wait_until(lambda: worker.free == worker.concurrency)

    # written after the pass, so this one is abandoned by a lease nobody renewed instead of being worked
    abandoned = await app.enqueue("works")
    await app.store.claim("a-worker-that-died", ("default",), 10, timedelta(seconds=-1), now())

    assert await app.store.reclaim(now()) == 1
    assert (await app.get(abandoned.id)).status == RunStatus.FAILED

    await app.cancel((await app.enqueue("works")).id)
    assert await app.store.purge(now() + timedelta(days=1), 1000) == 4, "done, failed, given up on and called off"
    assert await app.count() == 0


async def test_an_outcome_the_store_would_not_take_is_not_announced_as_one_it_took(app):
    """the lease ran out and somebody else took the run over, and telling the listeners it finished puts one run in an audit trail twice"""
    told = []

    @app.task("work")
    async def work():
        return None

    worker = Worker(app, poll=0.05)

    async def finished(run, answer, seconds):
        told.append(run.id)

    worker.on_finish(finished)

    written = await app.enqueue("work")

    async def refused(run_id, name, moment, result):
        return False

    worker.app.store.complete = refused

    await worker.run_once()
    await wait_until(lambda: worker.free == worker.concurrency)

    assert told == [], f"the store refused the outcome of {written.id}, so whoever holds it now is the one that answers for it"


@pytest.mark.parametrize("outcome", ["boom", "permanent", "unknown"])
async def test_no_ending_the_store_would_not_take_is_announced_as_one_it_took(app, outcome):
    """the guard covered the run that finished and none of the three ways one ends badly, so a lost lease wrote one failure per worker into the audit trail"""
    told = []

    @app.task("work", max_attempts=3)
    async def work():
        raise PermanentError("no") if outcome == "permanent" else RuntimeError("boom")

    worker = Worker(app, poll=0.05)

    async def failed(run, error, seconds, retrying):
        told.append(run.id)

    worker.on_error(failed)

    written = await app.enqueue("work") if outcome != "unknown" else await app.store.add(Run(name="from_the_future"))

    async def refused(*arguments):
        return False

    worker.app.store.fail = refused
    worker.app.store.retry_later = refused

    await worker.run_once()
    await wait_until(lambda: worker.free == worker.concurrency)

    assert told == [], f"the store refused how {written.id} ended, so whoever holds it now is the one that answers for it"


async def test_a_listener_that_is_a_plain_function_is_a_listener(app, caplog):
    """the same ambiguity a handler already answers for. it is not that the listener never ran — it ran, and then awaiting what it returned raised, which is a `TypeError` in a log nobody reads for a hook that looked like it worked"""
    told = []

    @app.task("work")
    async def work():
        return None

    worker = Worker(app, poll=0.05)
    worker.on_start(lambda run: told.append(run.name))

    await app.enqueue("work")
    await worker.run_once()
    await wait_until(lambda: worker.free == worker.concurrency)

    assert told == ["work"]
    assert "a listener of" not in caplog.text, "and nothing was logged as a listener that broke"


async def test_replicas_coming_up_together_all_come_up(app):
    """`create_all` asks whether the table is there and then creates it, and ten replicas booting together left eight of them dead on the question"""
    from fast_tasks.store.sqlalchemy import SqlAlchemyStore, metadata

    if not isinstance(app.store, SqlAlchemyStore):
        pytest.skip("only a database is asked to build anything")

    async with app.store.engine.begin() as connection:
        await connection.run_sync(metadata.drop_all)

    outcome = await asyncio.gather(*[SqlAlchemyStore(app.store.engine).setup() for _ in range(10)], return_exceptions=True)
    broke = [error for error in outcome if isinstance(error, BaseException)]

    assert broke == [], "every one of them came up"


async def test_a_database_that_refuses_to_build_the_table_still_says_so(app):
    """asking again is what tells a race apart from a real refusal, and swallowing both would be a worker that polls for ever against nothing"""
    from fast_tasks.store.sqlalchemy import SqlAlchemyStore

    if not isinstance(app.store, SqlAlchemyStore):
        pytest.skip("only a database is asked to build anything")

    broken = SqlAlchemyStore(create_async_engine("sqlite+aiosqlite:////this/is/not/a/place/runs.sqlite"))

    with pytest.raises(DBAPIError):
        await broken.setup()

    await broken.engine.dispose()


async def test_a_cluster_that_died_holding_everything_is_taken_back_a_batch_at_a_time(app):
    """one statement over every lease a dead cluster left is a transaction that holds the store while every other worker waits on it"""

    @app.task("work", max_attempts=5)
    async def work():
        return None

    for _ in range(RECLAIM_BATCH + 5):
        await app.enqueue("work")

    await app.store.claim("a-worker-that-died", ("default",), RECLAIM_BATCH + 5, timedelta(seconds=-1), now())

    assert await app.store.reclaim(now()) == RECLAIM_BATCH, "one pass took a batch"
    assert await app.store.reclaim(now()) == 5, "and the pass after it took the rest"


async def test_a_pruning_the_store_refused_does_not_cost_the_pass_its_claim(app):
    """housekeeping that ends the pass is a worker that stops claiming once an hour, for a reason nothing in the queue explains"""

    @app.task("work")
    async def work():
        return None

    worker = Worker(app, poll=0.05, keep=timedelta(0))
    worker.purged = None

    async def refuse(before, limit):
        raise RuntimeError("the store said no")

    worker.app.store.purge = refuse
    await app.enqueue("work")

    assert await worker.run_once(), "the pass went on and claimed what was due"


async def test_a_backlog_is_drained_pass_by_pass_and_not_hour_by_hour(app):
    """a thousand an hour is a year of hours to catch up on a year that was never pruned"""
    worker = Worker(app, poll=0.05, keep=timedelta(0))
    worker.purged = None
    passes = []

    async def full(before, limit):
        passes.append(limit)

        return limit if len(passes) < 3 else 0

    worker.app.store.purge = full

    assert await worker.tidy(now()) == PURGE_LIMIT
    assert await worker.tidy(now()) == PURGE_LIMIT, "a full batch means there is more of it, and the pass after it takes the next one"
    assert await worker.tidy(now()) == 0
    assert await worker.tidy(now()) == 0, "and once it comes back short the hour starts"
    assert len(passes) == 3


async def test_ten_workers_do_not_all_prune_in_the_same_instant(app):
    """the same herd a drawn retry delay exists to spread, one deploy later"""
    drawn = {Worker(app).purged for _ in range(20)}

    assert len(drawn) > 1


async def test_keeping_a_run_for_less_than_no_time_is_refused(app):
    """it asks for what is over to be dropped before it is over, which is every one of them"""
    with pytest.raises(QueueError):
        Worker(app, keep=timedelta(seconds=-1))

    assert Worker(app, keep=timedelta(0)), "and dropping a run the moment it is over is a thing somebody may want"


async def test_a_name_the_worker_beside_this_one_knows_is_not_destroyed_by_this_one(app):
    """a rolling deploy runs two versions at once, and the older replica claiming what the newer one enqueued must not be the end of it — `max_attempts` is one by default, so failing it here is failing it for good"""
    written = await app.store.add(Run(name="from_the_future"))

    assert written.max_attempts == 1

    worker = Worker(app, poll=0.05)
    await worker.run_once()
    await wait_until(lambda: worker.free == worker.concurrency)

    settled = await app.get(written.id)

    assert settled.status == RunStatus.PENDING, "it waits for the replica that knows the name"
    assert settled.error_type == "UnknownTask", "and it says what it is waiting for"


async def test_a_name_nobody_knows_backs_off_instead_of_being_asked_for_ever(app):
    """what is never destroyed has to stop costing something, and the backoff is what does that: the same growth every other retry gets, up to the ceiling"""
    written = await app.store.add(Run(name="nobody_knows", retry_policy=RetryPolicy.FIXED, retry_delay=30))

    worker = Worker(app, poll=0.05)
    await worker.run_once()
    await wait_until(lambda: worker.free == worker.concurrency)

    settled = await app.get(written.id)

    assert settled.due_at > now() + timedelta(seconds=20), "it is not asked for again on the next poll"


async def test_a_jitter_below_zero_is_refused(app):
    """it draws a delay shorter than the policy asked for, and one far enough below it is a retry due in the past — which is hammering, not backing off"""
    with pytest.raises(QueueError):
        Worker(app, jitter=-0.5)

    assert Worker(app, jitter=0), "and asking for no spread at all is a thing somebody may want"


async def test_a_timeout_stops_the_waiting_and_only_a_coroutine_stops_the_work(app):
    """python cannot end a thread from outside, so a plain handler carries on to its own end while the worker has already given up on it — and the attempt after it overlaps the one before"""
    ran = []

    @app.task("slow", timeout=0.1, max_attempts=2, retry_delay=0)
    def slow():
        ran.append("started")
        time.sleep(0.4)
        ran.append("finished anyway")

    worker = Worker(app, poll=0.05, keep=None)
    await app.enqueue("slow")

    for _ in range(8):
        await worker.run_once()
        await asyncio.sleep(0.08)

    await asyncio.sleep(0.6)

    assert ran.count("started") == 2, "the worker gave up on the first and started the attempt after it"
    assert ran.count("finished anyway") == 2, "and neither copy was stopped, which is what a timeout on a plain handler means"
