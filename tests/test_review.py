"""what a line by line reading found, each one pinned by the test that would have caught it"""

import asyncio
import time
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.ext.asyncio import create_async_engine

from fast_tasks.app import WIDEST_SLOT, occurrence_key
from fast_tasks.clock import EPOCH, now
from fast_tasks.errors import CronError, PermanentError, QueueError
from fast_tasks.retry import RetryPolicy, delay_for
from fast_tasks.run import Run, RunStatus
from fast_tasks.store.base import KEY_LIMIT, QUEUE_LIMIT, RECLAIM_BATCH, TASK_NAME_LIMIT, WORKER_NAME_LIMIT
from fast_tasks.task import Task
from fast_tasks.trigger import Cron, Interval
from fast_tasks.worker import PURGE_LIMIT, Worker, worker_name

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


@pytest.mark.parametrize("options,reason", [({"max_attempts": 0}, "at least once"), ({"timeout": 0}, "before it starts"), ({"retry_delay": -1}, "never negative"), ({"max_retry_delay": 0}, "hammers instead of backing off"), ({"max_retry_delay": -5}, "hammers instead of backing off")])
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
    worker.app.store.release = refused

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


async def test_a_worker_on_a_machine_with_a_long_name_can_still_claim_anything(app):
    """a pod is named after its deployment, its namespace and its cluster, and a name past what the column holds is a worker whose every claim the database refuses — it polls forever and takes nothing"""
    assert len(worker_name()) <= WORKER_NAME_LIMIT, "the name a worker gives itself always fits"

    @app.task("work")
    async def work():
        return None

    await app.enqueue("work")

    longest = Worker(app, name="x" * WORKER_NAME_LIMIT)

    assert len(await longest.app.store.claim(longest.name, ("default",), 1, timedelta(seconds=60), now())) == 1, "the store took the longest name a worker may have"

    with pytest.raises(QueueError, match="does not fit"):
        Worker(app, name="x" * (WORKER_NAME_LIMIT + 1))


async def test_a_name_this_worker_never_heard_of_costs_the_run_no_attempt(app):
    """`max_attempts` is one by default, so a rolling deploy that spent the attempt left the run to be destroyed by the first reclaim that met it"""
    written = await app.store.add(Run(name="from_the_future", max_attempts=1))

    worker = Worker(app, poll=0.05)
    await worker.run_once()
    await wait_until(lambda: worker.free == worker.concurrency)

    settled = await app.get(written.id)

    assert settled.status == RunStatus.PENDING
    assert settled.attempts == 0, "the attempt it never had is given back"
    assert await app.store.reclaim(now() + timedelta(hours=1)) == 0, "so nothing is left for a reclaim to give up on"
    assert (await app.get(written.id)).status == RunStatus.PENDING, "and the run is still there for the replica that knows the name"


async def test_a_run_evicted_between_the_claim_and_the_read_does_not_end_the_pass(app):
    """the script that claims checks the run is still there, and the read after it did not — one evicted hash raised out of the whole pass and the runs beside it were never started"""
    from fast_tasks.store.redis import RedisStore

    if not isinstance(app.store, RedisStore):
        pytest.skip("only redis keeps the run and the lane it is queued in as two separate things")

    @app.task("work")
    async def work():
        return None

    written = await app.enqueue("work")
    claiming = app.store.scripts["claim"]

    async def evicting(**arguments):
        taken = await claiming(**arguments)

        # the eviction lands after the script took the run and before the read that builds it
        await app.store.client.delete(app.store.run_key(written.id))

        return taken

    app.store.scripts["claim"] = evicting

    assert await app.store.claim("a-worker", ("default",), 10, timedelta(seconds=60), now()) == [], "nothing was handed out and nothing raised"


async def test_a_lane_names_the_same_instant_a_run_was_written_for(app):
    """`timestamp()` on a value with no zone reads it as the wall clock of whoever wrote it, so a run queued in one zone came due in another"""
    from fast_tasks.store.redis import RedisStore

    if not isinstance(app.store, RedisStore):
        pytest.skip("only redis scores a queue by when a run is due")

    overdue = now().replace(tzinfo=None) - timedelta(hours=1)
    written = await app.store.add(Run(name="work", due_at=overdue))

    assert await app.store.client.zscore(f"{app.store.prefix}:queue:default:0", written.id) == overdue.replace(tzinfo=timezone.utc).timestamp()
    assert len(await app.store.claim("a-worker", ("default",), 1, timedelta(seconds=60), now())) == 1, "an hour overdue is due"


async def test_a_run_a_pruning_already_dropped_is_not_counted_as_depth(app):
    """a scan names what a pruning beside it has already deleted, and reading fields a hash no longer has counted a run that is gone"""
    from fast_tasks.store.redis import RedisStore

    if not isinstance(app.store, RedisStore):
        pytest.skip("only redis counts by walking what it owns")

    @app.task("work")
    async def work():
        return None

    await app.enqueue("work")

    naming = app.store.client.scan_iter

    async def naming_a_ghost(**keywords):
        async for name in naming(**keywords):
            yield name

        yield f"{app.store.prefix}:run:gone".encode()

    app.store.client.scan_iter = naming_a_ghost

    assert await app.count() == 1, "the one run that is there, and not the one that was dropped mid scan"


def test_a_run_allowed_a_thousand_attempts_still_works_out_a_delay():
    """doubling is an integer that outgrows a float long before the ceiling has anything to say, and the retry raised instead of waiting"""
    assert delay_for(RetryPolicy.EXPONENTIAL, 5.0, 1100, 0.0, 3600.0) == 3600.0
    assert delay_for(RetryPolicy.EXPONENTIAL_JITTER, 5.0, 4000, 0.5, 60.0) <= 60.0
    assert delay_for(RetryPolicy.EXPONENTIAL, 5.0, 3, 0.0, 3600.0) == 20.0, "and the growth below the ceiling is untouched"


async def test_a_write_the_database_refused_for_its_own_reasons_is_not_a_key_that_was_taken(app):
    """a run with no key never raced anybody for one, and reading every refusal as a duplicate handed the caller a run belonging to somebody else"""
    from fast_tasks.store.sqlalchemy import SqlAlchemyStore

    if not isinstance(app.store, SqlAlchemyStore):
        pytest.skip("only a database refuses a write for reasons of its own")

    somebody_else = await app.store.add(Run(name="work", payload={"who": "somebody else entirely"}))

    with pytest.raises(IntegrityError):
        await app.store_once(Run(name=None, payload={"who": "mine"}))

    assert (await app.get(somebody_else.id)).payload == {"who": "somebody else entirely"}, "and the run of somebody else was never handed out as this one"


async def test_a_caller_that_changes_the_run_it_enqueued_does_not_change_the_row(app):
    """every other store writes the run out and keeps nothing of the object, and one that kept it let a caller edit a queued run from the outside"""
    mine = Run(name="work", payload={"who": "paulo"})
    written = await app.store.add(mine)

    mine.payload["who"] = "changed after the store took it"
    mine.status = RunStatus.DONE

    stored = await app.get(written.id)

    assert stored.payload == {"who": "paulo"}
    assert stored.status == RunStatus.PENDING


async def test_a_reclaim_leaves_alone_a_run_somebody_claimed_while_it_was_waiting(app, monkeypatch):
    """the batch of expired leases is named by a subselect the database reads once and then holds, and the update asserted nothing about those rows — so a row the statement waited on a lock for was written whatever it had since become, and a worker still working lost the run out from under it"""
    from fast_tasks.store import sqlalchemy as backend

    if not isinstance(app.store, backend.SqlAlchemyStore):
        pytest.skip("only a database names a batch in one statement and writes it in another")

    written = await app.store.add(Run(name="work", max_attempts=5))
    working = await app.store.claim("worker-still-alive", ("default",), 1, timedelta(seconds=600), now())

    assert len(working) == 1, "a live worker holds it, on a lease with ten minutes left"

    # the batch a sweep already read still names this run, which is what it was before the live worker took it over
    monkeypatch.setattr(backend, "limited", lambda condition, size: backend.runs.c.id.in_([written.id]))

    assert await app.store.reclaim(now()) == 0, "no lease has run out, so there is nothing to take back"

    held = await app.get(written.id)

    assert held.status == RunStatus.RUNNING
    assert held.worker == "worker-still-alive", "the run stayed with the worker that is still working on it"


async def test_a_handler_reaching_for_a_name_that_is_not_there_is_a_handler_that_failed(app):
    """`UnknownTask` means this worker does not declare the name, and only the lookup can say that. read off the handler instead, a task fanning out to a name nobody registered was handed back with its attempt given back — for ever, at every poll, repeating everything it did before it raised"""
    ran = []

    @app.task("fan_out", max_attempts=1, retry_delay=0)
    async def fan_out():
        ran.append(1)

        await app.enqueue("a_name_nobody_declared")

    written = await app.enqueue("fan_out")
    worker = Worker(app, poll=0.05)

    for _ in range(4):
        await worker.run_once()
        await worker.drain()

    settled = await app.get(written.id)

    assert len(ran) == 1, "the attempt was spent, instead of the side effect repeating for ever"
    assert settled.status == RunStatus.FAILED
    assert settled.error_type == "UnknownTask"
    assert settled.attempts == 1


def test_a_herd_that_doubled_its_way_past_the_ceiling_is_still_spread():
    """the ceiling was applied to the drawn delay instead of to the growth under it, so every run that saturated it worked the very same wait out from the very same numbers — which is the herd this policy exists to break up, handed back whole"""
    drawn = {delay_for(RetryPolicy.EXPONENTIAL_JITTER, 5.0, 40, 0.5, 60.0) for _ in range(50)}

    assert len(drawn) > 1, "they were spread, and every one of them is far past the ceiling"
    assert max(drawn) <= 60.0, "and no wait is ever longer than the ceiling"
    assert min(drawn) >= 40.0, "and none is drawn further under it than the jitter asked for"


async def test_the_lease_a_claim_hands_out_runs_from_the_instant_the_run_was_taken(app):
    """the pass read the instant once and claimed with it after reclaiming, pruning and materializing — three round trips — so a slow pass handed out a lease it had already spent most of, and the next reclaim took the run back while it was still being worked"""
    waiting = 0.3

    @app.task("work")
    async def work():
        return None

    original = type(app.store).reclaim

    async def slowly(self, moment):
        await asyncio.sleep(waiting)

        return await original(self, moment)

    type(app.store).reclaim = slowly

    try:
        await app.enqueue("work")

        worker = Worker(app, lease=timedelta(seconds=60))
        began = now()
        claimed = (await worker.run_once())[0]

        await worker.drain()
    finally:
        type(app.store).reclaim = original

    assert claimed.started_at >= began + timedelta(seconds=waiting - 0.05), "the claim was made with the instant it happened, and not with the one the pass opened with"


@pytest.mark.parametrize("options,reason", [({"name": "x" * (TASK_NAME_LIMIT + 1)}, "the name of"), ({"name": "work", "queue": "q" * (QUEUE_LIMIT + 1)}, "the queue"), ({"name": "x" * TASK_NAME_LIMIT, "every": 60}, "the key each slot")])
def test_a_name_a_store_could_never_hold_is_refused_where_it_is_declared(app, options, reason):
    """the worker name has been sized against the column it goes in since the beginning, and the task name, the queue and the key never were. a name past the column is a write the database refuses, and on one that is not strict a name quietly cut short — where two slots of two different minutes become one run"""
    with pytest.raises(QueueError, match=reason):
        app.task(**options)(lambda: None)


def test_a_task_that_fits_every_column_it_touches_is_declared(app):
    """the limits are the columns and not a guess at them, so the longest name a store can hold is a name that works"""
    assert app.task("x" * TASK_NAME_LIMIT, queue="q" * QUEUE_LIMIT)(lambda: None)

    # the slot key is the name and the widest instant one is ever named for, so a recurring task has less of the column to itself than a plain one
    assert app.task("y" * (KEY_LIMIT - len(occurrence_key("", WIDEST_SLOT))), every=60)(lambda: None)


async def test_a_key_a_store_could_never_hold_is_refused_at_the_call(app):
    """a caller keying a run by something long — an url, a rendered template — had it cut short by the database, and the run that came back was somebody else's"""

    @app.task("work")
    async def work():
        return None

    with pytest.raises(QueueError, match="the key"):
        await app.enqueue("work", key="k" * (KEY_LIMIT + 1))

    assert await app.enqueue("work", key="k" * KEY_LIMIT), "and the longest key a store can hold is a key that works"


async def test_the_columns_a_store_builds_are_the_limits_the_app_refuses_by(app):
    """two numbers written down twice drift, and the day they do the app refuses what the store would have taken or takes what it will not hold"""
    from fast_tasks.store.sqlalchemy import SqlAlchemyStore, runs

    if not isinstance(app.store, SqlAlchemyStore):
        pytest.skip("only a database sizes a column")

    assert (runs.c.name.type.length, runs.c.queue.type.length, runs.c.key.type.length) == (TASK_NAME_LIMIT, QUEUE_LIMIT, KEY_LIMIT)
    assert runs.c.worker.type.length == WORKER_NAME_LIMIT


async def test_a_reclaim_pass_takes_one_batch_however_many_ways_a_lease_can_end(app, monkeypatch):
    """the two halves of a sweep each took a batch of their own, so a pass over a cluster that died holding a mix of both wrote twice what the constant says one pass writes — and the constant is there to bound how long the store is held"""
    from fast_tasks.store import sqlalchemy as backend

    if not isinstance(app.store, backend.SqlAlchemyStore):
        pytest.skip("only this store takes back what is spent and what is not in two statements")

    monkeypatch.setattr(backend, "RECLAIM_BATCH", 10)

    @app.task("spent", max_attempts=1)
    async def spent():
        return None

    @app.task("work", max_attempts=5)
    async def work():
        return None

    for _ in range(10):
        await app.enqueue("spent")

    for _ in range(5):
        await app.enqueue("work")

    await app.store.claim("a-cluster-that-died", ("default",), 15, timedelta(seconds=-1), now())

    assert await app.store.reclaim(now()) == 10, "one pass took one batch, and not one of each kind"
    assert await app.store.reclaim(now()) == 5, "and the pass after it took the rest"


async def test_a_herd_the_database_rolled_back_does_not_come_back_in_lockstep(monkeypatch):
    """the wait between tries was a fixed schedule every contender worked out from the same numbers, so the transactions innodb rolled back came back together, deadlocked on each other again and spent the whole budget doing it. measured against a hot queue with forty claims and a reclaim on the same rows, that left outcomes the store refused on every single run — and a refused outcome is work silently done again a lease later"""
    from fast_tasks.store import sqlalchemy as backend

    waits = []

    async def instead(seconds):
        waits.append(seconds)

    monkeypatch.setattr(backend.asyncio, "sleep", instead)

    async def deadlocking():
        raise DBAPIError("UPDATE", {}, Exception(1213, "Deadlock found when trying to get lock"))

    for _ in range(2):
        with pytest.raises(DBAPIError):
            await backend.under_contention(deadlocking)

    mine, theirs = waits[: backend.TRIES - 1], waits[backend.TRIES - 1 :]

    assert len(mine) == len(theirs) == backend.TRIES - 1
    assert mine != theirs, "two transactions rolled back by the same deadlock do not come back at the same instants"
    assert all(later > earlier for earlier, later in zip(mine, mine[1:])), "and each wait is longer than the one before it"
    assert sum(mine) >= 0.6, "the budget outlasts a burst of contention instead of adding a few milliseconds and giving up on it"


async def test_priority_orders_a_claim_across_every_queue_the_worker_serves(app):
    """the lanes were walked one queue at a time, so a worker serving two of them drained the first to its end — and handed out an ordinary run while an urgent one sat waiting next door"""
    await app.store.add(Run(name="ordinary", queue="reports", priority=0, due_at=now() - timedelta(minutes=5)))
    await app.store.add(Run(name="urgent", queue="email", priority=10, due_at=now()))

    taken = await app.store.claim("a-worker", ("reports", "email"), 1, timedelta(seconds=60), now())

    assert [run.name for run in taken] == ["urgent"], "a queue is where a run waits and never what orders it"


async def test_the_oldest_of_a_priority_goes_first_whichever_queue_it_waits_in(app):
    """one lane per queue answers which run of that queue is oldest, and nothing was merging the answers — so the order inside a priority was the order the queues were named in"""
    await app.store.add(Run(name="second", queue="email", priority=5, due_at=now() - timedelta(minutes=1)))
    await app.store.add(Run(name="first", queue="reports", priority=5, due_at=now() - timedelta(minutes=9)))

    taken = await app.store.claim("a-worker", ("email", "reports"), 2, timedelta(seconds=60), now())

    assert [run.name for run in taken] == ["first", "second"]


async def test_a_recurring_task_is_measured_by_the_longest_key_it_will_ever_write(app, monkeypatch):
    """the guard read the one slot the task wanted next, and an interval landing on microseconds every other slot goes on to write a key seven characters longer than that one — accepted where it was declared, and then refused by `build` on every pass of every worker, before anything was ever claimed"""
    from fast_tasks import app as application

    fits = "n" * (KEY_LIMIT - len(occurrence_key("", WIDEST_SLOT)))

    # the guard read the clock, so which of the two keys it measured was a coin toss, and this is the toss that let the longer one through. only what is declared under it is pinned, because materializing wants a clock that moves
    with monkeypatch.context() as clock:
        clock.setattr(application, "now", lambda: EPOCH + timedelta(seconds=0.5))

        with pytest.raises(QueueError, match="the key each slot"):
            app.task(fits + "n", every=0.5)(lambda: None)

        assert app.task(fits, every=0.5)(lambda: None), "and the longest name that fits every slot it writes is a name that works"

    # both widths the interval alternates between, every one of them a key the app writes instead of raising on
    for _ in range(4):
        await app.materialize()
        app.written.clear()

        await asyncio.sleep(0.3)

    assert await app.count() >= 2, "the slots really were written, on a name at the very edge of the column"


@pytest.mark.parametrize("queues,reason", [((), "serving no queues"), (("q" * (QUEUE_LIMIT + 1),), "no task could ever be declared")])
def test_a_worker_that_could_never_be_served_is_refused_where_it_is_written(app, queues, reason):
    """a lease that has run out and a concurrency of zero were both refused, and a worker with nothing to serve was not — it came up, polled, claimed nothing and never said a word about why"""
    with pytest.raises(QueueError, match=reason):
        Worker(app, queues=queues)


@pytest.mark.parametrize("seconds", [0.001, 0.01, 0.1, 0.2, 0.3, 0.7, 1.0, 2.5])
def test_an_interval_always_names_a_slot_that_is_still_to_come(seconds):
    """the count of whole slots was carried back through a multiplication a float cannot undo, so the slot that came out was the instant it was asked after — the occurrence already written, and never the one after it"""
    moment = now()

    for _ in range(2000):
        due_at = Interval(seconds).next_after(moment)

        assert due_at > moment, f"an interval of {seconds}s answered the instant it was asked after"

        moment = due_at


def test_an_interval_no_store_could_tell_apart_is_refused_where_it_is_written(app):
    """every store keeps an instant to the microsecond, so a slot under one is the slot before it under another name — and counting in whole slots divides by a span that rounded to nothing"""
    with pytest.raises(QueueError, match="finer than the microsecond"):
        app.task("often", every=0.0000004)(lambda: None)

    assert Interval(0.000001), "and the finest span a store can still tell apart is one that works"
