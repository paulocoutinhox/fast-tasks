from datetime import datetime, timedelta, timezone

import pytest

from fast_tasks.app import FastTasks, occurrence_key
from fast_tasks.clock import now
from fast_tasks.errors import QueueError, UnknownTask
from fast_tasks.retry import RetryPolicy
from fast_tasks.run import RunStatus


def declare(target, name="greet", **options):
    @target.task(name, **options)
    async def handler(**payload):
        return payload

    return handler


async def test_a_declared_task_carries_its_policy_into_every_run_of_it(app):
    declare(app, max_attempts=5, timeout=30, retry_policy=RetryPolicy.LINEAR, retry_delay=2, queue="email", priority=7)

    written = await app.enqueue("greet", who="paulo")

    assert (written.max_attempts, written.timeout, written.retry_policy, written.retry_delay) == (5, 30, RetryPolicy.LINEAR, 2)
    assert (written.queue, written.priority, written.payload) == ("email", 7, {"who": "paulo"})
    assert written.status == RunStatus.PENDING


async def test_a_name_means_one_thing(app):
    declare(app)

    with pytest.raises(QueueError, match="registered twice"):
        declare(app)


async def test_enqueueing_something_nobody_declared_is_refused_at_the_call(app):
    with pytest.raises(UnknownTask, match="nothing here is called"):
        await app.enqueue("ghost")


async def test_a_task_runs_on_one_clock(app):
    with pytest.raises(QueueError, match="one clock"):
        declare(app, every=30, cron="* * * * *")


async def test_an_immediate_run_is_due_the_moment_it_is_written(app):
    declare(app)

    assert (await app.enqueue("greet")).due_at <= now()


async def test_a_run_asked_for_later_waits(app):
    declare(app)
    when = now() + timedelta(hours=2)

    assert (await app.enqueue_at("greet", when)).due_at == when


async def test_a_key_makes_the_same_call_twice_one_run(app):
    declare(app)

    first = await app.enqueue("greet", key="welcome:7", who="paulo")
    second = await app.enqueue("greet", key="welcome:7", who="somebody else")

    assert second.id == first.id
    assert second.payload == {"who": "paulo"}, "the one that got there first is the one that stands"
    assert await app.count() == 1


async def test_a_priority_given_at_the_call_beats_the_one_the_job_declared(app):
    declare(app, priority=1)

    assert (await app.enqueue("greet", priority=9)).priority == 9


async def test_a_task_with_no_trigger_is_never_written_on_its_own(app):
    declare(app)

    assert await app.materialize() == []


async def test_an_interval_task_gets_its_next_slot_written(app):
    declare(app, every=30)

    written = await app.materialize(datetime(2026, 1, 1, 10, 0, 5, tzinfo=timezone.utc))

    assert [task.due_at for task in written] == [datetime(2026, 1, 1, 10, 0, 30, tzinfo=timezone.utc)]
    assert written[0].key == occurrence_key("greet", written[0].due_at)


async def test_a_cron_task_gets_its_next_slot_written(app):
    declare(app, cron="0 4 * * *")

    written = await app.materialize(datetime(2026, 1, 1, 10, 0, tzinfo=timezone.utc))

    assert [task.due_at for task in written] == [datetime(2026, 1, 2, 4, 0, tzinfo=timezone.utc)]


async def test_the_same_worker_does_not_ask_the_store_about_a_slot_it_already_wrote(app):
    declare(app, every=30)
    moment = datetime(2026, 1, 1, 10, 0, 5, tzinfo=timezone.utc)

    assert len(await app.materialize(moment)) == 1
    assert await app.materialize(moment) == []


async def test_ten_workers_writing_the_same_slot_leave_one_run(store):
    """this is the whole answer to running the same code on ten machines, and it is a unique key"""
    moment = datetime(2026, 1, 1, 10, 0, 5, tzinfo=timezone.utc)
    written = []

    for _ in range(10):
        worker_queue = FastTasks(store)
        declare(worker_queue, every=30)
        written.extend(await worker_queue.materialize(moment))

    assert len(written) == 1
    assert await store.count() == 1


async def test_a_slot_that_already_ran_is_not_written_again(app):
    declare(app, every=30)
    moment = datetime(2026, 1, 1, 10, 0, 5, tzinfo=timezone.utc)

    await app.materialize(moment)

    # another worker of the same fleet, which has written nothing yet and still must not duplicate the slot
    twin = FastTasks(app.store)
    declare(twin, every=30)

    assert await twin.materialize(moment) == []


async def test_the_app_answers_for_what_it_wrote(app):
    declare(app)
    written = await app.enqueue("greet", key="one")

    assert (await app.get(written.id)).id == written.id
    assert (await app.find("one")).id == written.id
    assert await app.count(status=RunStatus.PENDING) == 1
    assert await app.cancel(written.id) is True
    assert (await app.get(written.id)).status == RunStatus.CANCELED


async def test_an_argument_named_like_an_option_is_still_reachable(app):
    """a task whose payload has a `key` or a `when` has to be enqueueable, and the whole-payload form is how"""
    declare(app, "carry")

    written = await app.enqueue("carry", payload={"key": "not the idempotency key", "when": "not the time", "priority": "not the order", "name": "not the task"})

    assert written.payload == {"key": "not the idempotency key", "when": "not the time", "priority": "not the order", "name": "not the task"}
    assert written.key is None
    assert written.priority == 0


async def test_a_payload_given_whole_and_in_pieces_at_once_is_refused(app):
    declare(app, "carry")

    with pytest.raises(QueueError, match="whole and in pieces"):
        await app.enqueue("carry", payload={"a": 1}, b=2)
