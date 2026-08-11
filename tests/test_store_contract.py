"""one suite, every store. what a store promises is the same wherever the rows live, and this is what keeps that true"""

from datetime import timedelta

import pytest

from fast_tasks.clock import now
from fast_tasks.retry import RetryPolicy
from fast_tasks.run import Run, RunStatus
from fast_tasks.store.base import Store

LEASE = timedelta(seconds=60)
QUEUES = ("default",)


def a_run(**overrides) -> Run:
    return Run(**{"name": "greet", "payload": {"who": "paulo"}} | overrides)


async def claim_one(store, worker="worker-1", moment=None):
    taken = await store.claim(worker, QUEUES, 1, LEASE, moment or now())

    return taken[0] if taken else None


async def test_a_written_run_comes_back_with_an_id(store):
    written = await store.add(a_run())

    assert written.id is not None
    assert (await store.get(written.id)).payload == {"who": "paulo"}


async def test_a_run_nobody_wrote_is_nothing(store):
    assert await store.get(999999) is None
    assert await store.find("nothing") is None


async def test_a_key_is_taken_once_and_the_second_writer_is_told_so(store):
    assert await store.add(a_run(key="slot")) is not None
    assert await store.add(a_run(key="slot")) is None
    assert await store.count() == 1


async def test_a_run_without_a_key_is_never_confused_with_another(store):
    await store.add(a_run())
    await store.add(a_run())

    assert await store.count() == 2


async def test_a_run_is_found_by_the_key_that_made_it_unique(store):
    written = await store.add(a_run(key="slot"))

    assert (await store.find("slot")).id == written.id


async def test_claiming_marks_the_run_as_this_worker_running_it(store):
    await store.add(a_run())
    taken = await claim_one(store)

    assert taken.status == RunStatus.RUNNING
    assert taken.worker == "worker-1"
    assert taken.attempts == 1
    assert taken.started_at is not None
    assert taken.lease_until is not None


async def test_two_workers_never_get_the_same_run(store):
    await store.add(a_run())

    assert await claim_one(store, "worker-1") is not None
    assert await claim_one(store, "worker-2") is None


async def test_a_run_that_is_not_due_yet_is_left_alone(store):
    await store.add(a_run(due_at=now() + timedelta(hours=1)))

    assert await claim_one(store) is None


async def test_a_run_of_another_queue_is_left_alone(store):
    await store.add(a_run(queue="email"))

    assert await claim_one(store) is None
    assert len(await store.claim("worker-1", ("email",), 1, LEASE, now())) == 1


async def test_a_claim_never_takes_more_than_it_was_asked_for(store):
    for _ in range(5):
        await store.add(a_run())

    assert len(await store.claim("worker-1", QUEUES, 2, LEASE, now())) == 2


async def test_priority_is_served_before_age(store):
    await store.add(a_run(payload={"who": "first"}))
    await store.add(a_run(payload={"who": "urgent"}, priority=10))

    assert (await claim_one(store)).payload == {"who": "urgent"}


async def test_the_older_of_two_equal_runs_goes_first(store):
    older = await store.add(a_run(due_at=now() - timedelta(minutes=5)))
    await store.add(a_run(due_at=now()))

    assert (await claim_one(store)).id == older.id


async def test_a_heartbeat_pushes_the_lease_of_the_worker_that_holds_it(store):
    await store.add(a_run())
    taken = await claim_one(store)

    assert await store.heartbeat(taken.id, "worker-1", timedelta(seconds=600), now()) is True
    assert (await store.get(taken.id)).lease_until > taken.lease_until


async def test_a_heartbeat_from_anybody_else_is_refused(store):
    await store.add(a_run())
    taken = await claim_one(store)

    assert await store.heartbeat(taken.id, "worker-2", LEASE, now()) is False


async def test_completing_closes_the_run_and_lets_the_lease_go(store):
    await store.add(a_run())
    taken = await claim_one(store)

    assert await store.complete(taken.id, "worker-1", now(), {"sent": True}) is True

    settled = await store.get(taken.id)

    assert settled.status == RunStatus.DONE
    assert settled.result == {"sent": True}
    assert settled.worker is None
    assert settled.lease_until is None
    assert settled.finished_at is not None


async def test_failing_closes_the_run_with_what_broke(store):
    await store.add(a_run())
    taken = await claim_one(store)

    assert await store.fail(taken.id, "worker-1", now(), "smtp refused", "SMTPError") is True

    settled = await store.get(taken.id)

    assert settled.status == RunStatus.FAILED
    assert (settled.error, settled.error_type) == ("smtp refused", "SMTPError")


async def test_a_retry_puts_the_run_back_with_a_time_to_come_back_at(store):
    await store.add(a_run(max_attempts=3))
    taken = await claim_one(store)
    later = now() + timedelta(minutes=5)

    assert await store.retry_later(taken.id, "worker-1", later, "smtp refused", "SMTPError") is True

    waiting = await store.get(taken.id)

    assert waiting.status == RunStatus.PENDING
    assert waiting.worker is None
    assert waiting.attempts == 1, "an attempt that happened is never taken back"
    assert await claim_one(store) is None, "it comes back when it is due and not before"


@pytest.mark.parametrize("closing", ["complete", "fail", "retry_later"])
async def test_nobody_closes_a_run_they_do_not_hold(store, closing):
    await store.add(a_run())
    taken = await claim_one(store)
    arguments = {"complete": (now(), None), "fail": (now(), "boom", "Error"), "retry_later": (now(), "boom", "Error")}[closing]

    assert await getattr(store, closing)(taken.id, "worker-2", *arguments) is False
    assert (await store.get(taken.id)).status == RunStatus.RUNNING


async def test_a_lease_that_ran_out_puts_the_run_back_in_the_queue(store):
    await store.add(a_run(max_attempts=3, due_at=now() - timedelta(hours=2)))
    taken = await claim_one(store, moment=now() - timedelta(hours=1))

    assert await store.reclaim(now()) == 1

    back = await store.get(taken.id)

    assert back.status == RunStatus.PENDING
    assert back.worker is None
    assert await claim_one(store) is not None


async def test_a_lease_that_ran_out_with_no_attempts_left_ends_the_run(store):
    await store.add(a_run(max_attempts=1, due_at=now() - timedelta(hours=2)))
    taken = await claim_one(store, moment=now() - timedelta(hours=1))

    assert await store.reclaim(now()) == 1

    dead = await store.get(taken.id)

    assert dead.status == RunStatus.FAILED
    assert dead.error_type == "LeaseExpired"


async def test_a_lease_still_good_is_left_where_it_is(store):
    await store.add(a_run())
    await claim_one(store)

    assert await store.reclaim(now()) == 0


async def test_a_run_that_never_started_can_be_called_off(store):
    written = await store.add(a_run())

    assert await store.cancel(written.id, now()) is True
    assert (await store.get(written.id)).status == RunStatus.CANCELED
    assert await claim_one(store) is None


async def test_a_run_already_going_is_too_late_to_call_off(store):
    written = await store.add(a_run())
    await claim_one(store)

    assert await store.cancel(written.id, now()) is False


async def test_calling_off_something_that_is_not_there_answers_false(store):
    assert await store.cancel(999999, now()) is False


async def test_the_depth_of_the_queue_is_countable_by_state_and_by_queue(store):
    await store.add(a_run())
    await store.add(a_run(queue="email"))
    await claim_one(store)

    assert await store.count() == 2
    assert await store.count(status=RunStatus.RUNNING) == 1
    assert await store.count(status=RunStatus.PENDING) == 1
    assert await store.count(queue="email") == 1
    assert await store.count(status=RunStatus.PENDING, queue="email") == 1


async def test_every_method_of_the_interface_is_answered(store):
    """a store that forgets one of these fails here rather than at three in the morning"""
    promised = {name for name in Store.__abstractmethods__}

    assert promised <= set(dir(store))
    assert not [name for name in promised if getattr(type(store), name) is getattr(Store, name)]


async def test_a_worker_serving_two_queues_takes_from_both(store):
    await store.add(a_run(queue="email"))
    await store.add(a_run(queue="default"))

    assert len(await store.claim("worker-1", ("default", "email"), 2, LEASE, now())) == 2


async def test_priority_is_served_before_age_across_a_deep_backlog(store):
    """the urgent one is written last and comes back first, however much is already waiting in front of it"""
    for index in range(30):
        await store.add(a_run(payload={"who": f"waiting-{index}"}, due_at=now() - timedelta(minutes=30 - index)))

    await store.add(a_run(payload={"who": "urgent"}, priority=5))

    assert (await claim_one(store)).payload == {"who": "urgent"}


async def test_a_run_that_came_back_from_a_retry_keeps_its_place_in_the_priority_order(store):
    await store.add(a_run(payload={"who": "urgent"}, priority=5, max_attempts=3))

    taken = await claim_one(store)
    await store.retry_later(taken.id, "worker-1", now() - timedelta(seconds=1), "boom", "Error")
    await store.add(a_run(payload={"who": "ordinary"}))

    assert (await claim_one(store)).payload == {"who": "urgent"}


async def test_every_field_of_a_run_survives_the_trip_to_the_store_and_back(store):
    """a field the store forgets is a policy the worker silently stops honouring"""
    written = await store.add(a_run(key="whole", priority=3, max_attempts=7, timeout=42.5, retry_policy=RetryPolicy.LINEAR, retry_delay=2.5, max_retry_delay=120.0))
    read = await store.get(written.id)

    assert (read.name, read.queue, read.key, read.payload) == ("greet", "default", "whole", {"who": "paulo"})
    assert (read.priority, read.max_attempts, read.timeout) == (3, 7, 42.5)
    assert (read.retry_policy, read.retry_delay, read.max_retry_delay) == (RetryPolicy.LINEAR, 2.5, 120.0)
    assert read.status == RunStatus.PENDING
    assert read.attempts == 0
