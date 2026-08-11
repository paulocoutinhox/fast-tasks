"""the same script of operations against every store, compared field by field"""

import random
from datetime import timedelta

import pytest

from fast_tasks.clock import now
from fast_tasks.retry import RetryPolicy
from fast_tasks.run import Run, RunStatus

LEASE = timedelta(seconds=60)
QUEUES = ("default", "email")

OPERATIONS = ["add", "claim", "complete", "fail", "retry_later", "release", "cancel", "reclaim", "purge", "heartbeat"]


def readable(run):
    if run is None:
        return None

    return (run.name, run.queue, run.key, run.status.value, run.priority, run.attempts, run.max_attempts, run.worker, run.error, run.error_type, run.result, run.payload)


async def snapshot(store, seen):
    return [readable(await store.get(run_id)) for run_id in seen]


async def script(store, seed):
    """one deterministic sequence of everything a store can be asked, with the ids it hands out mapped back to the order they were written in"""
    dice = random.Random(seed)
    base = now()
    seen = []
    trail = []

    for step in range(120):
        moment = base + timedelta(seconds=step)
        choice = dice.choice(OPERATIONS)

        if choice == "add":
            key = dice.choice([None, f"slot-{dice.randint(0, 3)}"])
            written = await store.add(Run(name="work", queue=dice.choice(QUEUES), key=key, priority=dice.choice([-5, 0, 5]), max_attempts=dice.randint(1, 3), due_at=base, retry_policy=RetryPolicy.FIXED, payload={"step": step}))
            trail.append(("add", written is not None))

            if written is not None:
                seen.append(written.id)

            continue

        if choice == "claim":
            taken = await store.claim(f"worker-{dice.randint(1, 2)}", QUEUES, dice.randint(1, 3), timedelta(seconds=dice.choice([-1, 60])), moment)
            trail.append(("claim", [readable(run) for run in taken]))

            continue

        if choice in ("reclaim", "purge"):
            trail.append((choice, await getattr(store, choice)(moment, 50) if choice == "purge" else await store.reclaim(moment)))

            continue

        if not seen:
            continue

        run_id = dice.choice(seen)
        worker = f"worker-{dice.randint(1, 2)}"

        if choice == "complete":
            trail.append((choice, await store.complete(run_id, worker, moment, dice.choice([None, {"ok": True}]))))
        elif choice == "fail":
            trail.append((choice, await store.fail(run_id, worker, moment, "boom", "Error")))
        elif choice == "retry_later":
            trail.append((choice, await store.retry_later(run_id, worker, moment, "boom", "Error")))
        elif choice == "release":
            trail.append((choice, await store.release(run_id, worker, moment, "gone", "UnknownTask")))
        elif choice == "cancel":
            trail.append((choice, await store.cancel(run_id, moment)))
        elif choice == "heartbeat":
            trail.append((choice, await store.heartbeat(run_id, worker, LEASE, moment)))

    return trail, await snapshot(store, seen), await store.count(), [await store.count(status=status) for status in RunStatus]


@pytest.mark.parametrize("seed", range(12))
async def test_every_store_answers_the_same_script_the_same_way(store, seed, differential):
    """a store that drifts from the others is a policy the worker honours on one backend and not on another"""
    answered = await script(store, seed)
    reference = differential.setdefault(seed, (type(store).__name__, answered))

    assert answered == reference[1], f"{type(store).__name__} answered differently from {reference[0]}"
