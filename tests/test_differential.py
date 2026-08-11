"""one script of operations, answered by every store and compared against the same script answered in memory. what a store promises is the same wherever the rows live, and a suite written by hand only ever asks the questions somebody thought to ask — this asks the ones nobody did"""

import random
from datetime import timedelta

import pytest

from fast_tasks.clock import now
from fast_tasks.retry import RetryPolicy
from fast_tasks.run import Run, RunStatus
from fast_tasks.store.memory import MemoryStore

QUEUES = ("default", "email", "reports")

# every way a store is asked to change a run, drawn one at a time. writing and claiming are drawn twice as often, because everything else needs a run to work on
OPERATIONS = ("add", "add", "claim", "claim", "complete", "fail", "retry_later", "release", "cancel", "reclaim", "purge", "heartbeat", "find")

STEPS = 150

# what a run is drawn from, held out here so the call that writes one stays on a line
NAMES = ("work", "grüßen 😀")
PRIORITIES = (-5, 0, 3, 5)
TIMEOUTS = (None, 1.5)
POLICIES = tuple(RetryPolicy)
RESULTS = (None, {}, {"ok": True})


def readable(run: Run | None):
    """every field a store writes and reads back, because one it quietly forgets is a policy the worker stops honouring"""
    if run is None:
        return None

    return (run.name, run.queue, run.key, run.status.value, run.priority, run.attempts, run.max_attempts, run.timeout, run.retry_policy.value, run.retry_delay, run.max_retry_delay, run.worker, run.error, run.error_type, run.result, run.payload)


async def written_by(store, dice: random.Random, base, step: int):
    due_at = base + timedelta(seconds=dice.choice([-30, 0, 5, 500]))
    key = dice.choice([None, f"slot-{dice.randint(0, 4)}"])
    payload = {"step": step, "who": "münchen 😀"}

    return await store.add(Run(name=dice.choice(NAMES), queue=dice.choice(QUEUES), key=key, priority=dice.choice(PRIORITIES), max_attempts=dice.randint(1, 3), due_at=due_at, timeout=dice.choice(TIMEOUTS), retry_policy=dice.choice(POLICIES), retry_delay=2.5, max_retry_delay=99.5, payload=payload))


async def closing(store, choice: str, run_id, worker: str, moment, dice: random.Random) -> bool:
    if choice == "complete":
        return await store.complete(run_id, worker, moment, dice.choice(RESULTS))

    if choice == "fail":
        return await store.fail(run_id, worker, moment, "boom", "Error")

    if choice == "retry_later":
        return await store.retry_later(run_id, worker, moment + timedelta(seconds=5), "boom", "Error")

    if choice == "release":
        return await store.release(run_id, worker, moment + timedelta(seconds=5), "gone", "UnknownTask")

    if choice == "cancel":
        return await store.cancel(run_id, moment)

    return await store.heartbeat(run_id, worker, timedelta(seconds=60), moment)


async def script(store, seed: int, base):
    """the same sequence every store is asked, with the ids each of them hands out mapped back to the order they were written in — because an id is the one thing a store is allowed to name for itself"""
    dice = random.Random(seed)
    seen = []
    trail = []

    for step in range(STEPS):
        moment = base + timedelta(seconds=step)
        choice = dice.choice(OPERATIONS)

        if choice == "add":
            written = await written_by(store, dice, base, step)
            trail.append((step, choice, written is not None))

            if written is not None:
                seen.append(written.id)

            continue

        if choice == "claim":
            served = tuple(dice.sample(QUEUES, dice.randint(1, 3)))
            taken = await store.claim(f"worker-{dice.randint(1, 3)}", served, dice.randint(1, 4), timedelta(seconds=dice.choice([-1, 60])), moment)
            trail.append((step, choice, [readable(run) for run in taken]))

            continue

        if choice == "reclaim":
            trail.append((step, choice, await store.reclaim(moment)))

            continue

        if choice == "purge":
            trail.append((step, choice, await store.purge(moment, 50)))

            continue

        if choice == "find":
            trail.append((step, choice, readable(await store.find(f"slot-{dice.randint(0, 4)}"))))

            continue

        # everything left needs a run to work on, and it is drawn by the order it was written in so the same step names the same run in every store
        if not seen:
            continue

        index = dice.randrange(len(seen))
        trail.append((step, choice, index, await closing(store, choice, seen[index], f"worker-{dice.randint(1, 3)}", moment, dice)))

    return trail, [readable(await store.get(run_id)) for run_id in seen], await store.count(), [await store.count(status=status) for status in RunStatus], [await store.count(queue=queue) for queue in QUEUES]


@pytest.mark.parametrize("seed", range(8))
async def test_every_store_answers_the_same_script_the_same_way(store, seed):
    """a store that drifts from the others is a promise the library keeps on one backend and breaks on another, and nothing in an application would ever say which"""
    reference = MemoryStore()
    await reference.setup()

    base = now()

    assert await script(store, seed, base) == await script(reference, seed, base), f"{type(store).__name__} answered the script differently from the store the whole library is defined by"
