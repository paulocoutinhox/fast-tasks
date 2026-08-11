# Fast Tasks

Standing context for anybody — human or model — working on this repository. Read it before writing a
line, and keep it true when the code moves.

---

## 1. What this is

An asynchronous task queue **and** scheduler for Python, where one task runs on exactly one worker.
Pure `asyncio`, no broker, no scheduler process, no leader election, no external coordination.

Everything reduces to a single row: **a run that is due, which exactly one worker claims.** An
immediate task, an interval, a fixed datetime and a cron expression differ only in *when the next run
is written*, and in nothing else.

The core package has **zero dependencies**. SQLAlchemy and Redis are optional extras.

| Question | Answer |
| --- | --- |
| who runs a due run? | whoever wins a conditional `UPDATE ... WHERE status = 'pending'` — the store decides |
| what stops ten workers writing ten copies of the 04:00 run? | a unique key per occurrence, `name@2026-08-01T04:00:00+00:00` |
| what happens when a worker dies mid-run? | its lease runs out and the run goes back to the queue |
| what elects the scheduler? | nothing — every worker writes the next occurrence, and the key leaves one |

### What it deliberately does not do

- It is not a broker. Delivery is polled, so a run becomes due within one poll interval, not within a microsecond.
- It does not spread one run across workers. One run, one worker.
- It is not a result store. A run holds a small JSON result; anything bigger belongs where your data lives.
- It is **at least once**, like every queue that survives a power cut. Handlers must be idempotent.

---

## 2. Layout

```
src/fast_tasks/
  __init__.py        empty, always — nothing is ever placed here
  app.py             FastTasks: the registry of tasks and the entry point for enqueueing
  worker.py          Worker: the polling loop, the lease heartbeat, the hooks
  run.py             Run dataclass and RunStatus
  task.py            Task dataclass (frozen)
  trigger.py         Trigger, Interval, Cron
  cron.py            POSIX five-field parser and the next-match search
  retry.py           RetryPolicy and the delay arithmetic
  clock.py           now(), as_utc(), naive_utc(), EPOCH — the only place time is decided
  errors.py          QueueError, UnknownTask, PermanentError, CronError
  fastapi.py         lifespan_for(worker)
  store/
    __init__.py      empty, always
    base.py          the abstract Store contract and the constants every store shares
    memory.py        MemoryStore
    sqlalchemy.py    SqlAlchemyStore (PostgreSQL, MySQL, SQLite)
    redis.py         RedisStore (Lua scripts)

tests/               pytest, asyncio_mode=auto, parametrized over every reachable store
docs/                the prose, kept honest by tests/test_docs.py
```

`src` layout, built with hatchling. `pyproject.toml` is the single source of tooling config.

---

## 3. The domain model

### Task — what a name means

Frozen dataclass. Declared once, at import time, and never mutated.

```
name, handler, queue, trigger, max_attempts, timeout,
retry_policy, retry_delay, max_retry_delay, priority
```

The **name** is what travels in the store, so it stays stable while the function behind it moves, is
renamed or changes module. `FastTasks.register` refuses a duplicate name.

### Run — one execution of one task

The only thing a worker ever claims. Every field of it is written by the store and read back
unchanged, and a field a store forgets is a policy the worker silently stops honouring.

```
name, queue, payload, key, status, priority, due_at,
attempts, max_attempts, timeout, retry_policy, retry_delay, max_retry_delay,
worker, lease_until, created_at, started_at, finished_at,
result, error, error_type, id
```

`RunStatus` is `pending | running | done | failed | canceled`. `SETTLED` is the last three — nothing
claims a run in one of them again.

`Run.exhausted` is `attempts >= max_attempts`.

### Trigger — when the next run is written

- `Interval(seconds)` — slots counted **from the unix epoch**, never from process start, so every worker of every machine names the same slot.
- `Cron(expression)` — five POSIX fields, rounded to the minute.

Both validate in `__post_init__`, so a bad trigger raises where it is declared and not on the night it
would first have run.

### Store — where runs live

Abstract base with **fourteen** methods. One rule runs through all of it:

> **Every method that changes a run is conditional on the state that run was in.**

That is what makes two workers safe without a lock anywhere. A store that answers "changed" for a row
it did not change breaks the guarantee for everybody.

| Method | Conditional on |
| --- | --- |
| `setup` | — builds what the store needs, and does nothing when it is already there |
| `add` | the key is free; answers `None` when it is taken |
| `claim` | `status = pending` and `due_at <= moment` and the queue matches |
| `heartbeat` | `worker` is the caller and `status = running` |
| `complete` | same |
| `fail` | same |
| `retry_later` | same — the attempt that happened stands |
| `release` | same — the attempt is **given back** |
| `reclaim` | `status = running` and the lease has run out |
| `cancel` | `status = pending` |
| `get` / `find` / `purge` / `count` | reads and housekeeping |

`retry_later` and `release` are the same write with one difference, and it matters: a store that
treats them as one method spends a run's whole retry budget on a rolling deploy.

---

## 4. The flow

### One worker pass — `Worker.run_once`

```
reclaim  → what a dead worker left goes back to the queue, or fails for good
tidy     → prune settled runs older than `keep`, once an hour, a batch at a time
materialize → write the next slot of every recurring task
claim    → take up to `free` due runs and start each in its own asyncio task
```

`run_once` returns what it claimed and does **not** wait for it. `drain` is what waits. Together they
are the whole of `run`, and they are what a test uses instead of sleeping.

`run` catches every `Exception` per pass and logs it: one bad minute must never end a worker.

### Running one run — `Worker.execute`

```
spawn a heartbeat task, pushing the lease every lease/HEARTBEAT_SHARE
announce on_start
look the name up
  UnknownTask       → store.release    → announce on_error   (the attempt is given back)
call the handler (coroutine directly, plain function via asyncio.to_thread), under `timeout`
  returned          → store.complete   → announce on_finish
  PermanentError    → store.fail       → announce on_error
  Exception         → retry_later or fail, by the policy → announce on_error
  SystemExit / KeyboardInterrupt → store.fail → announce on_error
  CancelledError    → re-raised untouched
ask the heartbeat to stop, and await it — never cancel it
```

Three rules hide in there and all three were bugs once:

- **The heartbeat is asked to stop and then awaited, never cancelled.** A command interrupted halfway leaves the connection with an answer nobody read, and whoever takes that connection next waits for a reply that already went somewhere else.
- **An outcome the store refused is not announced.** A worker whose lease ran out no longer holds the run; announcing anyway writes that run into an audit trail twice.
- **The lookup is outside the call, and `UnknownTask` is never caught around the handler.** Only the lookup can say this worker does not declare the name. A handler that raises it — one fanning out to a name nobody registered — is a handler with a bug, and reading that as a rolling deploy hands the run back with its attempt given back for ever, repeating on every poll everything the handler did before it raised.

### How a recurring task fires exactly once — `FastTasks.materialize`

Every worker, on every poll, computes the next slot of every recurring task and writes it under
`f"{name}@{due_at.isoformat()}"`. The key is unique, the store keeps one, the other nine writers are
told the key is taken. Then the run is claimed like any other.

`FastTasks.written` caches the slot each task was last asked for, so a poll every second is not a
cron expression walked every second. It is written **after** the store accepted the row — marking it
first would let a store that blinked drop that occurrence for good.

There is no catch-up. A fleet that was down for an hour writes the next slot, not the sixty it missed.

---

## 5. Invariants that must never be broken

1. **Every instant is UTC, decided in `clock.py` and nowhere else.** A naive datetime is the UTC instant it reads as. `datetime.timestamp()` on a naive value reads it as the local wall clock of whichever machine wrote it — the same value would name two instants in two stores. Never call `.timestamp()` directly in a store; go through `redis.stamp()` / `UtcDateTime`.
2. **A run is claimed by a conditional write, never by read-then-write.** Read candidates without a lock, take each with a write conditional on the state it was in, and let the store pick the winner.
3. **A key is what makes a run single.** Everything idempotent in this library — cron slots, interval slots, one-shots declared on every boot — is that one mechanism.
4. **A lease is what says a worker is still here.** Losing it is exactly how a run comes back. A heartbeat that could not reach the store is logged and never becomes the outcome of a run.
5. **Housekeeping is never the work.** A pruning the store refused must not cost the pass the claim it was on its way to make.
6. **Everything that decides ownership is conditional on the worker name**, and the name always fits `WORKER_NAME_LIMIT`.
7. **Nothing bounded is silently bounded.** Reclaim, purge and claim all take batches, and every batch size is a named constant with the reason beside it.

---

## 6. Tuning constants, and why each one exists

| Constant | Where | Value | Why |
| --- | --- | --- | --- |
| `CLAIM_SPREAD` | `store/base.py` | 5 | how many rows past the limit a claim looks at, so ten workers wanting one task do not all reach for the same row |
| `RECLAIM_BATCH` | `store/base.py` | 500 | a whole cluster dying expires everything at once; one statement over all of it holds the store while every other worker waits |
| `WORKER_NAME_LIMIT` | `store/base.py` | 128 | what every store sizes the `worker` column for — a name that does not fit is a worker whose every claim the database refuses while the process stays up |
| `HEARTBEAT_SHARE` | `worker.py` | 3 | the lease is pushed at a third of its span, well before it runs out |
| `PURGE_EVERY` | `worker.py` | 3600.0 | pruning happens on the hour, never on the poll |
| `PURGE_LIMIT` | `worker.py` | 1000 | a year that was never pruned is caught up over passes, not in one statement that holds the table |
| `MAX_DOUBLINGS` | `retry.py` | 64 | past this the exponent is a number a float no longer holds, and an ambitious `max_attempts` becomes a retry that raises instead of one that waits |
| `HORIZON` | `cron.py` | 8 × 366 | a leap day is the furthest any expression has to look, and the century that is not a leap year makes that gap eight years and not four |
| `CONTENDED` | `store/sqlalchemy.py` | {1205, 1213} | MySQL deadlock and lock-wait timeout — the documented handling is to ask again |
| `TRIES` / `BACKOFF` / `SPREAD` | `store/sqlalchemy.py` | 8 / 0.005 / 1.0 | short to begin with, doubling because contention comes in bursts, and drawn so a herd InnoDB rolled back does not come back in lockstep — measured, five linear tries left refused outcomes on every run of a hot queue |
| `TASK_NAME_LIMIT` / `KEY_LIMIT` / `QUEUE_LIMIT` | `store/base.py` | 255 / 255 / 64 | what every store sizes those columns for, refused where the task is declared — a value past the column is a write the database refuses, or one it quietly cuts short, and two slot keys cut to the same length are one run where there should be two |
| `PREFIX` | `store/redis.py` | `fast_tasks` | renames every key at once, so the store shares a Redis without ever meeting the application |

---

## 7. The stores

### MemoryStore

The whole library minus durability. Right for tests and for a single process, wrong for two. One
`asyncio.Lock` guards every mutation. It keeps a **deep copy** of every run, because a caller that
changes the run it enqueued must never change the row.

### SqlAlchemyStore — PostgreSQL, MySQL, SQLite

One table, `fast_tasks_run`, under **metadata of its own**, so it never creates or drops anything of
the application's.

Three indexes carry the whole load:

- `fast_tasks_run_ready` on `(queue, status, priority, due_at)` — every claim
- `fast_tasks_run_lease` on `(status, lease_until)` — every reclaim
- `fast_tasks_run_settled` on `(status, finished_at)` — every pruning

Things that are the way they are for a reason:

- **`UtcDateTime`** holds naive UTC and reads back aware UTC, because MySQL keeps no offset and a store that guesses one runs everything an hour late. On MySQL it becomes `DATETIME(fsp=6)` — MySQL **rounds** a datetime with no fractional precision, and a run due at `10:00:00.9` stored as `10:00:01` is a run nothing claims until the second turns.
- **`setup` runs `create_all` twice on failure.** `create_all` asks whether the table is there and then creates it, which is a question and a statement with a gap between them; ten replicas booting together used to leave eight of them dead on that gap. It reads no error message: with the table there the second call does nothing, and with it still missing it raises for whatever the real reason was.
- **`under_contention`** retries a write when the database asked for it, and lets everything else through untouched. InnoDB answers a duplicate two transactions race for with a deadlock as often as with a duplicate-key error.
- **`limited()`** puts the row limit in a derived table, because MySQL refuses a `LIMIT` directly inside an `IN`.
- **`take_back()` asserts the state again in the update, and never trusts the batch `limited()` handed it.** The subselect is read once and then held, so a row the statement waited on a lock for is written whatever it has since become. With the condition only in the subselect, a reclaim took back a run another worker had legitimately claimed in that moment — the same run on two workers at once, which is the one thing none of this may ever do.
- **`insert` only reads an `IntegrityError` as "the key is taken" when there is a key.** A keyless run never raced anybody for one, so swallowing that refusal would hand the caller somebody else's run.

SQLite across processes needs WAL and a busy timeout — see `docs/stores.md`.

### RedisStore

For whoever would rather not put a queue in their database. `setup` builds nothing; it registers the
scripts.

| Key | Holds |
| --- | --- |
| `fast_tasks:run:{id}` | the run itself, as a hash |
| `fast_tasks:queue:{queue}:{priority}` | one lane per priority, scored by `due_at` |
| `fast_tasks:priorities:{queue}` | which lanes exist, so a claim walks them highest first |
| `fast_tasks:leased` | what is running, scored by when its lease runs out |
| `fast_tasks:key:{key}` | the reservation that makes an occurrence single |
| `fast_tasks:sequence` | the id counter |
| `fast_tasks:settled` | what is over, scored by `finished_at`, so pruning is a range and not a scan |

**One lane per priority is why priority works.** A sorted set orders by one number, and a claim needs
the highest priority *among what is due* — two questions one score cannot answer.

Every mutation is Lua, so each is one atomic step on the server: `ADD`, `CLAIM`, `RECLAIM`, `SETTLE`,
`HEARTBEAT`, `CANCEL`, `PURGE`. A claim that read, decided and wrote in three round trips would let a
second worker in between them.

Hard constraints:

- **One instance, never Redis Cluster.** Every script builds the keys it touches from `ARGV` instead of declaring them in `KEYS`, because a claim discovers which run it took only while it runs. A replica for failover is fine; sharding is not.
- **`maxmemory-policy` must be `noeviction`.** An eviction takes the run hash without touching the lane it waits in. The store steps over that everywhere — the claim script, the read that follows it, and the reclaim all check the run is still there — but what an eviction costs is the run itself, which no code can give back.
- **A production client needs its own timeouts.** redis-py waits forever by default, and a connection whose network went away leaves a worker alive, polling nothing and answering nobody.
- **`count` is a scan.** Redis has no index over a hash. It is for an operator watching depth, not for a hot path, and it skips a run a pruning already dropped mid-scan.

---

## 8. Retries and failures

| What the handler did | What happens |
| --- | --- |
| returned | done, and a `dict` it returned is kept as the result |
| raised, attempts left | comes back, due after the policy's delay |
| raised, attempts spent | failed, with the message and the class that broke |
| raised `PermanentError` | failed **now**, however many attempts were allowed |
| ran past its `timeout` | the worker stops waiting and treats it as a retryable failure |
| carries a name this worker never declared | back to the queue, **and the attempt is given back** |
| the worker died | the lease runs out, and the run goes back to the queue or fails with `LeaseExpired` |
| raised `SystemExit` / `KeyboardInterrupt` | failed, and the worker keeps going |
| was cancelled | the cancellation is passed on, and the lease brings the run back |

Policies: `FIXED`, `LINEAR`, `EXPONENTIAL`, `EXPONENTIAL_JITTER`. No wait exceeds `max_retry_delay`.
The jitter fraction is **drawn per run** — a fixed multiplier, however large, hands the herd back
whole an hour later. The ceiling is what the draw happens **under**, and never what the drawn delay is
cut down to: a herd that doubled its way past the ceiling would otherwise work the very same wait out
from the very same numbers, which is the one case the policy exists for.

**A timeout stops the waiting, and only a coroutine stops the work.** Python cannot end a thread from
outside, so a plain handler carries on to its own end while the worker has already given up on it.

**`UnknownTask` is the rolling-deploy path, and it belongs to the lookup and never to the handler.**
The older replica meets runs the newer one enqueued for tasks it does not declare. The claim already
spent the attempt, so handing the run back is what gives it back — and a run left sitting on an
attempt it never used is one the first reclaim that meets it ends for good. A handler that raises
`UnknownTask` itself is an ordinary failure, retried and ended by the policy like any other.

---

## 9. Testing

```bash
make install     # venv + the package with its development tools
make servers     # redis on 6399, mysql on 3399, postgres on 5499
make test        # the suite
make coverage    # the suite with the 100% branch gate
make stress      # many machines against every server that answers, minutes and not seconds
make lint        # ruff check + black --check
make format      # ruff --fix, then black — in that order
make build       # wheel and sdist
```

Rules the suite enforces on itself:

- **Coverage stays at 100%, branches included.** It is a gate, not an aspiration.
- **Every store answers the same contract.** `tests/test_store_contract.py` is written against the interface and parametrized over every reachable store. Add a new backend to the fixture in `tests/conftest.py` and it inherits the whole suite.
- **A test never waits without a bound.** Use `wait_until` from `tests/conftest.py`. `pytest-timeout` is set to 120s with `timeout_method = "thread"`, so a hang becomes a failure with every stack dumped.
- **A store nobody can reach is not collected.** Memory and SQLite always run; Redis, MySQL and PostgreSQL join when their port answers. `make coverage` needs all three.
- **Run against a real MySQL before believing anything about MySQL.** Its `DATETIME` rounding is invisible to SQLite and PostgreSQL.
- **The stress suite is marked `stress` and left out of every ordinary run.** It is minutes rather than seconds, so a run of `make test` that included it is a run nobody waits for. Tracing costs an order of magnitude, which is why its load lives there and not in the graded suite.
- **100% coverage is not the same as 100% of the interleavings.** Two of the worst bugs found so far were invisible to a suite already at 100%: coverage counts lines a test reached, and neither of those was a line nobody reached. What found them was load and a second connection.

Files worth knowing:

| File | What it is for |
| --- | --- |
| `tests/test_store_contract.py` | the one suite every store answers |
| `tests/test_review.py` | one test per bug a line-by-line reading found, each named after what it would have caught |
| `tests/test_disasters.py` | clock skew, dying processes, handlers calling `sys.exit`, results the store cannot write |
| `tests/test_many_machines.py` | separate interpreters against one database, which is what containers are |
| `tests/test_many_workers.py`, `test_contention.py` | many workers in one process |
| `tests/test_fleet_stress.py` | `make stress` — many machines and many workers against a real server, with leases running out under them the whole time |
| `tests/test_docs.py` | the prose goes stale in silence, so this keeps it honest |
| `tests/fleet.py`, `machine.py`, `survivor.py` | the app, the store and the processes a fleet test spawns, against whichever url it is given |

**When you fix a bug, add the test that would have caught it to `tests/test_review.py`**, named after
the behaviour and not the fix, with a docstring saying what went wrong. Then confirm it fails against
the unfixed source — a test that passes either way pins nothing.

---

## 10. CI and releasing

Two workflows, both under `.github/workflows/`.

**`test.yml`** runs on every push and pull request, over Python 3.11, 3.12 and 3.13, with Redis, MySQL
and PostgreSQL as service containers on the same ports the local `make servers` uses. It lints and
then runs the suite with the coverage gate. It also declares `workflow_call`, so the release calls it
instead of repeating it.

**`release.yml`** runs on a `v*` tag and publishes to PyPI:

```
test    → the whole suite, called from test.yml
build   → check the tag equals the version in pyproject.toml, then `python -m build`, upload the artifact
publish → download that same artifact, push it to PyPI, cut the GitHub release
```

Three things make it safe:

- **A version on PyPI is permanent**, so the suite answers for it before anything is built.
- **The tag has to equal `project.version`.** A tag that disagrees publishes under a number nobody asked for, and PyPI never lets that number be used again.
- **What is published is what was checked.** The publish job downloads the artifact the build job produced instead of building a second time.

It authenticates by **Trusted Publishing (OIDC)** — `id-token: write` and the `pypi` environment — so
there is no API token anywhere in the repository. The publisher registered on PyPI must say:

| Field | Value |
| --- | --- |
| PyPI Project Name | `fast-tasks` |
| Owner | `paulocoutinhox` |
| Repository name | `fast-tasks` |
| Workflow name | `release.yml` |
| Environment name | `pypi` |

To cut a release: bump `project.version` in `pyproject.toml`, commit, then push the matching tag.

```bash
git tag v1.0.1
git push origin v1.0.1
```

---

## 11. Code style — non-negotiable

Formatting is decided by `ruff` and `black` with **line length 320** and
`skip-magic-trailing-comma`. That number is not an accident: it exists so calls and signatures stay on
one line.

**Layout**

- Functions, methods, constructors and calls stay on **one line**, always. Never break parameters across lines. Never format a signature vertically. However many parameters there are, they stay on one line.
- Keep it compact. Use only the blank lines that separate one context from the next, and always separate blocks of different responsibility with exactly one.
- Never leave `if`s, validations, state changes and returns visually glued together. A complex method has a beginning, a middle and an end you can see at a glance.
- Prefer early returns. No `else` after a `return`. Avoid needless nesting.
- Extract a small private method when a block is accumulating responsibility — and never just to make something shorter. An artificial helper that breaks the main flow is worse than the block it replaced.
- No semicolons, ever — not to join statements and not inside a sentence in prose.

**Comments**

- Rare, and only where they earn it. Well-named classes, methods and variables are the documentation.
- One-line comments starting with `#` or `//` are **lowercase**. Everything else reads normally.
- They explain **why** — context and intent — never what the line already says.
- Objective and natural. One complete sentence per line. Never continue a sentence on the next line: finish it, punctuate it, then start a new one.
- No decorative comments, no `# --- helpers ---` section banners, no comments narrating a change that was made.

**Python**

- `__init__.py` files are **empty**. Absolutely nothing goes in them.
- No `TYPE_CHECKING`, and no `if TYPE_CHECKING:` import blocks.
- No backward compatibility, no legacy paths, no "it used to work the other way" checks. There is one current version, and refactoring the whole thing to get there is expected.
- No generic fallbacks and no `else` branches invented for cases nobody understands. Something unknown fails loudly where it happens.
- No dead code.
- Everything — code, comments, docstrings, log messages, tests — is in **English**.

**Validation**

Anything that could never work is refused **where it is written**, not at three in the morning: a poll
of zero, a concurrency of zero, a lease that has already run out, a cron expression that matches
nothing, a task asking for an interval and a cron at once. Each one carries a message that says what
was asked for and why it cannot be.

**Prose in `docs/`**

Every heading starts with an emoji, and no page uses the same one twice — `tests/test_docs.py`
enforces both, along with every symbol, table name, Redis key family and internal link the prose
names. Keep the voice: plain, concrete, and explaining the reason rather than the mechanism.
