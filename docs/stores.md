# 🗄️ Stores

A store is where runs live. It is the only thing that knows about durability, and it is the seam a
different backend plugs into.

## 🐘 SQLAlchemy

```python
from sqlalchemy.ext.asyncio import create_async_engine

from fast_tasks.store.sqlalchemy import SqlAlchemyStore

store = SqlAlchemyStore(create_async_engine("postgresql+asyncpg://user:pass@host/db"))
await store.setup()
```

Works on PostgreSQL, MySQL and SQLite. `setup` builds one table, `fast_tasks_run`, under metadata of
its own, so it never creates or drops anything of yours.

**`setup` survives being called by everybody at once.** `create_all` asks whether the table is there
and then creates it, which is a question and a statement with a gap in between — ten replicas booting
together used to leave eight of them dead on that gap, told the table already exists. It asks the
database again rather than reading an error message, so a race carries on and a real refusal still
raises.

Timestamps are held as naive UTC and read back as aware UTC, because MySQL keeps no offset and a
queue that guesses one runs tasks an hour late.

Two indexes carry the whole load: `(queue, status, priority, due_at)` answers every claim, and
`(status, lease_until)` answers every reclaim.

### 📁 SQLite across processes

SQLite works with several processes against one file if you ask for it:

```python
engine = create_async_engine(f"sqlite+aiosqlite:///{path}", connect_args={"timeout": 30})

async with engine.begin() as connection:
    await connection.exec_driver_sql("PRAGMA journal_mode=WAL")
```

Without write-ahead logging the second process meets a locked database. With it, this is a fine
setup for a single machine. For several machines, use a database several machines can reach.

## 🔴 Redis

```python
from redis.asyncio import Redis

from fast_tasks.store.redis import RedisStore

store = RedisStore(Redis.from_url("redis://localhost:6379/0"))
await store.setup()
```

For whoever would rather not put a queue in their database. `setup` builds nothing — Redis needs no
schema — it only registers the scripts the store runs.

**The claim, the reclaim and every close are Lua**, so each of them is one atomic step on the server.
A claim that read, decided and wrote in three round trips would let a second worker in between them,
and two workers would run the same thing.

What it keeps:

| Key | What it holds |
| --- | --- |
| `fast_tasks:run:{id}` | the run itself, as a hash |
| `fast_tasks:queue:{queue}:{priority}` | one lane per priority, scored by `due_at` |
| `fast_tasks:priorities:{queue}` | which lanes exist, so a claim walks them highest first |
| `fast_tasks:leased` | what is running, scored by when its lease runs out |
| `fast_tasks:key:{key}` | the reservation that makes an occurrence single |

**One lane per priority is why priority works.** A sorted set orders by one number, and a claim needs
the highest priority *among what is due* — two questions that one score cannot answer. The lanes are
created on demand, so a deployment that never sets a priority has exactly one.

**A claim gathers the lanes of every queue it serves before it walks any of them.** Priority is what
orders a claim and a queue is only where a run waits, so a worker serving two of them takes the urgent
run of the second before the ordinary ones of the first. Inside a priority the lanes are merged by
`due_at`, which is the order the other stores read straight out of one index.

`prefix` renames every key at once, which is what lets this share a Redis with an application without
ever meeting it.

### 🛠️ What a production client needs

```python
Redis.from_url(url, socket_timeout=15, socket_connect_timeout=5, health_check_interval=30, retry_on_timeout=True)
```

**redis-py waits forever by default.** With no `socket_timeout`, a connection whose network went away
leaves a worker blocked on a read that never answers — the run stays claimed until its lease expires
and another worker picks it up, but this one is gone and will not come back on its own.

With a timeout the read raises, the worker logs it and the next pass carries on, which is what a
process is supposed to do when a dependency blinks.

`health_check_interval` is what closes a connection an idle NAT or load balancer already dropped, and
it is the difference between one failed pass and a worker that answers nothing.

> **Counting is a scan.** Redis has no index over a hash, so `count` walks what the store owns. It is
> for an operator watching depth, not for a hot path.

### 🚧 One instance, and not a cluster

**A single Redis, or one replicated primary — never Redis Cluster.** Every script here builds the keys
it touches from `ARGV` instead of declaring them in `KEYS`, because a claim discovers which run it took
only while it runs, and a cluster refuses a script that reaches a key it was not given. Slots would
scatter the lanes, the leases and the run hashes across nodes, and the atomic step that makes two
workers safe would stop being one.

A replica for failover is fine, since every write goes to the primary. What is not fine is sharding.

> **The reclaim takes a batch, and that is deliberate.** Redis runs a script to the end before it
> answers anybody else, so a pass over an unbounded backlog of expired leases would freeze the server
> for every other client. `RECLAIM_BATCH` is 500 per pass, and the pass after it takes the next 500.
> **Every store takes the same batch**, for the same reason worn differently: a cluster that died
> holding a hundred thousand runs is one statement over a hundred thousand rows, in one transaction,
> issued by every surviving worker at once.

### 🧨 Nothing may evict a run

**`maxmemory-policy` has to be `noeviction`** on whatever Redis holds this, or the queue belongs to a
database of its own. A run is a hash and the queue it waits in is a sorted set, and an eviction takes
the first without touching the second: the lane keeps handing out an id whose run is gone.

The store steps over that instead of building a hash out of the fields a claim writes — the claim, the
read that follows it and the reclaim all check the run is still there — so what an eviction costs is
the run itself, silently, which is the part no code on either side can give back.

`allkeys-lru` on a shared Redis is the setting that does this, and it is a common one.

## 🧠 Memory

```python
from fast_tasks.store.memory import MemoryStore
```

The whole library minus durability: right for tests and for a single process, wrong for two, because
nothing outside the process that owns it can see a thing.

## ✍️ Writing your own

Subclass `fast_tasks.store.base.Store` and answer fourteen methods. The contract is short, and one rule
runs through all of it:

> **Every method that changes a run is conditional on the state that run was in.**

`claim` only takes a run that is `pending` and due. `complete`, `fail`, `retry_later`, `release` and
`heartbeat` only touch a run whose `worker` is the caller and whose status is `running`. `add` refuses
a key that is taken. That is what makes two workers safe without a lock anywhere, and a store that
answers "changed" for a row it did not change breaks the guarantee for everybody.

`retry_later` and `release` are the same write with one difference: an attempt that happened stands,
and one a worker never had anything to try is given back. A store that treats them as the same method
spends a run's whole budget on a rolling deploy.

The suite in `tests/test_store_contract.py` is written against the interface and parametrized over
every store. Add yours to the fixture and it inherits the whole thing — that is the intended way to
know a new backend is correct.
