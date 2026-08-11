# 🚀 Getting started

## 📦 Install

```bash
pip install "fast-tasks[sqlalchemy]"
```

The core has no dependencies. The extra pulls SQLAlchemy for the store that most applications use.

## 🧱 Build the app

A queue is a store plus the tasks you declare against it. Every process of your fleet builds the same
one from the same code — that is the setup the whole design is for.

```python
from sqlalchemy.ext.asyncio import create_async_engine

from fast_tasks.app import FastTasks
from fast_tasks.store.sqlalchemy import SqlAlchemyStore

engine = create_async_engine("postgresql+asyncpg://user:pass@host/db")
app = FastTasks(SqlAlchemyStore(engine))
```

`await app.setup()` builds the one table the library needs. It keeps its own metadata, so it never
touches a table of yours.

## 📝 Declare a task

```python
@app.task("send_email", max_attempts=5, timeout=30)
async def send_email(to: str, subject: str):
    ...
```

A handler takes keyword arguments and nothing else, because a payload has to survive a trip through
JSON. It may be `async def` or a plain `def` — a plain one runs off the event loop so it never blocks
the tasks beside it.

## 📨 Ask for a run

```python
await app.enqueue("send_email", to="reader@example.com", subject="Welcome")
```

This returns immediately. The request that called it is done; a worker picks the run up.

## 👷 Run a worker

```python
from fast_tasks.worker import Worker

await Worker(app, concurrency=8).run()
```

That is the whole loop: reclaim what a dead worker left, write the next slot of every recurring task,
claim what is due, run it.

## 💡 The one thing to know

Start that on ten machines and nothing changes. There is no scheduler process to single out, no lock
to configure and no leader to elect — read [Jobs](tasks.md) for why.
