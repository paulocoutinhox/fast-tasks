<p align="center">
    <a href="https://github.com/paulocoutinhox/fast-tasks" target="_blank" rel="noopener noreferrer">
        <img width="420" src="extras/images/logo.png" alt="Fast Tasks">
    </a>
</p>

<p align="center">
  <a href="https://github.com/paulocoutinhox/fast-tasks/actions/workflows/test.yml"><img src="https://github.com/paulocoutinhox/fast-tasks/actions/workflows/test.yml/badge.svg" alt="Fast Tasks - Test"></a>
  <a href="https://github.com/paulocoutinhox/fast-tasks/blob/main/LICENSE.md"><img src="https://img.shields.io/badge/license-MIT-blue.svg" alt="License: MIT"></a>
  <a href="https://www.python.org"><img src="https://img.shields.io/badge/python-3.11%20|%203.12%20|%203.13-blue.svg" alt="Python versions"></a>
</p>

<p align="center">
Asynchronous task queue and scheduler for Python, where one task runs on exactly one worker.
</p>

<br>

## 🚀 Project

Fast Tasks is a task queue and a scheduler that are the same thing.

Everything it does reduces to a single row: **a run that is due, which exactly one worker claims**. An
immediate task, an interval, a fixed datetime and a cron expression differ only in *when the next run
is written*, and in nothing else.

That is why there is no separate scheduler process, no broker and no leader election. Start it on ten
machines and the nightly report still runs once.

## ✨ Features

- [x] Run a task now, off the request that asked for it
- [x] Run a task every N seconds
- [x] Run a task once, at a stated date and time
- [x] Run a task on a cron expression
- [x] One run on exactly one worker, however many processes, workers and machines
- [x] Retries with fixed, linear, exponential and jittered policies
- [x] Timeouts, heartbeats and recovery of what a dead worker was holding
- [x] Named queues and priorities
- [x] Lifecycle hooks for auditing, metrics and alerting
- [x] Pluggable stores: PostgreSQL, MySQL, SQLite, Redis and memory
- [x] Survives a kill: what a dead worker held comes back, and a schedule resumes where it stopped
- [x] No dependencies in the core
- [x] 100% branch coverage: one contract against every store, plus multi-worker, multi-machine, everyday and disaster suites

## 📦 Install

```bash
pip install "fast-tasks[sqlalchemy]"
```

Or with Redis:

```bash
pip install "fast-tasks[redis]"
```

## 🧭 The four ways to ask for work

| What you want | How you ask for it |
| --- | --- |
| run it now | `await app.enqueue("send_email", to="a@b.com")` |
| run it every 30 seconds | `@app.task("poll", every=30)` |
| run it once, at a stated time | `await app.enqueue_at("report", when)` |
| run it on a cron expression | `@app.task("nightly", cron="0 4 * * *")` |

## 💡 How to use

```python
import asyncio

from sqlalchemy.ext.asyncio import create_async_engine

from fast_tasks.app import FastTasks
from fast_tasks.store.sqlalchemy import SqlAlchemyStore
from fast_tasks.worker import Worker

app = FastTasks(SqlAlchemyStore(create_async_engine("sqlite+aiosqlite:///app.db")))


@app.task("send_email", max_attempts=5, timeout=30)
async def send_email(to: str, subject: str):
    ...


@app.task("nightly_report", cron="0 4 * * *")
async def nightly_report():
    ...


async def main():
    await app.setup()
    await app.enqueue("send_email", to="reader@example.com", subject="Welcome")
    await Worker(app, concurrency=8).run()


asyncio.run(main())
```

## 🧱 The words it uses

Three of them, and each means one thing:

| Word | What it is |
| --- | --- |
| **Task** | what you declare: a name, the code, and the policy it runs under |
| **Run** | one execution of a task, which is the only thing a worker ever claims |
| **Queue** | a named lane a task belongs to, and a worker serves |

## 📚 Documentation

- [Getting started](docs/getting-started.md)
- [Tasks](docs/tasks.md) — the four kinds, and what a name means
- [Workers](docs/workers.md) — concurrency, leases and many machines
- [Retries and failures](docs/retries.md) — policies, timeouts, permanent errors
- [Hooks](docs/hooks.md) — being told about every run
- [Stores](docs/stores.md) — SQLAlchemy, Redis, memory, and writing your own
- [FastAPI](docs/fastapi.md) — running a worker beside an API
- [Contribution](docs/contribution.md) — how to help

## ☕ Buy me a coffee

Support the continuous development of this project.

<a href='https://ko-fi.com/A0A412XEV' target='_blank'><img height='36' style='border:0px;height:36px;' src='https://storage.ko-fi.com/cdn/kofi2.png?v=6' border='0' alt='Buy Me a Coffee at ko-fi.com' /></a>

## 📄 License

[MIT](http://opensource.org/licenses/MIT)

Copyright (c) 2026, Paulo Coutinho
