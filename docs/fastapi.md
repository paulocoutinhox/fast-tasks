# 🌐 FastAPI

## 🤝 A worker beside the API

For a small deployment the simplest thing is to run the worker inside the API process:

```python
from fastapi import FastAPI

from fast_tasks.fastapi import lifespan_for
from fast_tasks.worker import Worker

worker = Worker(app, concurrency=8)
app = FastAPI(lifespan=lifespan_for(worker))
```

The lifespan builds the store, starts the worker with the process, and on shutdown stops the polling
loop and waits for what is in flight. A deploy loses no work.

Then a request only writes a row:

```python
@app.post("/signup")
async def signup(payload: SignUpRequest):
    account = await create_account(payload)
    await app.enqueue("send_email", to=account.email, subject="Welcome")

    return account
```

The response does not wait for the mail server.

## 👥 Several workers

`uvicorn --workers 4` gives four processes, each with its own worker. Nothing changes: they
coordinate through the store, so the nightly report still runs once. This is the case the library is
built for, and there is nothing to configure for it.

## ✂️ Separating the worker from the API

Past a certain size, run the queue somewhere the web traffic cannot starve it:

```python
# worker.py
import asyncio

from fast_tasks.worker import Worker

from myapp.queue import queue


async def main():
    await app.setup()
    await Worker(app, concurrency=16).run()


asyncio.run(main())
```

Then the API process builds the queue and only enqueues, and never starts a worker. Both import the
same task declarations, because a name has to mean the same thing on both sides.

## 📈 Watching it

```python
@app.get("/queue")
async def depth():
    return {"pending": await app.count(status=RunStatus.PENDING), "running": await app.count(status=RunStatus.RUNNING), "failed": await app.count(status=RunStatus.FAILED)}
```

Pending climbing means not enough workers. Failed climbing means something is broken, and the run
carries the message and the class that broke it.
