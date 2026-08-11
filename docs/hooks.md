# 🪝 Hooks

A worker tells the application about every run it handles. That is how an audit trail, a metric or an
alert is kept without the queue ever knowing what any of those are.

```python
worker = Worker(app)


@worker.on_start
async def started(task):
    logger.info("[queue] %s started", task.name)


@worker.on_finish
async def finished(task, result, seconds):
    logger.info("[queue] %s finished in %.2fs", task.name, seconds)


@worker.on_error
async def failed(task, error, seconds, retrying):
    logger.error("[queue] %s broke: %s (%s)", task.name, error, "coming back" if retrying else "given up")
```

| Stage | Arguments |
| --- | --- |
| `on_start` | `task` |
| `on_finish` | `task`, what the handler answered, seconds it took |
| `on_error` | `task`, the exception, seconds it took, whether it comes back for another attempt |

A listener may be a coroutine or a plain function, and every listener of a stage is told in the order
it was registered. Registering hands the listener back, so it works as a decorator or as a plain call.

## 🛡️ A listener that breaks breaks alone

A listener that raises is logged and nothing else. It never changes the outcome of the run, and it
never stops the listeners after it.

That rule is not politeness — an audit table that is full, or a metrics endpoint that is down, must
not turn a task that worked into a task that failed.

## 🤐 An ending the store would not take is not announced

A worker whose lease ran out while it was working no longer holds the run — somebody else took it over
and is running it too. When that worker finishes, the store refuses its outcome, and **the listeners
are not told**, whichever way the attempt ended.

Without that, one run writes two lines into an audit trail: one here, for an outcome that was thrown
away, and one under the worker that actually holds it. What the listeners are told is exactly what the
store recorded, and the dropped outcome is a warning in the log instead.

## 📒 Writing an audit trail

```python
@worker.on_finish
async def record(task, result, seconds):
    async with SessionLocal() as session:
        session.add(AuditLog(name=task.name, attempts=task.attempts, seconds=seconds))
        await session.commit()
```

The task carries everything worth writing down: `name`, `queue`, `attempts`, `payload`, `key` and the
timestamps the store filled in.
