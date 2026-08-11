# 📝 Tasks

A task is a **name**, a handler and a policy. The name is what travels in the database, so it stays
stable while the function behind it moves, is renamed or changes module.

## 🧭 The four kinds

### ⚡ Run it now

```python
@app.task("send_email", max_attempts=5)
async def send_email(to: str):
    ...


await app.enqueue("send_email", to="reader@example.com")
```

### 🔁 Run it every N seconds

```python
@app.task("poll_inbox", every=30)
async def poll_inbox():
    ...
```

`every` is seconds, and a `timedelta` works too. Slots are counted from the unix epoch and never from
whenever a given process started, so **every worker of every machine names the same slot**. That is
what makes the next line true.

### 📅 Run it once, at a stated time

```python
await app.enqueue_at("close_campaign", datetime(2026, 8, 1, 10, tzinfo=timezone.utc), campaign_id=7)
```

A one-shot is a run with a future `due_at` and nothing more. If every worker is down when it comes
due, the first one back picks it up — nothing is lost by being late.

> **A datetime with no zone is read as UTC**, and it is settled once, here, rather than by each store
> for itself. That matters because the obvious reading is not the same on both sides: a column with no
> offset can only mean UTC, while `datetime.timestamp()` reads a naive value as the wall clock of
> whichever machine happened to write it. The same value would name two instants depending on the
> store. Pass an aware datetime and none of this is yours to think about.

To declare one from code that runs on every boot, give it a key so the tenth boot does not enqueue a
tenth copy:

```python
await app.enqueue_at("close_campaign", when, key="close_campaign:7", campaign_id=7)
```

### ⏰ Run it on a cron expression

```python
@app.task("nightly_report", cron="0 4 * * *")
async def nightly_report():
    ...
```

Standard five POSIX fields, with `*`, numbers, `a-b` ranges, `a,b` lists and `*/n` steps. Sunday is 0
or 7. Day-of-month and day-of-week are joined by **or** when both are restricted, which is what POSIX
says and what `0 0 1 * 1` — the first of the month *or* any Monday — depends on.

An expression that cannot mean anything raises where it is declared, not on the night it would first
have run — `0 0 30 2 *` included, because no February has a thirtieth. That one is worth naming: the
search is what would otherwise discover it, and the search walks a **year of minutes** before giving
up, on every pass, for as long as the process lives.

> **A day nothing has is only fatal while the weekday field is open.** POSIX joins the two with an
> **or** when both are restricted, so `0 0 30 2 5` is a perfectly good expression: every Friday in
> February matches it.

## 🔑 Why ten workers do not make ten runs

A recurring task is not run by a scheduler. Every worker, on every poll, computes the **next slot** of
every recurring task it knows and writes it down under a key built from the name and that instant:

```
nightly_report@2026-08-01T04:00:00+00:00
```

The key is unique. Ten workers write it, the database keeps one, and the other nine are told the key
is taken and carry on. Then the run is claimed like any other — by exactly one of them.

Nothing elects a leader, because nothing has to.

## 🗝️ Keys

Any run may carry a key, not just a recurring one. A key is an idempotency guarantee: the second
caller is handed the run the first one wrote.

```python
first = await app.enqueue("send_email", key="welcome:42", to="reader@example.com")
again = await app.enqueue("send_email", key="welcome:42", to="somebody@example.com")

assert again.id == first.id
```

## 🚚 Queues

A task may name a queue, and a worker names the queues it serves. That is how a slow task is kept from
sitting in front of a fast one:

```python
@app.task("transcode", queue="heavy", timeout=3600)
async def transcode(path: str):
    ...


Worker(app, queues=("heavy",), concurrency=2)
Worker(app, queues=("default",), concurrency=32)
```

## 🧩 Handlers

A handler may be `async def`, a plain `def`, a callable object, or something a decorator wrapped. A
plain one runs off the event loop so it never blocks the runs beside it, and one that only *looks*
plain — a wrapper that answers a coroutine — is awaited all the same.

The payload arrives as keyword arguments, so it has to survive a trip through JSON.

## 🥇 Priority

`priority` is served before age. A task declares its own, and a single call may override it:

```python
await app.enqueue("send_email", priority=10, to="reader@example.com")
```
