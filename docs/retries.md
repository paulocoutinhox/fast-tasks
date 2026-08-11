# 🔁 Retries and failures

## 🧭 What an attempt becomes

| What the handler did | What happens |
| --- | --- |
| returned | the run is done, and a dictionary it returned is kept as the result |
| raised, attempts left | the run comes back, due after the policy's delay |
| raised, attempts spent | the run ends as failed, with the message and the class that broke |
| raised `PermanentError` | the run ends as failed **now**, however many attempts were allowed |
| ran past its `timeout` | the worker stops waiting for it and treats that as a failure that may be retried |
| the worker died | the lease runs out and the run comes back, or ends as failed when nothing is left |
| raised `SystemExit` or `KeyboardInterrupt` | the run ends as failed, and the worker keeps going |
| was cancelled | the cancellation is passed on, and the lease is what brings the run back |

A pass that works clears what the pass before it wrote, so a run that succeeded on its third attempt
carries no error.

## 📊 Policies

```python
@app.task("send_email", max_attempts=5, retry_policy=RetryPolicy.EXPONENTIAL, retry_delay=5)
```

| Policy | Waits before attempt 2, 3, 4 |
| --- | --- |
| `FIXED` | 5, 5, 5 |
| `LINEAR` | 5, 10, 15 |
| `EXPONENTIAL` | 5, 10, 20 |
| `EXPONENTIAL_JITTER` | the above, plus a **drawn** fraction of it, up to the worker's `jitter` |

**No wait is longer than `max_retry_delay`**, an hour by default. Doubling has no ceiling of its own,
and five seconds over twenty attempts is a retry a month away — which nobody ever meant to ask for.

Jitter matters when a shared dependency falls over: without it every run that failed in the same
second comes back in the same second.

> **The fraction is drawn per run, and that is the whole point.** Ten thousand runs work the delay out
> from the same numbers, so a fixed multiplier — even a large one — hands the herd back whole an hour
> later. The draw is never below what the policy without jitter says, so backing off is still backing
> off; it only ever spreads the return.

## 🚚 A name this worker has never heard of

A rolling deploy runs two versions at once, so the older replica meets runs the newer one enqueued for
tasks it does not declare. **That never costs the run an attempt and never ends it**: the worker hands
it straight back to the queue with `UnknownTask` written on it, and the replica that knows the name
picks it up.

It has to work that way because `max_attempts` is **one** by default — counting it would mean the older
replica destroying a perfectly good run, silently, every time it got there first.

> **A name nobody knows waits instead of dying**, which is the other side of that. It is not free
> forever, though: it backs off like any other retry, up to `max_retry_delay`, so a typo settles into
> one claim an hour and sits in the queue where an operator can see it and cancel it.

## 🛑 A handler that asks the process to stop

`SystemExit` and `KeyboardInterrupt` are not `Exception`, and asyncio never swallows them inside a
task — it hands them to the event loop, which ends the **whole worker** and every run it was holding.
A library calling `sys.exit()` somewhere deep would do exactly that.

So they are caught, the run ends as failed, and the worker carries on. One handler does not get to
take down the runs beside it, and it is not something another attempt would fix either.

`asyncio.CancelledError` is the one exception to the rule: it is passed on untouched, because that is
the shutdown asking and a worker that swallowed it would be one nobody can stop.

## 🚫 Never retry this one

```python
from fast_tasks.errors import PermanentError


@app.task("charge", max_attempts=5)
async def charge(account_id: int, cents: int):
    if cents <= 0:
        raise PermanentError("a charge of nothing is a bug and not a blip")
```

A malformed payload does not get better by being tried four more times, and a card that was declined
is a decision and not an outage.

## ⏱️ Timeouts

```python
@app.task("transcode", timeout=3600)
```

Without a timeout a run may run forever, holding a slot of the worker's concurrency. With one, the
worker stops waiting and the failure is retried like any other. Set `timeout` below `lease` only if you
would rather the timeout fire than the lease.

> **A timeout stops the waiting, and only a coroutine stops the work.** A plain handler runs in a
> thread, and Python cannot end a thread from outside — so `wait_for` cancels the await while the
> function carries on to its own end. Measured: a plain handler with `timeout=0.2` and a retry ran
> **twice at once**, and both copies finished.
>
> So on a plain handler a timeout is a promise about the worker and never about the work. Where the
> work itself has to stop, the handler has to be a coroutine — or it has to watch its own deadline and
> give up on its own.

## 🌩️ When the outcome never reaches the store

A run whose close could not be written stays claimed until its lease expires, and then comes back like
any other abandoned run. The worker says so in its log — otherwise a store that blinked shows up as a
run executed twice with nothing anywhere explaining why.

A heartbeat that cannot reach the store is never the outcome of a run either. It is logged and the
loop carries on: the lease is what says a worker is still here, and losing it is exactly how the run
comes back to somebody else.

## 🎯 Exactly once, or at least once

This library is **at least once**, like every queue that survives a power cut. A run that was claimed
and executed but whose worker died before recording the outcome will be executed again.

Make the handler idempotent — that is the only real answer, and it is cheap: a payment keyed by an
idempotency key, a file written to a name derived from the payload, an `INSERT` guarded by a unique
constraint. For work that genuinely must not repeat, `max_attempts=1` turns the second execution into
a failure instead.
