# 👷 Workers

A worker claims what is due and runs it. Any number of them may run against one store, on one machine
or on twenty.

```python
from fast_tasks.worker import Worker

worker = Worker(app, concurrency=8, poll=1.0, lease=timedelta(seconds=60))
await worker.run()
```

| Option | What it decides |
| --- | --- |
| `queues` | which queues this worker serves |
| `concurrency` | how many runs it holds at once |
| `poll` | how long it waits between passes, which is also the delay a due task may see |
| `lease` | how long a claim is good for before another worker may take it over, and it has to be a real span |
| `jitter` | at most how much of a retry delay is drawn at random on top of it, a quarter by default, and never below zero |
| `grace` | how long a shutdown waits for what is in flight before leaving it to the lease |
| `keep` | how long a run that is over is kept before it is pruned, a week by default, and `None` keeps everything |
| `name` | who it says it is, drawn from host, process and a random tail when left out |

A worker that could never work is refused where it is written: a poll of zero is not a wait, a
concurrency of zero never takes anything, a lease of zero has already run out, and a worker serving no
queues claims nothing while there is nothing in the queue that would ever explain why. Each of those
fails quietly rather than loudly when it is only checked at run time.

## 🧹 What is over is pruned

**A queue nobody prunes grows for ever**, and what it grows by is rows nothing reads again: four tasks
on five minute schedules write about a thousand runs a day, which is four hundred thousand a year that
every count, every index and every backup carries.

So a worker prunes what is over and older than `keep` — done, failed, given up on and called off alike.
It happens **once an hour** and never on the poll, and it takes a thousand at a time, so a deployment
that was never pruned is caught up over some passes instead of in one statement that holds the table.

> **`keep=None` prunes nothing**, which is the right answer whenever those rows are the record. It is
> also the answer that grows for ever, and both of those are the point of saying it out loud. A `keep`
> below zero is refused where it is written: it asks for what is over to be dropped before it is over,
> which is every one of them.

**A full batch means the next pass takes the next one**, and only a short one starts the hour — so a
deployment that ran a year unpruned catches up in passes instead of in a year of hours. The first
pruning of each worker is drawn somewhere inside the hour, for the same reason a retry delay is drawn:
ten workers coming up together must not all reach for the same rows in the same instant.

**And a pruning the store refuses never costs the pass.** It is logged and the pass carries on to the
claim it was on its way to make, because housekeeping that ends a pass is a worker that stops working
once an hour for a reason nothing in the queue explains.

> **The key of a run goes with it.** A key is what makes a run single, so pruning a run frees the key
> it was written under — which is what you want for a cron slot two weeks old, and what to think about
> before giving a one-shot a key you intend to reuse.

## 🔄 One pass

```python
await worker.run_once()   # reclaim, prune, materialize, claim, start
await worker.drain()      # wait for what it started
```

`run_once` returns the runs it claimed and starts them; it does not wait for them. `drain` is what
waits. Together they are the whole of `run`, and they are what a test should use instead of sleeping.

## 🪪 Identity

A worker names itself `host:pid:draw`. The host tells two machines apart, the pid tells two processes
apart, and the draw covers a pid the operating system handed out again after a restart. Everything
that decides ownership is conditional on that name.

> **A name is at most `WORKER_NAME_LIMIT` characters, and the host is what gives way.** A pod is named
> after its deployment, its namespace and its cluster, which is well past what a store keeps a worker
> name in — and a name that does not fit is not a worker that logs a warning, it is a worker whose
> every claim the database refuses while the process stays up and polls forever. So the host is cut to
> fit and the draw is what still tells two machines apart. A `name` you pass yourself is refused where
> it is written if it is longer than that.

## ⏳ Leases and a worker that dies

A claim is good for `lease`. While a task runs, the worker pushes its own lease every third of that
period, so a run that takes an hour is never taken from under it.

A process that is killed pushes nothing. Its lease runs out, the next pass of any worker notices, and
the run goes back to the queue — unless its attempts are spent, in which case it ends as failed with
`LeaseExpired`. This is why `max_attempts=1` and a task that must not run twice are a pair: a run that
was interrupted halfway is indistinguishable from one that never started.

## 🌩️ When the store blinks

A pass that raises is logged and the loop carries on — one bad minute never ends a worker. A run whose
close never reached the store stays claimed until its lease expires, and then comes back like any
other abandoned run.

That is why **the store client needs its own timeouts**. A client that waits forever turns a network
blip into a worker that is alive, polling nothing and answering nobody. See
[Stores](stores.md) for what each one needs.

## 🛑 Shutting down

```python
worker.stop()
```

`stop` ends the polling loop and `run` then waits for what is in flight, for up to `grace` seconds.
Nothing new is claimed after `stop`, so a deploy loses no work.

A run still going when the grace runs out is left where it is and said out loud. Its lease is what
brings it back — to this worker's replacement, or to any other. **A shutdown always ends**, because
one that waits forever is a deploy that never finishes.

## 🕰️ The clocks have to agree

Every worker asks **its own machine** what time it is, and everything that decides who holds what is a
timestamp: when a run is due, and when a lease runs out. Workers never talk to each other, so a machine
whose clock is wrong does not disagree with anybody — it simply acts on a different now.

A machine running ahead by more than a `lease` treats a lease that is very much alive as expired.
Measured with five minutes of skew and a sixty second lease: it took a run back from a worker that was
still working on it, and the outcome of the worker that finished was dropped.

**So keep the clocks in sync — NTP, and nothing more exotic than that.** A `lease` comfortably longer
than the drift you could ever have is the second half of it, and the default of sixty seconds is
already far outside what a synced machine ever drifts.

## 📐 How many

Start with `concurrency` around what your task's blocking profile justifies and one worker per
process. Ten processes each holding eight runs is eighty at once, which the store handles with one
`UPDATE` per claim.

Splitting by queue is how a heavy task is kept away from a light one. Splitting by machine needs
nothing: workers coordinate through the store and never talk to each other.
