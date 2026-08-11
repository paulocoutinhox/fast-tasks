# 🤝 Contribution

Thanks for wanting to help. 🙌

## 🚀 Getting set up

```bash
make install
make test
```

The suite runs against memory and SQLite with nothing else installed. The other three stores take part
only when their server is reachable, and are simply not collected when it is not:

```bash
make servers
```

That starts a Redis on 6399, a MySQL on 3399 and a PostgreSQL on 5499. Point them somewhere else with
`FAST_TASKS_REDIS_URL`, `FAST_TASKS_MYSQL_URL` and `FAST_TASKS_POSTGRES_URL`.

> **`make coverage` needs all three.** The gate is 100%, and a store nobody could reach is a store
> whose lines nobody ran — the gate fails and tells you exactly which. `make test` works without them.

**Run the suite against a real MySQL before believing anything about MySQL.** It rounds a `DATETIME`
with no fractional precision, which once stored a run due at `10:00:00.9` as due at `10:00:01` — a
second in the future, where nothing claimed it until the second turned. SQLite says nothing about
that, and neither does PostgreSQL.

## ✅ Before you open a pull request

```bash
make format
make coverage
make lint
```

Three things the pipeline will check anyway, and it is faster to hear it from your own machine.

## 📐 What the project asks of a change

**Coverage stays at 100%, branches included.** It is a gate and not an aspiration. A line nobody
exercises is a line nobody knows the behaviour of.

**A new store answers the same contract.** `tests/test_store_contract.py` is written against the
interface and parametrized over every store — add yours to the fixture in `tests/conftest.py` and it
inherits the whole suite. That is the intended way to know a backend is correct, and a store that
passes it behaves exactly like the others.

**A test never waits without a bound.** Use `wait_until` from `tests/conftest.py`. A loop that spins
until something happens is a pipeline that burns for hours when it does not.

**A name means one thing.** A *task* is what you declare, a *run* is one execution of it, and a
*queue* is a lane. Mixing them is how documentation stops being true.

## 🎨 Style

`ruff` and `black` decide formatting, and `make format` runs them in the order that settles: the
linter first, because it removes imports the formatter already laid out.

Calls stay on one line — the line length is 320 for exactly that reason.

Comments are rare, lowercase, one sentence, and explain **why**. If a comment says what the line does,
the line should have been clearer instead.

## 🐛 Reporting something

An issue that shows how to reproduce is worth ten that describe. If it is a race, say how many workers
and which store — those two answer most of it.
