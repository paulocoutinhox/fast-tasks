import asyncio
from copy import deepcopy
from datetime import datetime, timedelta
from itertools import count

from fast_tasks.run import SETTLED, Run, RunStatus
from fast_tasks.store.base import CLAIM_SPREAD, RECLAIM_BATCH, Store


class MemoryStore(Store):
    """runs held in the process that owns it. it is the whole library minus durability, which makes it right for tests and for a single process, and wrong for two"""

    def __init__(self) -> None:
        self.runs: dict[int, Run] = {}
        self.sequence = count(1)
        self.guard = asyncio.Lock()

    async def setup(self) -> None:
        return None

    def taken(self, key: str | None) -> bool:
        return key is not None and any(run.key == key for run in self.runs.values())

    async def add(self, run: Run) -> Run | None:
        async with self.guard:
            if self.taken(run.key):
                return None

            run.id = next(self.sequence)
            self.runs[run.id] = run

            return deepcopy(run)

    def claimable(self, run: Run, queues: tuple[str, ...], moment: datetime) -> bool:
        return run.status == RunStatus.PENDING and run.queue in queues and run.due_at <= moment

    async def claim(self, worker: str, queues: tuple[str, ...], limit: int, lease: timedelta, moment: datetime) -> list[Run]:
        async with self.guard:
            ready = [run for run in self.runs.values() if self.claimable(run, queues, moment)]
            ready.sort(key=lambda run: (-run.priority, run.due_at, run.id))
            taken = []

            for run in ready[: limit * CLAIM_SPREAD]:
                if len(taken) == limit:
                    break

                run.status = RunStatus.RUNNING
                run.worker = worker
                run.attempts += 1
                run.started_at = moment
                run.lease_until = moment + lease
                taken.append(deepcopy(run))

            return taken

    def held(self, run_id, worker: str) -> Run | None:
        run = self.runs.get(run_id)

        return run if run is not None and run.worker == worker and run.status == RunStatus.RUNNING else None

    async def heartbeat(self, run_id, worker: str, lease: timedelta, moment: datetime) -> bool:
        async with self.guard:
            run = self.held(run_id, worker)

            if run is None:
                return False

            run.lease_until = moment + lease

            return True

    async def complete(self, run_id, worker: str, moment: datetime, result: dict | None) -> bool:
        async with self.guard:
            run = self.held(run_id, worker)

            if run is None:
                return False

            run.status = RunStatus.DONE
            run.finished_at = moment
            run.result = result
            run.error = None
            run.error_type = None
            run.worker = None
            run.lease_until = None

            return True

    async def fail(self, run_id, worker: str, moment: datetime, error: str, error_type: str) -> bool:
        async with self.guard:
            run = self.held(run_id, worker)

            if run is None:
                return False

            run.status = RunStatus.FAILED
            run.finished_at = moment
            run.error = error
            run.error_type = error_type
            run.worker = None
            run.lease_until = None

            return True

    async def retry_later(self, run_id, worker: str, due_at: datetime, error: str, error_type: str) -> bool:
        async with self.guard:
            run = self.held(run_id, worker)

            if run is None:
                return False

            run.status = RunStatus.PENDING
            run.due_at = due_at
            run.error = error
            run.error_type = error_type
            run.worker = None
            run.lease_until = None
            run.started_at = None

            return True

    async def reclaim(self, moment: datetime) -> int:
        async with self.guard:
            abandoned = [run for run in self.runs.values() if run.status == RunStatus.RUNNING and run.lease_until is not None and run.lease_until < moment][:RECLAIM_BATCH]

            for run in abandoned:
                run.worker = None
                run.lease_until = None
                run.started_at = None

                if run.exhausted:
                    run.status = RunStatus.FAILED
                    run.finished_at = moment
                    run.error = "the worker holding this run stopped answering"
                    run.error_type = "LeaseExpired"
                else:
                    run.status = RunStatus.PENDING
                    run.due_at = moment

            return len(abandoned)

    async def cancel(self, run_id, moment: datetime) -> bool:
        async with self.guard:
            run = self.runs.get(run_id)

            if run is None or run.status != RunStatus.PENDING:
                return False

            run.status = RunStatus.CANCELED
            run.finished_at = moment

            return True

    async def purge(self, before: datetime, limit: int) -> int:
        async with self.guard:
            gone = [run.id for run in self.runs.values() if run.status in SETTLED and run.finished_at is not None and run.finished_at < before][:limit]

            for run_id in gone:
                del self.runs[run_id]

            return len(gone)

    async def get(self, run_id) -> Run | None:
        run = self.runs.get(run_id)

        return deepcopy(run) if run is not None else None

    async def find(self, key: str) -> Run | None:
        return next((deepcopy(run) for run in self.runs.values() if run.key == key), None)

    async def count(self, status: RunStatus | None = None, queue: str | None = None) -> int:
        return len([run for run in self.runs.values() if (status is None or run.status == status) and (queue is None or run.queue == queue)])
