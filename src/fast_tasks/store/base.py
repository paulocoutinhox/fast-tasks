from abc import ABC, abstractmethod
from datetime import datetime, timedelta

from fast_tasks.run import Run, RunStatus

# how many rows past the asked-for limit a claim looks at, so ten workers wanting one task do not all reach for the same row
CLAIM_SPREAD = 5

# how many expired leases one pass takes back: a whole cluster dying expires everything it held at once, and one statement over all of it is a transaction that holds the store while every other worker waits on it
RECLAIM_BATCH = 500

# the longest a worker may be called, which is what every store sizes the column it keeps that name in. a pod is named after its deployment and its namespace, and a name that does not fit is a worker whose every claim the database refuses
WORKER_NAME_LIMIT = 128

# the longest a task may be called, and the longest a key may be. a value past what the column holds is a write the database refuses — and on one that does not refuse it, a value quietly cut short, which is two occurrences of two different minutes becoming one run
TASK_NAME_LIMIT = 255
KEY_LIMIT = 255

# the longest a queue may be called
QUEUE_LIMIT = 64


class Store(ABC):
    """where runs live. every method that changes a run is conditional on the state that run was in, which is what makes two workers safe without a lock anywhere"""

    @abstractmethod
    async def setup(self) -> None:
        """build whatever the store needs to hold runs, and do nothing when it is already there"""

    @abstractmethod
    async def add(self, run: Run) -> Run | None:
        """writes the run, and answers nothing when its key is already taken — which is how an occurrence stays single"""

    @abstractmethod
    async def claim(self, worker: str, queues: tuple[str, ...], limit: int, lease: timedelta, moment: datetime) -> list[Run]:
        """takes up to `limit` due runs for this worker, and never hands one to two of them"""

    @abstractmethod
    async def heartbeat(self, run_id, worker: str, lease: timedelta, moment: datetime) -> bool:
        """pushes the lease of a run this worker still holds, and answers false once it no longer holds it"""

    @abstractmethod
    async def complete(self, run_id, worker: str, moment: datetime, result: dict | None) -> bool:
        """closes a run this worker holds as done"""

    @abstractmethod
    async def fail(self, run_id, worker: str, moment: datetime, error: str, error_type: str) -> bool:
        """closes a run this worker holds as failed, which is the end of it"""

    @abstractmethod
    async def retry_later(self, run_id, worker: str, due_at: datetime, error: str, error_type: str) -> bool:
        """puts a run this worker holds back in the queue, due when the policy says, with the attempt it just spent standing"""

    @abstractmethod
    async def release(self, run_id, worker: str, due_at: datetime, error: str, error_type: str) -> bool:
        """puts a run this worker holds back in the queue and gives the attempt back with it, for a worker that never had anything to try"""

    @abstractmethod
    async def reclaim(self, moment: datetime) -> int:
        """a worker that died holds a lease nobody will ever drop, so what ran out goes back to the queue or fails for good"""

    @abstractmethod
    async def cancel(self, run_id, moment: datetime) -> bool:
        """drops a run that has not started, and answers false for one already running or over"""

    @abstractmethod
    async def get(self, run_id) -> Run | None:
        """one run by id"""

    @abstractmethod
    async def find(self, key: str) -> Run | None:
        """one run by the key that made it unique"""

    @abstractmethod
    async def purge(self, before: datetime, limit: int) -> int:
        """drops settled runs that finished before an instant, up to `limit` of them, because a queue nobody prunes grows for ever"""

    @abstractmethod
    async def count(self, status: RunStatus | None = None, queue: str | None = None) -> int:
        """how deep the queue is, which is the one number an operator watches"""
