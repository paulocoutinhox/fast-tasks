import asyncio
from contextlib import asynccontextmanager

from fast_tasks.worker import Worker


def lifespan_for(worker: Worker):
    """runs a worker beside the api: it comes up with the process, and is given the chance to finish what it holds before the process goes"""

    @asynccontextmanager
    async def lifespan(app):
        await worker.app.setup()
        polling = asyncio.create_task(worker.run())

        try:
            yield
        finally:
            worker.stop()
            await polling

    return lifespan
