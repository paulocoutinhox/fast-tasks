"""one machine of the fleet, run as its own process by the test that proves ten of them hand out each job once"""

import asyncio
import sys

from fast_tasks.worker import Worker
from tests.fleet import build_queue


async def main(database: str, output: str, seconds: float) -> None:
    app, engine = build_queue(database, output)
    worker = Worker(app, concurrency=4, poll=0.02)
    polling = asyncio.create_task(worker.run())

    try:
        await asyncio.sleep(seconds)
        worker.stop()
        await polling
    finally:
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1], sys.argv[2], float(sys.argv[3])))
