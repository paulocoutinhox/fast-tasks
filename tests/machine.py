"""one machine of the fleet, run as its own process by the tests that prove many of them hand out each job once"""

import asyncio
import json
import sys
from datetime import timedelta

from fast_tasks.worker import Worker
from tests.fleet import build_queue


async def main(settings: dict) -> None:
    app, closing = build_queue(settings["url"], settings["output"], settings["attempts"])

    # every process sets its own store up, because what that means for redis is registering the scripts this one is about to run
    await app.setup()

    worker = Worker(app, concurrency=settings["concurrency"], poll=0.02, lease=timedelta(seconds=settings["lease"]))
    polling = asyncio.create_task(worker.run())

    try:
        await asyncio.sleep(settings["seconds"])
        worker.stop()
        await polling
    finally:
        await closing()


if __name__ == "__main__":
    asyncio.run(main(json.loads(sys.argv[1])))
