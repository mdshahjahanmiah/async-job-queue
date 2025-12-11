import asyncio
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from async_job_queue import JobQueue


def test_single_job_completes():
    async def runner():
        async with JobQueue() as queue:
            async def add(a, b):
                await asyncio.sleep(0.01)
                return a + b

            result = queue.schedule(add, args=(2, 3))
            assert await result == 5

    asyncio.run(runner())


def test_recurring_job_runs_multiple_times():
    async def runner():
        async with JobQueue() as queue:
            counter = 0
            ready = asyncio.Event()

            async def tick():
                nonlocal counter
                counter += 1
                if counter >= 3:
                    ready.set()

            queue.schedule(tick, interval=0.05)
            await asyncio.wait_for(ready.wait(), timeout=1.0)
        return counter

    assert asyncio.run(runner()) >= 3


def test_pending_jobs_resolve_on_stop():
    async def runner():
        queue = JobQueue()
        await queue.start()

        async def never_runs():
            return "done"

        result = queue.schedule(never_runs, delay=10)

        await queue.stop()

        with pytest.raises(asyncio.CancelledError):
            await result

    asyncio.run(runner())
