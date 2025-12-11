# async-job-queue

Async Job Queue with Task Scheduling using Python.

This project provides a tiny asyncio-based job queue that can schedule work to run after a delay or on a recurring interval. It is useful for lightweight background workers, cron-style tasks, or delayed processing without additional dependencies.

## Features
- Schedule asynchronous or synchronous callables
- Optional recurring jobs with fixed intervals
- Simple context manager for automatic startup and shutdown
- Futures returned to callers for awaiting the first run's result

## Installation
No external dependencies are required beyond Python 3.10+. To run the tests locally, install `pytest`:

```bash
pip install pytest
```

## Usage
```python
import asyncio
from async_job_queue import JobQueue

async def main():
    async with JobQueue(workers=2) as queue:
        # Run once after a small delay
        result = queue.schedule(lambda: "hello", delay=0.1)
        print(await result)  # -> "hello"

        # Run a recurring task every 5 seconds
        async def heartbeat():
            print("still alive")

        queue.schedule(heartbeat, interval=5.0, name="heartbeat")

        # Keep the queue running for 12 seconds
        await asyncio.sleep(12)

asyncio.run(main())
```

## Running Tests
```bash
pytest
```
