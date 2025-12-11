from __future__ import annotations

import asyncio
import inspect
import itertools
from dataclasses import dataclass, field
from typing import Any, Callable, Optional


@dataclass(order=True)
class ScheduledJob:
    """Description of work to run once or on an interval."""

    run_at: float
    name: str
    func: Callable[..., Any] = field(compare=False)
    args: tuple[Any, ...] = field(default_factory=tuple, compare=False)
    kwargs: dict[str, Any] = field(default_factory=dict, compare=False)
    interval: Optional[float] = field(default=None, compare=False)
    result: "ScheduledResult[Any]" = field(default=None, compare=False)

    def reschedule(self, now: float) -> "ScheduledJob":
        """Create a new job scheduled after the interval."""
        if self.interval is None:
            raise ValueError("Job is not recurring and cannot be rescheduled.")
        return ScheduledJob(
            run_at=now + self.interval,
            name=self.name,
            func=self.func,
            args=self.args,
            kwargs=self.kwargs,
            interval=self.interval,
            result=self.result,
        )


class ScheduledResult(asyncio.Future):
    """Future returned to callers for scheduled work."""

    def __init__(self, loop: Optional[asyncio.AbstractEventLoop] = None):
        if loop is None:
            loop = asyncio.get_event_loop()
        super().__init__(loop=loop)


class JobQueue:
    """Simple asyncio-based job queue with scheduling support."""

    def __init__(self, *, workers: int = 1):
        if workers < 1:
            raise ValueError("workers must be at least 1")
        self._workers = workers
        self._queue: asyncio.PriorityQueue[tuple[float, int, ScheduledJob]] = (
            asyncio.PriorityQueue()
        )
        self._counter = itertools.count()
        self._tasks: list[asyncio.Task[None]] = []
        self._running = False
        self._loop: Optional[asyncio.AbstractEventLoop] = None

    async def __aenter__(self) -> "JobQueue":
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.stop()

    async def start(self) -> None:
        if self._running:
            return
        self._loop = asyncio.get_running_loop()
        self._running = True
        for _ in range(self._workers):
            task = asyncio.create_task(self._worker())
            self._tasks.append(task)

    async def stop(self) -> None:
        if not self._running:
            return
        self._running = False
        for _ in self._tasks:
            await self._queue.put((0.0, -1, None))
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()

    def schedule(
        self,
        func: Callable[..., Any],
        *,
        delay: float = 0.0,
        interval: Optional[float] = None,
        name: Optional[str] = None,
        args: Optional[tuple[Any, ...]] = None,
        kwargs: Optional[dict[str, Any]] = None,
    ) -> ScheduledResult:
        """Schedule a callable to run after ``delay`` seconds.

        If ``interval`` is provided, the callable is re-enqueued after it runs.
        Returns a :class:`ScheduledResult` that resolves with the callable's
        return value or exception on first execution.
        """

        if not self._running:
            raise RuntimeError("Queue must be started before scheduling jobs")
        if delay < 0:
            raise ValueError("delay must be non-negative")
        if interval is not None and interval <= 0:
            raise ValueError("interval must be positive when provided")

        if self._loop is None:
            self._loop = asyncio.get_running_loop()
        result = ScheduledResult(loop=self._loop)
        run_at = self._loop.time() + delay
        job = ScheduledJob(
            run_at=run_at,
            name=name or func.__name__,
            func=func,
            args=args or tuple(),
            kwargs=kwargs or {},
            interval=interval,
            result=result,
        )
        count = next(self._counter)
        self._queue.put_nowait((run_at, count, job))
        return result

    async def _worker(self) -> None:
        assert self._loop is not None
        loop = self._loop
        while self._running:
            run_at, _, job = await self._queue.get()
            if job is None:
                continue
            now = loop.time()
            if run_at > now:
                await asyncio.sleep(run_at - now)
            try:
                result = job.func(*job.args, **job.kwargs)
                if inspect.isawaitable(result):
                    result = await result
                if not job.result.done():
                    job.result.set_result(result)
            except Exception as exc:  # noqa: BLE001
                if not job.result.done():
                    job.result.set_exception(exc)
            finally:
                if job.interval is not None and self._running:
                    next_job = job.reschedule(loop.time())
                    count = next(self._counter)
                    self._queue.put_nowait((next_job.run_at, count, next_job))
            self._queue.task_done()
