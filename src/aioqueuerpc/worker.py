import asyncio
from collections.abc import Callable
from datetime import datetime
from typing import Any


from .job_spec import JobError, JobSpec


class Worker:
    _semaphore: asyncio.Semaphore
    _loop: asyncio.AbstractEventLoop

    stop_requested: bool
    job_queue: asyncio.Queue[JobSpec]

    def __init__(self, n_concurrent: int = 16, job_queue: asyncio.Queue = None) -> None:
        self._semaphore = asyncio.Semaphore(n_concurrent)
        self._loop = None

        self.stop_requested = False
        if job_queue is not None:
            self.job_queue = job_queue
        else:
            self.job_queue = asyncio.Queue()

    async def run(self):
        self._loop = asyncio.get_running_loop()
        while not self.stop_requested:
            await self._sem_start_next_job()

    async def _sem_start_next_job(self) -> None:
        def release_semaphore_slot(_sem_task: asyncio.Task) -> None:
            self._semaphore.release()

        await self._semaphore.acquire()
        job = await self.job_queue.get()
        sem_task: asyncio.Task = asyncio.create_task(
            self._run_job(job), name=f"job-{self.get_current_isodate()}-{job.name}"
        )
        sem_task.add_done_callback(release_semaphore_slot)

    async def _run_job(self, job: JobSpec) -> None:
        if job.future is None:
            job.future = self._loop.create_future()
        try:
            result = await self._exec_in_task(func=job.coro, params=job.params)
            job.future.set_result(result)
        except JobError as e:
            job.future.set_exception(e)

    async def _exec_in_task(self, func: Callable, params: dict[str, Any]) -> Any:
        task = asyncio.create_task(
            self._exec_coro(func, params), name=f"coro-{self.get_current_isodate()}"
        )
        future = self._loop.create_future()

        def done_cb(_task: asyncio.Task) -> None:
            future.set_result(None)

        task.add_done_callback(done_cb)
        try:
            await future
        except asyncio.CancelledError as exc:
            # the worker is likely shutting down,
            if task.cancel():
                await task
            raise JobError(
                f"{asyncio.current_task()} got cancelled", cancelled=True
            ) from exc

        if task.cancelled():
            # normally this would be raised only if the task got cancelled from within
            raise JobError(f"{task.get_name()} got cancelled", cancelled=True)
        exc = task.exception()
        if exc is not None:
            raise JobError(f"{task.get_name()} raised {exc}", exc=exc)
        return task.result()

    @staticmethod
    async def _exec_coro(func: Callable, params: dict[str, Any] = None) -> Any:
        if params is None:
            params = {}
        return await func(**params)

    @staticmethod
    def get_current_isodate():
        return datetime.now().isoformat(timespec="microseconds")
