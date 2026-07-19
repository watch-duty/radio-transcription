"""Bounded asynchronous execution for Broadcastify Calls Feed batches."""

from __future__ import annotations

import asyncio
import dataclasses
import typing


class _BatchExecutor[BatchT, ResultT](typing.Protocol):
    """Source-specific operation executed once for each admitted batch."""

    async def execute(self, batch: BatchT) -> ResultT:
        """Execute one complete batch without pool-level interleaving."""
        ...


@dataclasses.dataclass(frozen=True, slots=True)
class _QueuedBatch[BatchT, ResultT]:
    """One admitted batch and the future settled by its worker."""

    batch: BatchT
    completion: asyncio.Future[ResultT]


def _require_positive(value: int, field_name: str) -> int:
    """Validate one externally configured positive integer."""
    if isinstance(value, bool):
        message = f"{field_name} must be an integer"
        raise TypeError(message)
    if value <= 0:
        message = f"{field_name} must be positive"
        raise ValueError(message)
    return value


class BcfyCallsWorkPool[BatchT, ResultT]:
    """Run complete Feed batches through a fixed bounded worker pool.

    A submission owns one queue permit until a worker begins executing it.
    The injected executor owns all ordering within that batch. Fixed workers
    allow different batches to overlap without the pool understanding their
    source-specific contents.
    """

    def __init__(
        self,
        executor: _BatchExecutor[BatchT, ResultT],
        concurrency: int,
        queue_capacity: int,
    ) -> None:
        """Initialize an inactive pool.

        Args:
            executor: Source-specific atomic batch executor.
            concurrency: Fixed number of worker tasks.
            queue_capacity: Maximum number of batches waiting for a worker.

        Raises:
            TypeError: A numeric setting is boolean.
            ValueError: A numeric setting is not positive.
        """
        self._executor = executor
        self._concurrency = _require_positive(concurrency, "concurrency")
        self._queue: asyncio.Queue[_QueuedBatch[BatchT, ResultT] | None] = (
            asyncio.Queue(
                maxsize=_require_positive(queue_capacity, "queue_capacity")
            )
        )

        self._start_called = False
        self._started = False
        self._admission_open = False
        self._closed = False
        self._submitting = 0
        self._submissions_drained = asyncio.Event()
        self._submissions_drained.set()
        self._workers_started = asyncio.Event()
        self._worker_owner: asyncio.Task[None] | None = None
        self._close_task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        """Start the fixed workers exactly once.

        Raises:
            RuntimeError: The pool was already started.
        """
        if self._start_called:
            message = "Broadcastify Calls work pool may only be started once"
            raise RuntimeError(message)

        self._start_called = True
        self._worker_owner = asyncio.create_task(
            self._run_workers(),
            name="bcfy-calls-work-pool",
        )
        await self._workers_started.wait()
        if self._worker_owner.done():
            self._worker_owner.result()
        self._started = True
        self._admission_open = True

    async def submit(
        self,
        batch: BatchT,
    ) -> asyncio.Future[ResultT]:
        """Admit one complete batch, waiting for bounded queue capacity.

        Args:
            batch: Opaque source-specific batch executed atomically.

        Returns:
            A future settled with the executor result or exception.

        Raises:
            RuntimeError: The pool is not accepting submissions.
            asyncio.CancelledError: Admission is cancelled while backpressured.
        """
        if not self._admission_open:
            message = "Broadcastify Calls work pool is not accepting work"
            raise RuntimeError(message)

        completion = asyncio.get_running_loop().create_future()
        queued = _QueuedBatch(batch=batch, completion=completion)
        self._submitting += 1
        self._submissions_drained.clear()
        try:
            await self._queue.put(queued)
        except BaseException:
            completion.cancel()
            raise
        finally:
            self._submitting -= 1
            if self._submitting == 0:
                self._submissions_drained.set()
        return completion

    async def close(self) -> None:
        """Stop admission and drain every accepted batch before returning.

        Repeated and concurrent close calls await the same drain operation.

        Raises:
            RuntimeError: The pool has not been started.
        """
        if not self._started:
            message = "Broadcastify Calls work pool has not been started"
            raise RuntimeError(message)
        if self._closed:
            return

        if self._close_task is None:
            self._admission_open = False
            self._close_task = asyncio.create_task(
                self._drain_and_close(),
                name="bcfy-calls-work-pool-close",
            )
        await asyncio.shield(self._close_task)

    async def _run_workers(self) -> None:
        """Own all fixed workers in one structured-concurrency scope."""
        async with asyncio.TaskGroup() as task_group:
            for worker_index in range(self._concurrency):
                task_group.create_task(
                    self._worker(),
                    name=f"bcfy-calls-work-pool-{worker_index}",
                )
            self._workers_started.set()

    async def _worker(self) -> None:
        """Execute queued batches until the post-drain stop marker arrives."""
        while True:
            queued = await self._queue.get()
            try:
                if queued is None:
                    return
                try:
                    result = await self._executor.execute(queued.batch)
                except asyncio.CancelledError:
                    queued.completion.cancel()
                    raise
                except Exception as error:
                    if not queued.completion.done():
                        queued.completion.set_exception(error)
                else:
                    if not queued.completion.done():
                        queued.completion.set_result(result)
            finally:
                self._queue.task_done()

    async def _drain_and_close(self) -> None:
        """Close the admission race, drain work, and stop fixed workers."""
        await self._submissions_drained.wait()
        await self._queue.join()
        for _worker_index in range(self._concurrency):
            await self._queue.put(None)

        worker_owner = self._worker_owner
        if worker_owner is None:
            message = "started pool is missing its worker owner"
            raise RuntimeError(message)
        await worker_owner
        self._closed = True
