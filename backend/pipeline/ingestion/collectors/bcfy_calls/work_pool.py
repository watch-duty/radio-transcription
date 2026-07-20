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
    """Validate one externally configured positive integer.

    Args:
        value: Configured integer.
        field_name: Name included in validation errors.

    Returns:
        The validated value.

    Raises:
        TypeError: The value is boolean.
        ValueError: The value is not positive.
    """
    if isinstance(value, bool):
        message = f"{field_name} must be an integer"
        raise TypeError(message)
    if value <= 0:
        message = f"{field_name} must be positive"
        raise ValueError(message)
    return value


async def _settle_before_reraising_cancellation[ResultT](
    completion: asyncio.Future[ResultT],
    cancellation: asyncio.CancelledError,
) -> typing.NoReturn:
    """Settle accepted work before preserving caller cancellation.

    Args:
        completion: Result of work whose queue admission already committed.
        cancellation: Original cancellation to propagate after settlement.

    Raises:
        asyncio.CancelledError: Always, chained from any terminal work failure.
    """
    while not completion.done():
        try:
            await asyncio.shield(completion)
        except asyncio.CancelledError:
            continue
        except Exception:
            break

    try:
        completion.result()
    except BaseException as error:
        raise cancellation from error
    raise cancellation


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
        self._queue: asyncio.Queue[_QueuedBatch[BatchT, ResultT]] = (
            asyncio.Queue(
                maxsize=_require_positive(queue_capacity, "queue_capacity")
            )
        )

        self._start_called = False
        self._started = False
        self._admission_open = False
        self._submitting = 0
        self._submissions_drained = asyncio.Event()
        self._submissions_drained.set()
        self._workers_started = asyncio.Event()
        self._workers_stopped = asyncio.Event()
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
        started_wait = asyncio.create_task(self._workers_started.wait())
        try:
            done, _pending = await asyncio.wait(
                (started_wait, self._worker_owner),
                return_when=asyncio.FIRST_COMPLETED,
            )
            if self._worker_owner in done:
                self._worker_owner.result()
            self._started = True
            self._admission_open = True
        except BaseException:
            self._worker_owner.cancel()
            await asyncio.gather(
                self._worker_owner,
                return_exceptions=True,
            )
            raise
        finally:
            started_wait.cancel()
            await asyncio.gather(started_wait, return_exceptions=True)

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
        admission = asyncio.create_task(self._queue.put(queued))
        workers_stopped = asyncio.create_task(self._workers_stopped.wait())
        accepted = False
        try:
            try:
                done, _pending = await asyncio.wait(
                    (admission, workers_stopped),
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if admission not in done:
                    message = (
                        "Broadcastify Calls work pool workers have terminated"
                    )
                    raise RuntimeError(message)
                admission.result()
                accepted = True
            finally:
                try:
                    admission.cancel()
                    workers_stopped.cancel()
                    await asyncio.gather(
                        admission,
                        workers_stopped,
                        return_exceptions=True,
                    )
                finally:
                    # Admission may commit concurrently with caller
                    # cancellation before asyncio.wait returns its done set.
                    if (
                        not accepted
                        and admission.done()
                        and not admission.cancelled()
                    ):
                        admission.result()
                        accepted = True
                    if not accepted:
                        completion.cancel()
                    self._submitting -= 1
                    if self._submitting == 0:
                        self._submissions_drained.set()
        except asyncio.CancelledError as cancellation:
            if accepted:
                await _settle_before_reraising_cancellation(
                    completion,
                    cancellation,
                )
            raise
        return completion

    async def close(self) -> None:
        """Stop admission and drain every accepted batch before returning.

        Repeated and concurrent close calls await the same drain operation.

        Raises:
            RuntimeError: The pool has not been started or workers stopped
                before the accepted work drained.
        """
        if not self._started:
            message = "Broadcastify Calls work pool has not been started"
            raise RuntimeError(message)

        if self._close_task is None:
            self._admission_open = False
            self._close_task = asyncio.create_task(
                self._drain_and_close(),
                name="bcfy-calls-work-pool-close",
            )
        await asyncio.shield(self._close_task)

    async def _run_workers(self) -> None:
        """Own all fixed workers in one structured-concurrency scope."""
        try:
            async with asyncio.TaskGroup() as task_group:
                for worker_index in range(self._concurrency):
                    task_group.create_task(
                        self._worker(),
                        name=f"bcfy-calls-work-pool-{worker_index}",
                    )
                self._workers_started.set()
        finally:
            self._admission_open = False
            self._workers_stopped.set()
            await self._submissions_drained.wait()
            self._cancel_queued_completions()

    async def _worker(self) -> None:
        """Execute queued batches until the worker owner stops the pool."""
        while True:
            queued = await self._queue.get()
            try:
                try:
                    result = await self._executor.execute(queued.batch)
                except asyncio.CancelledError:
                    queued.completion.cancel()
                    worker = asyncio.current_task()
                    if worker is None or worker.cancelling():
                        raise
                except Exception as error:
                    if not queued.completion.done():
                        queued.completion.set_exception(error)
                    worker = asyncio.current_task()
                    if worker is not None and worker.cancelling():
                        raise asyncio.CancelledError from error
                except BaseException as error:
                    if not queued.completion.done():
                        queued.completion.set_exception(error)
                    raise
                else:
                    if not queued.completion.done():
                        queued.completion.set_result(result)
                    worker = asyncio.current_task()
                    if worker is not None and worker.cancelling():
                        raise asyncio.CancelledError
            finally:
                self._queue.task_done()

    async def _drain_and_close(self) -> None:
        """Close the admission race, drain work, and stop fixed workers."""
        await self._submissions_drained.wait()
        await self._queue.join()

        worker_owner = self._worker_owner
        if worker_owner is None:
            message = "started pool is missing its worker owner"
            raise RuntimeError(message)
        if worker_owner.done():
            if worker_owner.cancelled():
                message = "Broadcastify Calls work pool workers terminated"
                raise RuntimeError(message)
            worker_owner.result()
        else:
            worker_owner.cancel()
            try:
                await worker_owner
            except asyncio.CancelledError:
                pass

    def _cancel_queued_completions(self) -> None:
        """Settle work left behind by an unexpectedly stopped worker owner."""
        while True:
            try:
                queued = self._queue.get_nowait()
            except asyncio.QueueEmpty:
                return
            queued.completion.cancel()
            self._queue.task_done()
