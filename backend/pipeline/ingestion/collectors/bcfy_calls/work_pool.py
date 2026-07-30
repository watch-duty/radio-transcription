"""Bounded asynchronous execution for Broadcastify Calls Feed batches."""

from __future__ import annotations

import asyncio
import collections.abc  # noqa: TC003 - public hints resolve at runtime.
import dataclasses
import typing


@dataclasses.dataclass(frozen=True, slots=True)
class SettledBatches[BatchT, ResultT]:
    """Terminal evidence for one caller-ordered batch sequence.

    Attributes:
        results: Successful accepted batches paired with their results in
            exact caller order.
        failure: First unexpected admission or execution failure, if any.
        cancellation: First exact caller cancellation received while
            admission or settlement was in progress.
    """

    results: tuple[tuple[BatchT, ResultT], ...]
    failure: BaseException | None
    cancellation: asyncio.CancelledError | None


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


@dataclasses.dataclass(frozen=True, slots=True)
class _BatchAdmission[ResultT]:
    """Resolved outcome of one queue admission attempt.

    Attributes:
        completion: Accepted batch completion, or ``None`` before acceptance.
        failure: Unexpected admission failure, if one occurred.
        cancellation: First exact caller cancellation deferred during
            admission settlement.
    """

    completion: asyncio.Future[ResultT] | None
    failure: BaseException | None
    cancellation: asyncio.CancelledError | None


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


async def _wait_for_all[ResultT](
    futures: collections.abc.Iterable[asyncio.Future[ResultT]],
    cancellation: asyncio.CancelledError | None = None,
) -> asyncio.CancelledError | None:
    """Wait without transferring caller cancellation to issued work.

    Args:
        futures: Issued operations that must reach a terminal state.
        cancellation: Earlier caller cancellation to retain, if any.

    Returns:
        The first exact caller cancellation received before all operations
        settled, or ``None``.
    """
    pending = {future for future in futures if not future.done()}
    while pending:
        try:
            _done, pending = await asyncio.wait(pending)
        except asyncio.CancelledError as error:
            if cancellation is None:
                cancellation = error
    return cancellation


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
        self._workers_stopped: asyncio.Future[None] | None = None
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
        self._workers_stopped = asyncio.get_running_loop().create_future()
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

    async def settle_batches(
        self,
        batches: collections.abc.Iterable[BatchT],
    ) -> SettledBatches[BatchT, ResultT]:
        """Admit and settle a caller-ordered sequence of complete batches.

        Queue ``put`` completion is the admission boundary. Caller
        cancellation stops later admissions, but a racing current admission
        is first resolved and every accepted batch reaches a terminal state.

        Args:
            batches: Complete batches in source caller order.

        Returns:
            Successful batch/result pairs, the first unexpected failure, and
            the first exact caller cancellation. No evidence is raised here so
            the caller can apply successful durable outcomes before choosing
            exception precedence.
        """
        accepted: list[tuple[BatchT, asyncio.Future[ResultT]]] = []
        admission_failure: BaseException | None = None
        cancellation: asyncio.CancelledError | None = None

        try:
            batch_iterator = iter(batches)
        except BaseException as error:
            return SettledBatches((), error, None)

        while admission_failure is None and cancellation is None:
            try:
                batch = next(batch_iterator)
            except StopIteration:
                break
            except BaseException as error:
                admission_failure = error
                break

            admission = await self._admit_batch(batch)
            if admission.completion is not None:
                accepted.append((batch, admission.completion))
            if admission.failure is not None:
                admission_failure = admission.failure
            if admission.cancellation is not None:
                cancellation = admission.cancellation

        cancellation = await _wait_for_all(
            (completion for _batch, completion in accepted),
            cancellation,
        )

        results: list[tuple[BatchT, ResultT]] = []
        failure: BaseException | None = None
        for batch, completion in accepted:
            try:
                result = completion.result()
            except BaseException as error:
                if failure is None:
                    failure = error
            else:
                results.append((batch, result))
        if failure is None:
            failure = admission_failure

        return SettledBatches(
            results=tuple(results),
            failure=failure,
            cancellation=cancellation,
        )

    async def _admit_batch(  # noqa: PLR0912
        self,
        batch: BatchT,
    ) -> _BatchAdmission[ResultT]:
        """Resolve one queue admission without losing caller cancellation.

        Args:
            batch: Caller-ordered Feed batch to admit.

        Returns:
            Accepted completion evidence, admission failure, or deferred
            caller cancellation.
        """
        if not self._admission_open:
            message = "Broadcastify Calls work pool is not accepting work"
            return _BatchAdmission(None, RuntimeError(message), None)

        workers_stopped = self._workers_stopped
        if workers_stopped is None:
            message = "Broadcastify Calls work pool workers are unavailable"
            return _BatchAdmission(None, RuntimeError(message), None)

        completion = asyncio.get_running_loop().create_future()
        queued = _QueuedBatch(batch=batch, completion=completion)
        self._submitting += 1
        self._submissions_drained.clear()
        admission = asyncio.create_task(self._queue.put(queued))
        failure: BaseException | None = None
        cancellation: asyncio.CancelledError | None = None
        admission_cancel_requested = False
        accepted = False
        done: set[asyncio.Future[None]] = set()

        try:
            try:
                done, _pending = await asyncio.wait(
                    (admission, workers_stopped),
                    return_when=asyncio.FIRST_COMPLETED,
                )
            except asyncio.CancelledError as error:
                cancellation = error
                done = set()

            if admission not in done and cancellation is None:
                message = "Broadcastify Calls work pool workers have terminated"
                failure = RuntimeError(message)
        finally:
            if admission not in done and not admission.done():
                admission_cancel_requested = admission.cancel()
            cancellation = await _wait_for_all(
                (admission,),
                cancellation,
            )

            if admission.cancelled():
                if not admission_cancel_requested and failure is None:
                    try:
                        admission.result()
                    except BaseException as error:
                        failure = error
            else:
                try:
                    admission.result()
                except BaseException as error:
                    if failure is None:
                        failure = error
                else:
                    accepted = True

            if not accepted:
                completion.cancel()
            self._submitting -= 1
            if self._submitting == 0:
                self._submissions_drained.set()

        return _BatchAdmission(
            completion if accepted else None,
            failure,
            cancellation,
        )

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
            workers_stopped = self._workers_stopped
            if workers_stopped is not None and not workers_stopped.done():
                workers_stopped.set_result(None)
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
