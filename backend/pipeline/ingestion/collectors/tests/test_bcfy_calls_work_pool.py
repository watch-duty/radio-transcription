"""Tests for the bounded Broadcastify Calls Feed-batch work pool."""

from __future__ import annotations

import asyncio
import typing
import unittest

from backend.pipeline.ingestion.collectors.bcfy_calls import work_pool

if typing.TYPE_CHECKING:
    import collections.abc


class _Executor[BatchT, ResultT]:
    """Adapt one async test operation to the pool executor protocol."""

    def __init__(
        self,
        operation: collections.abc.Callable[
            [BatchT],
            collections.abc.Awaitable[ResultT],
        ],
    ) -> None:
        self._operation = operation

    async def execute(self, batch: BatchT) -> ResultT:
        return await self._operation(batch)


class _CleanupCancellationEvent(asyncio.Event):
    """Expose when cancellation cleanup is waiting for its child task."""

    def __init__(self) -> None:
        super().__init__()
        self.cleanup_entered = asyncio.Event()
        self._hold_cleanup = asyncio.Event()

    async def wait(self) -> typing.Literal[True]:
        try:
            return await super().wait()
        except asyncio.CancelledError:
            self.cleanup_entered.set()
            await self._hold_cleanup.wait()
            raise


class TestBcfyCallsWorkPool(unittest.IsolatedAsyncioTestCase):
    """Verify bounded admission and fixed-worker lifecycle semantics."""

    async def test_queue_capacity_backpressures_submitter(self) -> None:
        started: asyncio.Queue[str] = asyncio.Queue()
        gates = {
            "one": asyncio.Event(),
            "two": asyncio.Event(),
            "three": asyncio.Event(),
        }

        async def execute(batch: str) -> str:
            await started.put(batch)
            await gates[batch].wait()
            return batch.upper()

        pool = work_pool.BcfyCallsWorkPool(
            _Executor(execute),
            concurrency=1,
            queue_capacity=1,
        )
        await pool.start()
        try:
            first = await pool.submit("one")
            self.assertEqual(await started.get(), "one")
            second = await pool.submit("two")

            third_submission = asyncio.create_task(pool.submit("three"))
            await asyncio.sleep(0)
            self.assertFalse(third_submission.done())

            gates["one"].set()
            third = await asyncio.wait_for(third_submission, timeout=1)

            gates["two"].set()
            gates["three"].set()
            self.assertEqual(await first, "ONE")
            self.assertEqual(await second, "TWO")
            self.assertEqual(await third, "THREE")
        finally:
            for gate in gates.values():
                gate.set()
            await pool.close()

    async def test_fixed_workers_overlap_only_configured_batches(self) -> None:
        release = asyncio.Event()
        started: asyncio.Queue[str] = asyncio.Queue()
        active = 0
        peak_active = 0

        async def execute(batch: str) -> str:
            nonlocal active, peak_active
            active += 1
            peak_active = max(peak_active, active)
            await started.put(batch)
            try:
                await release.wait()
                return batch
            finally:
                active -= 1

        pool = work_pool.BcfyCallsWorkPool(
            _Executor(execute),
            concurrency=2,
            queue_capacity=3,
        )
        await pool.start()
        try:
            completions = [
                await pool.submit(batch) for batch in ("one", "two", "three")
            ]
            first_started = await started.get()
            second_started = await started.get()
            self.assertEqual(
                {first_started, second_started},
                {"one", "two"},
            )
            self.assertEqual(active, 2)
            self.assertEqual(peak_active, 2)
            self.assertTrue(started.empty())

            release.set()
            self.assertEqual(
                await asyncio.gather(*completions),
                ["one", "two", "three"],
            )
            self.assertEqual(peak_active, 2)
        finally:
            release.set()
            await pool.close()

    async def test_executor_exception_only_settles_its_batch(self) -> None:
        executed: list[str] = []

        async def execute(batch: str) -> str:
            executed.append(batch)
            if batch == "bad":
                message = "bad batch"
                raise ValueError(message)
            return batch.upper()

        pool = work_pool.BcfyCallsWorkPool(
            _Executor(execute),
            concurrency=1,
            queue_capacity=2,
        )
        await pool.start()
        try:
            failed = await pool.submit("bad")
            succeeded = await pool.submit("good")

            with self.assertRaisesRegex(ValueError, "bad batch"):
                await failed
            self.assertEqual(await succeeded, "GOOD")
            self.assertEqual(executed, ["bad", "good"])
        finally:
            await pool.close()

    async def test_batch_local_cancellation_only_settles_its_batch(
        self,
    ) -> None:
        executed: list[str] = []

        async def execute(batch: str) -> str:
            executed.append(batch)
            if batch == "cancelled":
                raise asyncio.CancelledError
            return batch.upper()

        pool = work_pool.BcfyCallsWorkPool(
            _Executor(execute),
            concurrency=1,
            queue_capacity=1,
        )
        await pool.start()
        try:
            cancelled = await pool.submit("cancelled")
            succeeded = await pool.submit("good")

            with self.assertRaises(asyncio.CancelledError):
                await cancelled
            self.assertEqual(await succeeded, "GOOD")
            self.assertEqual(executed, ["cancelled", "good"])
        finally:
            await pool.close()

    async def test_cancelled_worker_settles_active_completion(self) -> None:
        started = asyncio.Event()

        async def execute(batch: str) -> str:
            started.set()
            await asyncio.Event().wait()
            return batch

        pool = work_pool.BcfyCallsWorkPool(
            _Executor(execute),
            concurrency=1,
            queue_capacity=1,
        )
        await pool.start()
        completion = await pool.submit("one")
        await started.wait()

        worker_owner = pool._worker_owner
        self.assertIsNotNone(worker_owner)
        owner_task = typing.cast("asyncio.Task[None]", worker_owner)
        owner_task.cancel()

        with self.assertRaises(asyncio.CancelledError):
            await completion
        with self.assertRaises(asyncio.CancelledError):
            await owner_task

    async def test_cancelled_worker_exits_when_executor_reports_uncertainty(
        self,
    ) -> None:
        started = asyncio.Event()

        async def execute(batch: str) -> str:
            started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError as error:
                message = "outcome unknown"
                raise RuntimeError(message) from error
            return batch

        pool = work_pool.BcfyCallsWorkPool(
            _Executor(execute),
            concurrency=1,
            queue_capacity=1,
        )
        await pool.start()
        completion = await pool.submit("one")
        await started.wait()

        worker_owner = pool._worker_owner
        self.assertIsNotNone(worker_owner)
        owner_task = typing.cast("asyncio.Task[None]", worker_owner)
        owner_task.cancel()

        with self.assertRaisesRegex(RuntimeError, "outcome unknown"):
            await completion
        with self.assertRaises(asyncio.CancelledError):
            await asyncio.wait_for(owner_task, timeout=1)

    async def test_cancelled_worker_exits_when_executor_returns(self) -> None:
        started = asyncio.Event()
        execution_count = 0

        async def execute(batch: str) -> str:
            nonlocal execution_count
            execution_count += 1
            if execution_count > 1:
                raise asyncio.CancelledError
            started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                pass
            return batch

        pool = work_pool.BcfyCallsWorkPool(
            _Executor(execute),
            concurrency=1,
            queue_capacity=1,
        )
        await pool.start()
        completion = await pool.submit("one")
        await started.wait()

        worker_owner = pool._worker_owner
        self.assertIsNotNone(worker_owner)
        owner_task = typing.cast("asyncio.Task[None]", worker_owner)
        owner_task.cancel()

        self.assertEqual(await completion, "one")
        done, _pending = await asyncio.wait((owner_task,), timeout=0.05)
        try:
            self.assertIn(owner_task, done)
        finally:
            if not owner_task.done():
                cleanup = await pool.submit("cleanup")
                with self.assertRaises(asyncio.CancelledError):
                    await cleanup
                with self.assertRaises(asyncio.CancelledError):
                    await owner_task

    async def test_cancelled_worker_settles_queued_completions(self) -> None:
        started = asyncio.Event()

        async def execute(batch: str) -> str:
            started.set()
            await asyncio.Event().wait()
            return batch

        pool = work_pool.BcfyCallsWorkPool(
            _Executor(execute),
            concurrency=1,
            queue_capacity=2,
        )
        await pool.start()
        active = await pool.submit("active")
        await started.wait()
        queued = await pool.submit("queued")

        worker_owner = pool._worker_owner
        self.assertIsNotNone(worker_owner)
        owner_task = typing.cast("asyncio.Task[None]", worker_owner)
        owner_task.cancel()

        with self.assertRaises(asyncio.CancelledError):
            await active
        with self.assertRaises(asyncio.CancelledError):
            await queued
        with self.assertRaises(asyncio.CancelledError):
            await owner_task
        with self.assertRaisesRegex(RuntimeError, "workers terminated"):
            await asyncio.wait_for(pool.close(), timeout=1)

    async def test_start_cancellation_stops_worker_owner(self) -> None:
        entered = asyncio.Event()
        stopped = asyncio.Event()

        class _BlockedStartPool(
            work_pool.BcfyCallsWorkPool[str, str],
        ):
            async def _run_workers(self) -> None:
                entered.set()
                try:
                    await asyncio.Event().wait()
                finally:
                    stopped.set()

        pool = _BlockedStartPool(
            _Executor(lambda batch: asyncio.sleep(0, batch)),
            concurrency=1,
            queue_capacity=1,
        )

        start_task = asyncio.create_task(pool.start())
        await entered.wait()
        start_task.cancel()

        with self.assertRaises(asyncio.CancelledError):
            await start_task
        await asyncio.wait_for(stopped.wait(), timeout=1)
        worker_owner = pool._worker_owner
        self.assertIsNotNone(worker_owner)
        self.assertTrue(typing.cast("asyncio.Task[None]", worker_owner).done())

    async def test_cancelled_completion_does_not_discard_accepted_work(
        self,
    ) -> None:
        first_started = asyncio.Event()
        release_first = asyncio.Event()
        executed: list[str] = []

        async def execute(batch: str) -> str:
            executed.append(batch)
            if batch == "first":
                first_started.set()
                await release_first.wait()
            return batch

        pool = work_pool.BcfyCallsWorkPool(
            _Executor(execute),
            concurrency=1,
            queue_capacity=1,
        )
        await pool.start()
        try:
            first = await pool.submit("first")
            await first_started.wait()
            abandoned_observer = await pool.submit("second")
            abandoned_observer.cancel()

            release_first.set()
            self.assertEqual(await first, "first")
            await asyncio.wait_for(pool.close(), timeout=1)
            self.assertEqual(executed, ["first", "second"])
        finally:
            release_first.set()
            await pool.close()

    async def test_close_rejects_new_work_and_drains_accepted_batches(
        self,
    ) -> None:
        release = asyncio.Event()
        first_started = asyncio.Event()

        async def execute(batch: str) -> str:
            if batch == "one":
                first_started.set()
            await release.wait()
            return batch

        pool = work_pool.BcfyCallsWorkPool(
            _Executor(execute),
            concurrency=1,
            queue_capacity=1,
        )
        await pool.start()
        first = await pool.submit("one")
        await first_started.wait()
        second = await pool.submit("two")
        third_submission = asyncio.create_task(pool.submit("three"))
        await asyncio.sleep(0)
        self.assertFalse(third_submission.done())

        close_task = asyncio.create_task(pool.close())
        await asyncio.sleep(0)
        with self.assertRaisesRegex(RuntimeError, "not accepting"):
            await pool.submit("late")
        self.assertFalse(close_task.done())

        release.set()
        third = await asyncio.wait_for(third_submission, timeout=1)
        self.assertEqual(await first, "one")
        self.assertEqual(await second, "two")
        self.assertEqual(await third, "three")
        await asyncio.wait_for(close_task, timeout=1)
        await pool.close()

    async def test_submit_cancellation_cannot_leak_admission_accounting(
        self,
    ) -> None:
        async def execute(batch: str) -> str:
            return batch

        pool = work_pool.BcfyCallsWorkPool(
            _Executor(execute),
            concurrency=1,
            queue_capacity=1,
        )
        await pool.start()
        cancellation_gate = _CleanupCancellationEvent()
        pool._workers_stopped = cancellation_gate

        submission = asyncio.create_task(pool.submit("accepted"))
        await cancellation_gate.cleanup_entered.wait()
        submission.cancel()

        try:
            with self.assertRaises(asyncio.CancelledError):
                await submission
            self.assertEqual(pool._submitting, 0)
            self.assertTrue(pool._submissions_drained.is_set())
        finally:
            pool._submissions_drained.set()
            await pool.close()

    async def test_cancelled_submit_settles_already_admitted_batch(
        self,
    ) -> None:
        started = asyncio.Event()
        release = asyncio.Event()
        settled = asyncio.Event()

        async def execute(batch: str) -> str:
            started.set()
            try:
                await release.wait()
                return batch
            finally:
                settled.set()

        pool = work_pool.BcfyCallsWorkPool(
            _Executor(execute),
            concurrency=1,
            queue_capacity=1,
        )
        await pool.start()
        cancellation_gate = _CleanupCancellationEvent()
        pool._workers_stopped = cancellation_gate

        submission = asyncio.create_task(pool.submit("accepted"))
        await cancellation_gate.cleanup_entered.wait()
        await started.wait()
        submission.cancel()
        done, _pending = await asyncio.wait(
            (submission,),
            timeout=0.05,
        )

        try:
            self.assertFalse(done)
            self.assertFalse(settled.is_set())
            release.set()
            with self.assertRaises(asyncio.CancelledError):
                await submission
            self.assertTrue(settled.is_set())
        finally:
            release.set()
            pool._submissions_drained.set()
            await pool.close()

    async def test_repeated_close_preserves_worker_failure(self) -> None:
        async def execute(batch: str) -> str:
            return batch

        pool = work_pool.BcfyCallsWorkPool(
            _Executor(execute),
            concurrency=1,
            queue_capacity=1,
        )
        await pool.start()
        worker_owner = pool._worker_owner
        self.assertIsNotNone(worker_owner)
        owner_task = typing.cast("asyncio.Task[None]", worker_owner)
        owner_task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await owner_task

        for _attempt in range(2):
            with self.assertRaisesRegex(RuntimeError, "workers terminated"):
                await pool.close()

    async def test_submit_requires_one_successful_start(self) -> None:
        async def execute(batch: str) -> str:
            return batch

        pool = work_pool.BcfyCallsWorkPool(
            _Executor(execute),
            concurrency=1,
            queue_capacity=1,
        )

        with self.assertRaisesRegex(RuntimeError, "not accepting"):
            await pool.submit("early")

        await pool.start()
        with self.assertRaisesRegex(RuntimeError, "only be started once"):
            await pool.start()
        await pool.close()

        with self.assertRaisesRegex(RuntimeError, "not accepting"):
            await pool.submit("late")
