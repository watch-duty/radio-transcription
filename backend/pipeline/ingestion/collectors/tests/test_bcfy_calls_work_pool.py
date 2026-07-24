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


class _PutCompletionQueue[ItemT](asyncio.Queue[ItemT]):
    """Expose queue-put completion before its admission waiter resumes."""

    def __init__(self, maxsize: int) -> None:
        super().__init__(maxsize=maxsize)
        self.put_completed = asyncio.Event()
        self.put_count = 0

    async def put(self, item: ItemT) -> None:
        await super().put(item)
        self.put_count += 1
        self.put_completed.set()


class _FailingPutQueue[ItemT](asyncio.Queue[ItemT]):
    """Fail one selected admission after earlier queue puts complete."""

    def __init__(
        self,
        maxsize: int,
        fail_on: int,
        failure: BaseException,
    ) -> None:
        super().__init__(maxsize=maxsize)
        self._fail_on = fail_on
        self._failure = failure
        self.put_count = 0

    async def put(self, item: ItemT) -> None:
        self.put_count += 1
        if self.put_count == self._fail_on:
            raise self._failure
        await super().put(item)


class TestBcfyCallsWorkPool(unittest.IsolatedAsyncioTestCase):
    """Verify bounded admission, settlement, and worker lifecycle."""

    async def test_queue_capacity_backpressures_later_admission(self) -> None:
        started: asyncio.Queue[str] = asyncio.Queue()
        gates = {batch: asyncio.Event() for batch in ("one", "two", "three")}

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
        settlement_task = asyncio.create_task(
            pool.settle_batches(("one", "two", "three"))
        )
        try:
            self.assertEqual(await started.get(), "one")
            for _attempt in range(20):
                if pool._submitting:
                    break
                await asyncio.sleep(0)
            self.assertEqual(pool._submitting, 1)
            self.assertFalse(settlement_task.done())

            gates["one"].set()
            self.assertEqual(await started.get(), "two")
            gates["two"].set()
            self.assertEqual(await started.get(), "three")
            gates["three"].set()

            settled = await settlement_task
            self.assertEqual(
                settled.results,
                (("one", "ONE"), ("two", "TWO"), ("three", "THREE")),
            )
            self.assertIsNone(settled.failure)
            self.assertIsNone(settled.cancellation)
        finally:
            for gate in gates.values():
                gate.set()
            if not settlement_task.done():
                await asyncio.gather(
                    settlement_task,
                    return_exceptions=True,
                )
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
        settlement_task = asyncio.create_task(
            pool.settle_batches(("one", "two", "three"))
        )
        try:
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
            settled = await settlement_task
            self.assertEqual(
                settled.results,
                (("one", "one"), ("two", "two"), ("three", "three")),
            )
            self.assertEqual(peak_active, 2)
        finally:
            release.set()
            if not settlement_task.done():
                await asyncio.gather(
                    settlement_task,
                    return_exceptions=True,
                )
            await pool.close()

    async def test_results_and_first_failure_keep_caller_order(self) -> None:
        batches = ("good-one", "bad-one", "good-two", "bad-two")
        gates = {batch: asyncio.Event() for batch in batches}
        started: asyncio.Queue[str] = asyncio.Queue()
        first_failure = RuntimeError("first failure")
        second_failure = ValueError("second failure")

        async def execute(batch: str) -> str:
            await started.put(batch)
            await gates[batch].wait()
            if batch == "bad-one":
                raise first_failure
            if batch == "bad-two":
                raise second_failure
            return batch.upper()

        pool = work_pool.BcfyCallsWorkPool(
            _Executor(execute),
            concurrency=4,
            queue_capacity=4,
        )
        await pool.start()
        settlement_task = asyncio.create_task(pool.settle_batches(batches))
        try:
            observed = {await started.get() for _batch in batches}
            self.assertEqual(observed, set(batches))
            for batch in reversed(batches):
                gates[batch].set()

            settled = await settlement_task
            self.assertEqual(
                settled.results,
                (("good-one", "GOOD-ONE"), ("good-two", "GOOD-TWO")),
            )
            self.assertIs(settled.failure, first_failure)
            self.assertIsNone(settled.cancellation)
        finally:
            for gate in gates.values():
                gate.set()
            if not settlement_task.done():
                await asyncio.gather(
                    settlement_task,
                    return_exceptions=True,
                )
            await pool.close()

    async def test_executor_cancellation_is_failure_not_caller_cancel(
        self,
    ) -> None:
        executed: list[str] = []

        async def execute(batch: str) -> str:
            executed.append(batch)
            if batch == "cancelled":
                message = "executor"
                raise asyncio.CancelledError(message)
            return batch.upper()

        pool = work_pool.BcfyCallsWorkPool(
            _Executor(execute),
            concurrency=1,
            queue_capacity=2,
        )
        await pool.start()
        try:
            settled = await pool.settle_batches(("cancelled", "good"))

            self.assertEqual(settled.results, (("good", "GOOD"),))
            self.assertIsInstance(settled.failure, asyncio.CancelledError)
            self.assertIsNone(settled.cancellation)
            self.assertEqual(executed, ["cancelled", "good"])
        finally:
            await pool.close()

    async def test_partial_admission_settles_accepted_prefix(self) -> None:
        failure = RuntimeError("queue put failed")
        executed: list[str] = []

        async def execute(batch: str) -> str:
            executed.append(batch)
            return batch.upper()

        pool = work_pool.BcfyCallsWorkPool(
            _Executor(execute),
            concurrency=1,
            queue_capacity=2,
        )
        queue: _FailingPutQueue[work_pool._QueuedBatch[str, str]] = (
            _FailingPutQueue(2, 2, failure)
        )
        pool._queue = queue
        await pool.start()
        try:
            settled = await pool.settle_batches(("one", "two", "three"))

            self.assertEqual(settled.results, (("one", "ONE"),))
            self.assertIs(settled.failure, failure)
            self.assertIsNone(settled.cancellation)
            self.assertEqual(queue.put_count, 2)
            self.assertEqual(executed, ["one"])
        finally:
            await pool.close()

    async def test_cancel_racing_completed_put_keeps_accepted_batch(
        self,
    ) -> None:
        started = asyncio.Event()
        release = asyncio.Event()
        executed: list[str] = []

        async def execute(batch: str) -> str:
            executed.append(batch)
            started.set()
            await release.wait()
            return batch.upper()

        pool = work_pool.BcfyCallsWorkPool(
            _Executor(execute),
            concurrency=1,
            queue_capacity=1,
        )
        queue: _PutCompletionQueue[work_pool._QueuedBatch[str, str]] = (
            _PutCompletionQueue(1)
        )
        pool._queue = queue
        await pool.start()
        settlement_task = asyncio.create_task(
            pool.settle_batches(("accepted", "later"))
        )
        try:
            await queue.put_completed.wait()
            first_marker = object()
            second_marker = object()
            self.assertTrue(settlement_task.cancel(first_marker))
            await asyncio.sleep(0)
            self.assertTrue(settlement_task.cancel(second_marker))
            await asyncio.sleep(0)
            await started.wait()
            self.assertFalse(settlement_task.done())

            release.set()
            settled = await settlement_task
            self.assertEqual(
                settled.results,
                (("accepted", "ACCEPTED"),),
            )
            self.assertIsNone(settled.failure)
            cancellation = typing.cast(
                "asyncio.CancelledError",
                settled.cancellation,
            )
            self.assertIs(cancellation.args[0], first_marker)
            self.assertEqual(queue.put_count, 1)
            self.assertEqual(executed, ["accepted"])
        finally:
            release.set()
            if not settlement_task.done():
                await asyncio.gather(
                    settlement_task,
                    return_exceptions=True,
                )
            await pool.close()

    async def test_cancelled_blocked_put_stops_later_admission(self) -> None:
        started = asyncio.Event()
        release = asyncio.Event()
        executed: list[str] = []

        async def execute(batch: str) -> str:
            executed.append(batch)
            if batch == "active":
                started.set()
                await release.wait()
            return batch.upper()

        pool = work_pool.BcfyCallsWorkPool(
            _Executor(execute),
            concurrency=1,
            queue_capacity=1,
        )
        await pool.start()
        settlement_task = asyncio.create_task(
            pool.settle_batches(("active", "queued", "blocked", "later"))
        )
        try:
            await started.wait()
            for _attempt in range(20):
                if pool._submitting and pool._queue.qsize() == 1:
                    break
                await asyncio.sleep(0)
            self.assertEqual(pool._submitting, 1)
            self.assertEqual(pool._queue.qsize(), 1)

            marker = object()
            settlement_task.cancel(marker)
            await asyncio.sleep(0)
            release.set()
            settled = await settlement_task

            self.assertEqual(
                settled.results,
                (("active", "ACTIVE"), ("queued", "QUEUED")),
            )
            self.assertIsNone(settled.failure)
            cancellation = typing.cast(
                "asyncio.CancelledError",
                settled.cancellation,
            )
            self.assertIs(cancellation.args[0], marker)
            self.assertEqual(executed, ["active", "queued"])
            self.assertEqual(pool._submitting, 0)
            self.assertTrue(pool._submissions_drained.is_set())
        finally:
            release.set()
            if not settlement_task.done():
                await asyncio.gather(
                    settlement_task,
                    return_exceptions=True,
                )
            await pool.close()

    async def test_cancellation_and_execution_failure_are_both_retained(
        self,
    ) -> None:
        started = asyncio.Event()
        finish = asyncio.Event()
        failure = RuntimeError("execution failed")

        async def execute(batch: str) -> str:
            started.set()
            await finish.wait()
            raise failure

        pool = work_pool.BcfyCallsWorkPool(
            _Executor(execute),
            concurrency=1,
            queue_capacity=1,
        )
        await pool.start()
        settlement_task = asyncio.create_task(pool.settle_batches(("one",)))
        try:
            await started.wait()
            first_marker = object()
            second_marker = object()
            settlement_task.cancel(first_marker)
            await asyncio.sleep(0)
            settlement_task.cancel(second_marker)
            await asyncio.sleep(0)

            finish.set()
            settled = await settlement_task
            self.assertEqual(settled.results, ())
            self.assertIs(settled.failure, failure)
            cancellation = typing.cast(
                "asyncio.CancelledError",
                settled.cancellation,
            )
            self.assertIs(cancellation.args[0], first_marker)
        finally:
            finish.set()
            if not settlement_task.done():
                await asyncio.gather(
                    settlement_task,
                    return_exceptions=True,
                )
            await pool.close()

    async def test_concurrent_group_cancellation_is_isolated(self) -> None:
        first_started = asyncio.Event()
        release_first = asyncio.Event()

        async def execute(batch: str) -> str:
            if batch == "first":
                first_started.set()
                await release_first.wait()
            return batch.upper()

        pool = work_pool.BcfyCallsWorkPool(
            _Executor(execute),
            concurrency=2,
            queue_capacity=2,
        )
        await pool.start()
        first_group = asyncio.create_task(pool.settle_batches(("first",)))
        try:
            await first_started.wait()
            second_group = await pool.settle_batches(("second",))
            self.assertEqual(
                second_group,
                work_pool.SettledBatches(
                    (("second", "SECOND"),),
                    None,
                    None,
                ),
            )

            marker = object()
            first_group.cancel(marker)
            await asyncio.sleep(0)
            self.assertFalse(first_group.done())
            release_first.set()

            first_settlement = await first_group
            self.assertEqual(
                first_settlement.results,
                (("first", "FIRST"),),
            )
            self.assertIsNone(first_settlement.failure)
            cancellation = typing.cast(
                "asyncio.CancelledError",
                first_settlement.cancellation,
            )
            self.assertIs(cancellation.args[0], marker)
        finally:
            release_first.set()
            if not first_group.done():
                await asyncio.gather(first_group, return_exceptions=True)
            await pool.close()

    async def test_close_rejects_late_work_and_drains_settlement(self) -> None:
        started = asyncio.Event()
        release = asyncio.Event()

        async def execute(batch: str) -> str:
            started.set()
            await release.wait()
            return batch.upper()

        pool = work_pool.BcfyCallsWorkPool(
            _Executor(execute),
            concurrency=1,
            queue_capacity=1,
        )
        await pool.start()
        settlement_task = asyncio.create_task(
            pool.settle_batches(("one", "two"))
        )
        await started.wait()
        for _attempt in range(20):
            if pool._submitting == 0 and pool._queue.qsize() == 1:
                break
            await asyncio.sleep(0)

        close_task = asyncio.create_task(pool.close())
        await asyncio.sleep(0)
        late = await pool.settle_batches(("late",))
        self.assertEqual(late.results, ())
        self.assertIsInstance(late.failure, RuntimeError)
        self.assertIn("not accepting", str(late.failure))
        self.assertFalse(close_task.done())

        release.set()
        settled = await asyncio.wait_for(settlement_task, timeout=1)
        self.assertEqual(
            settled.results,
            (("one", "ONE"), ("two", "TWO")),
        )
        await asyncio.wait_for(close_task, timeout=1)
        await pool.close()

    async def test_worker_termination_settles_work_and_close_fails(
        self,
    ) -> None:
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
        settlement_task = asyncio.create_task(
            pool.settle_batches(("active", "queued"))
        )
        await started.wait()
        for _attempt in range(20):
            if pool._submitting == 0 and pool._queue.qsize() == 1:
                break
            await asyncio.sleep(0)

        worker_owner = typing.cast(
            "asyncio.Task[None]",
            pool._worker_owner,
        )
        worker_owner.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await worker_owner

        settled = await asyncio.wait_for(settlement_task, timeout=1)
        self.assertEqual(settled.results, ())
        self.assertIsInstance(settled.failure, asyncio.CancelledError)
        self.assertIsNone(settled.cancellation)
        for _attempt in range(2):
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
        worker_owner = typing.cast(
            "asyncio.Task[None]",
            pool._worker_owner,
        )
        self.assertTrue(worker_owner.done())

    async def test_settlement_requires_one_successful_start(self) -> None:
        async def execute(batch: str) -> str:
            return batch.upper()

        pool = work_pool.BcfyCallsWorkPool(
            _Executor(execute),
            concurrency=1,
            queue_capacity=1,
        )
        early = await pool.settle_batches(("early",))
        self.assertIsInstance(early.failure, RuntimeError)
        self.assertIn("not accepting", str(early.failure))

        await pool.start()
        with self.assertRaisesRegex(RuntimeError, "only be started once"):
            await pool.start()
        empty = await pool.settle_batches(())
        self.assertEqual(empty, work_pool.SettledBatches((), None, None))
        await pool.close()

        late = await pool.settle_batches(("late",))
        self.assertIsInstance(late.failure, RuntimeError)
        self.assertIn("not accepting", str(late.failure))


class TestConfiguration(unittest.TestCase):
    """Reject invalid fixed pool bounds at construction."""

    def test_positive_integer_bounds_are_required(self) -> None:
        executor = _Executor(lambda batch: asyncio.sleep(0, batch))

        with self.assertRaises(TypeError):
            work_pool.BcfyCallsWorkPool(
                executor,
                concurrency=True,
                queue_capacity=1,
            )
        with self.assertRaises(ValueError):
            work_pool.BcfyCallsWorkPool(
                executor,
                concurrency=1,
                queue_capacity=0,
            )
