"""Functional tests for the private Feed-affine shard state machine."""

from __future__ import annotations

import asyncio
import dataclasses
import datetime
import unittest
import uuid

from backend.pipeline.ingestion.feed_work_scheduler import _shard, _types
from backend.pipeline.storage import feed_store, ingestion_lease_store

_OWNER_ID = uuid.UUID("11111111-2222-3333-4444-555555555555")
_FEED_IDS = tuple(uuid.UUID(int=value) for value in range(1, 5))
_SOURCE_TIME = datetime.datetime(2026, 7, 12, 12, 0, tzinfo=datetime.UTC)


def _grant(
    *,
    lease_key: str = "150",
    fencing_token: int = 1,
) -> ingestion_lease_store.LeaseGrant:
    return ingestion_lease_store.LeaseGrant(
        source_type=feed_store.SourceType.BCFY_CALLS,
        lease_key=lease_key,
        owner_worker_id=_OWNER_ID,
        fencing_token=fencing_token,
    )


def _assign_attribute(value: object, name: str, replacement: object) -> None:
    setattr(value, name, replacement)


def _raise_observer_failure(_failure: BaseException) -> None:
    message = "fatal observer failed"
    raise RuntimeError(message)


def _call_work(
    feed_id: uuid.UUID,
    grant: ingestion_lease_store.LeaseGrant,
    *,
    timestamp: datetime.datetime | None = _SOURCE_TIME,
    page_sequence: int = 0,
) -> _types._CallWork:
    return _types._CallWork(
        feed_id=feed_id,
        grant=grant,
        cohort_timestamp=timestamp,
        payload=object(),
        page_sequence=page_sequence,
    )


class TestSchedulerConstants(unittest.TestCase):
    def test_production_constants_are_exact(self) -> None:
        self.assertEqual(_types.PRODUCTION_SHARD_COUNT, 8)
        self.assertEqual(_types.PRODUCTION_SHARD_CAPACITY, 500)
        self.assertEqual(_types.PRODUCTION_WORKERS_PER_SHARD, 4)
        self.assertEqual(_types.PRODUCTION_HIGH_WATER, 400)
        self.assertEqual(_types.PRODUCTION_RESUME_AT, 299)

    def test_production_affinity_is_exact_uuid_modulo(self) -> None:
        for feed_id in _FEED_IDS:
            with self.subTest(feed_id=str(feed_id)):
                self.assertEqual(_types._shard_index(feed_id), feed_id.int % 8)

    def test_private_limits_validate_ordering(self) -> None:
        valid = _types._SchedulerLimits(
            shard_count=2,
            capacity=5,
            workers_per_shard=2,
            high_water=4,
            resume_at=2,
        )
        self.assertEqual(valid.capacity, 5)
        self.assertFalse(hasattr(valid, "__dict__"))

        invalid_values = (
            {"shard_count": 0},
            {"capacity": 0},
            {"workers_per_shard": 0},
            {"high_water": 6},
            {"resume_at": 4},
            {"resume_at": -1},
        )
        defaults = {
            "shard_count": 2,
            "capacity": 5,
            "workers_per_shard": 2,
            "high_water": 4,
            "resume_at": 2,
        }
        for replacement in invalid_values:
            with self.subTest(replacement=replacement):
                with self.assertRaises((TypeError, ValueError)):
                    _types._SchedulerLimits(**(defaults | replacement))

    def test_call_values_are_frozen(self) -> None:
        grant = _grant()
        payload = object()
        work = _types._CallWork(
            feed_id=_FEED_IDS[0],
            grant=grant,
            cohort_timestamp=_SOURCE_TIME,
            payload=payload,
            page_sequence=3,
        )
        record = _types._CallRecord(work=work, local_sequence=11)

        self.assertIs(work.grant, grant)
        self.assertIs(work.payload, payload)
        self.assertEqual(record.local_sequence, 11)
        self.assertFalse(hasattr(work, "__dict__"))
        self.assertFalse(hasattr(record, "__dict__"))
        with self.assertRaises(dataclasses.FrozenInstanceError):
            _assign_attribute(record, "local_sequence", 12)


class _ImmediateExecutor:
    def __init__(self) -> None:
        self.sequences: list[int] = []

    async def execute(self, record: _types._CallRecord) -> None:
        self.sequences.append(record.local_sequence)


class _GateExecutor:
    def __init__(self) -> None:
        self.started: list[int] = []
        self.changed = asyncio.Event()
        self._release: dict[int, asyncio.Event] = {}

    async def execute(self, record: _types._CallRecord) -> None:
        sequence = record.local_sequence
        self._release.setdefault(sequence, asyncio.Event())
        self.started.append(sequence)
        self.changed.set()
        await self._release[sequence].wait()

    async def wait_for_started(self, count: int) -> None:
        while len(self.started) < count:
            self.changed.clear()
            if len(self.started) >= count:
                return
            await asyncio.wait_for(self.changed.wait(), timeout=1)

    def allow(self, sequence: int) -> None:
        self._release.setdefault(sequence, asyncio.Event()).set()


class _CancellationExecutor:
    def __init__(self, *, swallow: bool) -> None:
        self.swallow = swallow
        self.entered = asyncio.Event()
        self.cancellation_seen = asyncio.Event()
        self.release = asyncio.Event()

    async def execute(self, record: _types._CallRecord) -> None:
        del record
        self.entered.set()
        try:
            await self.release.wait()
        except asyncio.CancelledError:
            self.cancellation_seen.set()
            if not self.swallow:
                raise
            current = asyncio.current_task()
            if current is None:
                message = "executor is not running in a Task"
                raise RuntimeError(message)
            current.uncancel()
            await self.release.wait()


class _FailingExecutor:
    def __init__(self) -> None:
        self.entered = asyncio.Event()

    async def execute(self, record: _types._CallRecord) -> None:
        del record
        self.entered.set()
        message = "unexpected executor failure"
        raise RuntimeError(message)


class TestShard(unittest.IsolatedAsyncioTestCase):
    def _limits(
        self,
        *,
        workers: int = 2,
        capacity: int = 10,
    ) -> _types._SchedulerLimits:
        return _types._SchedulerLimits(
            shard_count=1,
            capacity=capacity,
            workers_per_shard=workers,
            high_water=capacity,
            resume_at=max(0, capacity - 1),
        )

    async def test_fixed_workers_preserve_per_feed_fifo(self) -> None:
        executor = _GateExecutor()
        shard = _shard._Shard(0, executor, limits=self._limits())
        grant = _grant()
        await shard.start()
        try:
            hot = _FEED_IDS[0]
            cold = _FEED_IDS[1]
            records = (
                await shard.admit(_call_work(hot, grant)),
                await shard.admit(_call_work(hot, grant)),
                await shard.admit(_call_work(cold, grant)),
                await shard.admit(
                    _call_work(
                        hot,
                        grant,
                        timestamp=_SOURCE_TIME - datetime.timedelta(seconds=1),
                    )
                ),
            )
            await executor.wait_for_started(2)
            self.assertEqual(set(executor.started[:2]), {0, 2})

            executor.allow(0)
            executor.allow(2)
            await executor.wait_for_started(3)
            self.assertEqual(executor.started[2], 1)
            executor.allow(1)
            await executor.wait_for_started(4)
            self.assertEqual(executor.started[3], 3)
            executor.allow(3)
            await shard.wait_for_held(0)

            self.assertEqual(
                [record.local_sequence for record in records],
                [0, 1, 2, 3],
            )
            self.assertEqual(executor.started, [0, 2, 1, 3])
        finally:
            await shard.close()

    async def test_exact_purge_releases_only_queued_predecessor_work(
        self,
    ) -> None:
        executor = _GateExecutor()
        shard = _shard._Shard(
            0,
            executor,
            limits=self._limits(workers=1),
        )
        predecessor = _grant(fencing_token=1)
        successor = _grant(fencing_token=2)
        feed_id = _FEED_IDS[0]
        await shard.start()
        try:
            await shard.admit(_call_work(feed_id, predecessor))
            await executor.wait_for_started(1)
            await shard.admit(_call_work(feed_id, predecessor))
            await shard.admit(_call_work(feed_id, successor))

            await shard.purge_exact(predecessor)
            executor.allow(0)
            await executor.wait_for_started(2)

            self.assertEqual(executor.started, [0, 2])
            executor.allow(2)
            await shard.wait_for_held(0)
        finally:
            await shard.close()

    async def test_expected_cancellation_replaces_worker(self) -> None:
        executor = _CancellationExecutor(swallow=False)
        shard = _shard._Shard(
            0,
            executor,
            limits=self._limits(workers=1),
        )
        grant = _grant()
        await shard.start()
        try:
            await shard.admit(_call_work(_FEED_IDS[0], grant))
            await asyncio.wait_for(executor.entered.wait(), timeout=1)

            self.assertEqual(await shard.cancel_active_exact(grant), (0,))
            self.assertTrue(executor.cancellation_seen.is_set())

            followup = _ImmediateExecutor()
            shard._executor = followup
            await shard.admit(_call_work(_FEED_IDS[1], grant))
            await shard.wait_for_held(0)
            self.assertEqual(followup.sequences, [1])
        finally:
            await shard.close()

    async def test_interrupted_exact_cancellation_fails_closed(self) -> None:
        executor = _CancellationExecutor(swallow=True)
        shard = _shard._Shard(
            0,
            executor,
            limits=self._limits(workers=1),
        )
        grant = _grant()
        await shard.start()
        await shard.admit(_call_work(_FEED_IDS[0], grant))
        await asyncio.wait_for(executor.entered.wait(), timeout=1)
        cancellation = asyncio.create_task(shard.cancel_active_exact(grant))
        await asyncio.wait_for(executor.cancellation_seen.wait(), timeout=1)

        cancellation.cancel()
        try:
            with self.assertRaises(asyncio.CancelledError):
                await cancellation
            self.assertIsInstance(
                shard.fatal_failure,
                _shard._ShardUndrainedError,
            )
            self.assertTrue(shard._workers[0].abandoned)
        finally:
            executor.release.set()
            tasks = tuple(
                slot.task for slot in shard._workers if slot.task is not None
            )
            await asyncio.gather(*tasks, return_exceptions=True)

    async def test_concurrent_exact_cancellation_is_idempotent(self) -> None:
        executor = _CancellationExecutor(swallow=True)
        shard = _shard._Shard(
            0,
            executor,
            limits=self._limits(workers=1),
        )
        grant = _grant()
        await shard.start()
        await shard.admit(_call_work(_FEED_IDS[0], grant))
        await asyncio.wait_for(executor.entered.wait(), timeout=1)
        first = asyncio.create_task(shard.cancel_active_exact(grant))
        await asyncio.wait_for(executor.cancellation_seen.wait(), timeout=1)
        second = asyncio.create_task(shard.cancel_active_exact(grant))
        await asyncio.sleep(0)
        executor.release.set()

        try:
            results = await asyncio.gather(
                first,
                second,
                return_exceptions=True,
            )
            self.assertEqual(results, [(0,), ()])
        finally:
            executor.release.set()
            await shard.wait_for_held(0)
            await shard.close()

    async def test_wait_for_held_rejects_nonzero_targets(self) -> None:
        executor = _GateExecutor()
        shard = _shard._Shard(
            0,
            executor,
            limits=self._limits(workers=1),
        )
        grant = _grant()
        await shard.start()
        record = await shard.admit(_call_work(_FEED_IDS[0], grant))
        await executor.wait_for_started(1)
        try:
            with self.assertRaisesRegex(
                ValueError,
                "only supports draining",
            ):
                await shard.wait_for_held(1)
        finally:
            executor.allow(record.local_sequence)
            await shard.wait_for_held(0)
            await shard.close()

    async def test_cancelled_close_can_be_retried(self) -> None:
        shard = _shard._Shard(
            0,
            _ImmediateExecutor(),
            limits=self._limits(workers=1),
        )
        await shard.start()
        closing = asyncio.create_task(shard.close())
        await asyncio.sleep(0)
        self.assertFalse(closing.done())

        closing.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await closing

        await shard.close()
        self.assertTrue(shard._closed)

    async def test_close_accepts_worker_cancelled_during_shutdown(self) -> None:
        shard = _shard._Shard(
            0,
            _ImmediateExecutor(),
            limits=self._limits(workers=1),
        )
        await shard.start()
        tasks = tuple(
            slot.task for slot in shard._workers if slot.task is not None
        )
        async with shard._lock:
            shard._stopping = True
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

        await shard.close()
        self.assertTrue(shard._closed)

    async def test_abandoned_cancellation_fails_closed(self) -> None:
        executor = _CancellationExecutor(swallow=True)
        shard = _shard._Shard(
            0,
            executor,
            limits=self._limits(workers=1),
        )
        grant = _grant()
        await shard.start()
        await shard.admit(_call_work(_FEED_IDS[0], grant))
        await asyncio.wait_for(executor.entered.wait(), timeout=1)
        requests = await shard.request_cancel_exact(grant)
        await asyncio.wait_for(executor.cancellation_seen.wait(), timeout=1)

        failure = RuntimeError("worker swallowed cancellation")
        await shard.abandon_cancellation(requests[0].slot_id, failure)
        self.assertIs(shard.fatal_failure, failure)
        with self.assertRaises(_shard._ShardFatalError):
            await shard.admit(_call_work(_FEED_IDS[1], grant))

        executor.release.set()
        await asyncio.wait_for(requests[0].task, timeout=1)

    async def test_fatal_observer_cannot_skip_waiter_notification(
        self,
    ) -> None:
        executor = _CancellationExecutor(swallow=True)
        shard = _shard._Shard(
            0,
            executor,
            limits=self._limits(workers=1),
            fatal_observer=_raise_observer_failure,
        )
        grant = _grant()
        await shard.start()
        await shard.admit(_call_work(_FEED_IDS[0], grant))
        await asyncio.wait_for(executor.entered.wait(), timeout=1)
        requests = await shard.request_cancel_exact(grant)
        await asyncio.wait_for(executor.cancellation_seen.wait(), timeout=1)
        waiter = asyncio.create_task(shard.wait_for_held(0))
        await asyncio.sleep(0)

        try:
            failure = RuntimeError("worker swallowed cancellation")
            with self.assertRaisesRegex(RuntimeError, "observer failed"):
                await shard.abandon_cancellation(
                    requests[0].slot_id,
                    failure,
                )
            with self.assertRaises(_shard._ShardFatalError):
                await asyncio.wait_for(waiter, timeout=1)
        finally:
            if not waiter.done():
                waiter.cancel()
            executor.release.set()
            await asyncio.wait_for(requests[0].task, timeout=1)

    async def test_executor_failure_is_persistent(self) -> None:
        executor = _FailingExecutor()
        fatal = asyncio.Event()
        shard = _shard._Shard(
            0,
            executor,
            limits=self._limits(workers=1),
            fatal_observer=lambda _failure: fatal.set(),
        )
        await shard.start()
        await shard.admit(_call_work(_FEED_IDS[0], _grant()))
        await asyncio.wait_for(executor.entered.wait(), timeout=1)
        await fatal.wait()

        self.assertIsInstance(shard.fatal_failure, RuntimeError)
        with self.assertRaises(_shard._ShardFatalError):
            await shard.admit(_call_work(_FEED_IDS[1], _grant()))


if __name__ == "__main__":
    unittest.main()
