"""Deterministic oracle for the private Feed-affine shard state machine."""

from __future__ import annotations

import dataclasses
import datetime
import enum
import importlib
import random
import typing
import unittest
import uuid

from backend.pipeline.storage import feed_store, ingestion_lease_store

if typing.TYPE_CHECKING:
    import types


_OWNER_ID = uuid.UUID("11111111-2222-3333-4444-555555555555")
_OTHER_OWNER_ID = uuid.UUID("22222222-3333-4444-5555-666666666666")
_FEED_IDS = tuple(uuid.UUID(int=value) for value in range(1, 5))
_SOURCE_TIME = datetime.datetime(2026, 7, 12, 12, 0, tzinfo=datetime.UTC)


def _scheduler_types() -> types.ModuleType:
    """Load the private production vocabulary under test."""
    return importlib.import_module(
        "backend.pipeline.ingestion.feed_work_scheduler._types"
    )


def _grant(
    *,
    lease_key: str = "150",
    owner_worker_id: uuid.UUID = _OWNER_ID,
    fencing_token: int = 1,
) -> ingestion_lease_store.LeaseGrant:
    return ingestion_lease_store.LeaseGrant(
        source_type=feed_store.SourceType.BCFY_CALLS,
        lease_key=lease_key,
        owner_worker_id=owner_worker_id,
        fencing_token=fencing_token,
    )


@dataclasses.dataclass(frozen=True, slots=True)
class _ModelLimits:
    capacity: int = 5
    workers: int = 2
    high_water: int = 4
    resume_at: int = 2


_DEFAULT_MODEL_LIMITS = _ModelLimits()


class _ModelState(enum.StrEnum):
    QUEUED = "queued"
    ACTIVE = "active"
    PENDING_BOUNDARY = "pending_boundary"
    FLUSHING_BOUNDARY = "flushing_boundary"


@dataclasses.dataclass(slots=True)
class _ModelRecord:
    sequence: int
    feed_id: uuid.UUID
    grant: ingestion_lease_store.LeaseGrant
    source_order: int
    state: _ModelState
    worker_slot: int | None = None


@dataclasses.dataclass(frozen=True, slots=True)
class _ModelSnapshot:
    held: int
    queued_calls: int
    active_calls: int
    pending_boundaries: int
    flushing_boundaries: int
    pressure_paused: bool
    ready_feeds: tuple[uuid.UUID, ...]
    active_feeds: frozenset[uuid.UUID]
    local_sequences: tuple[int, ...]
    fatal: bool


class _ModelViolation(RuntimeError):
    """A generated command attempted an invalid scheduler transition."""


def _violation(message: str) -> typing.Never:
    raise _ModelViolation(message)


def _assign_attribute(
    target: object,
    field_name: str,
    value: object,
) -> None:
    setattr(target, field_name, value)


class _ReferenceModel:
    """Independent small-limit oracle for shard transition semantics."""

    def __init__(self, limits: _ModelLimits = _DEFAULT_MODEL_LIMITS) -> None:
        self.limits = limits
        self.held = 0
        self.pressure_paused = False
        self.fatal = False
        self._next_sequence = 0
        self._records: dict[int, _ModelRecord] = {}
        self._feed_queues: dict[uuid.UUID, list[int]] = {}
        self._ready: list[uuid.UUID] = []
        self._ready_members: set[uuid.UUID] = set()
        self._active_by_feed: dict[uuid.UUID, int] = {}
        self._active_by_slot: dict[int, int] = {}
        self._pending_boundary_by_feed: dict[uuid.UUID, int] = {}
        self._flushing_boundary_by_feed: dict[uuid.UUID, int] = {}
        self._released_sequences: set[int] = set()
        self._assert_invariants()

    @property
    def live_sequences(self) -> tuple[int, ...]:
        return tuple(sorted(self._records))

    @property
    def active_sequences(self) -> tuple[int, ...]:
        return tuple(sorted(self._active_by_slot.values()))

    @property
    def pending_boundary_sequences(self) -> tuple[int, ...]:
        return tuple(sorted(self._pending_boundary_by_feed.values()))

    @property
    def flushing_boundary_sequences(self) -> tuple[int, ...]:
        return tuple(sorted(self._flushing_boundary_by_feed.values()))

    def snapshot(self) -> _ModelSnapshot:
        counts = self._counts()
        return _ModelSnapshot(
            held=self.held,
            queued_calls=counts[_ModelState.QUEUED],
            active_calls=counts[_ModelState.ACTIVE],
            pending_boundaries=counts[_ModelState.PENDING_BOUNDARY],
            flushing_boundaries=counts[_ModelState.FLUSHING_BOUNDARY],
            pressure_paused=self.pressure_paused,
            ready_feeds=tuple(self._ready),
            active_feeds=frozenset(self._active_by_feed)
            | frozenset(self._flushing_boundary_by_feed),
            local_sequences=self.live_sequences,
            fatal=self.fatal,
        )

    def admit_call(
        self,
        feed_id: uuid.UUID,
        grant: ingestion_lease_store.LeaseGrant,
        *,
        source_order: int,
    ) -> int | None:
        if (
            self.fatal
            or self.pressure_paused
            or self.held >= self.limits.capacity
        ):
            self._assert_invariants()
            return None
        sequence = self._allocate_sequence()
        record = _ModelRecord(
            sequence=sequence,
            feed_id=feed_id,
            grant=grant,
            source_order=source_order,
            state=_ModelState.QUEUED,
        )
        self._records[sequence] = record
        self._feed_queues.setdefault(feed_id, []).append(sequence)
        self.held += 1
        if self.held >= self.limits.high_water:
            self.pressure_paused = True
        if (
            feed_id not in self._active_by_feed
            and feed_id not in self._flushing_boundary_by_feed
        ):
            self._ensure_ready(feed_id)
        self._assert_invariants()
        return sequence

    def dispatch(self, worker_slot: int) -> int | None:
        if worker_slot < 0 or worker_slot >= self.limits.workers:
            _violation("worker slot is outside the fixed registry")
        if worker_slot in self._active_by_slot:
            _violation("worker slot already owns an active record")
        if not self._ready:
            self._assert_invariants()
            return None
        feed_id = self._ready.pop(0)
        self._ready_members.remove(feed_id)
        return self._dispatch_feed(feed_id, worker_slot)

    def _dispatch_feed(self, feed_id: uuid.UUID, worker_slot: int) -> int:
        if feed_id in self._active_by_feed:
            _violation("Feed already has an active call")
        if feed_id in self._flushing_boundary_by_feed:
            _violation("Feed already has an active boundary")
        queue = self._feed_queues.get(feed_id)
        if not queue:
            _violation("ready Feed has no queued call")
        sequence = queue.pop(0)
        if not queue:
            del self._feed_queues[feed_id]
        record = self._records[sequence]
        record.state = _ModelState.ACTIVE
        record.worker_slot = worker_slot
        self._active_by_feed[feed_id] = sequence
        self._active_by_slot[worker_slot] = sequence
        self._assert_invariants()
        return sequence

    def terminalize(self, sequence: int) -> None:
        record = self._require_record(sequence, _ModelState.ACTIVE)
        worker_slot = record.worker_slot
        if worker_slot is None:
            _violation("active record has no worker owner")
        del self._active_by_slot[worker_slot]
        del self._active_by_feed[record.feed_id]
        self._release(sequence)
        if record.feed_id in self._feed_queues:
            self._ensure_ready(record.feed_id)
        self._assert_invariants()

    def admit_boundary(
        self,
        feed_id: uuid.UUID,
        grant: ingestion_lease_store.LeaseGrant,
        *,
        source_order: int,
    ) -> int | None:
        if (
            self.fatal
            or self.pressure_paused
            or self.held >= self.limits.capacity
        ):
            self._assert_invariants()
            return None
        if (
            feed_id in self._pending_boundary_by_feed
            or feed_id in self._flushing_boundary_by_feed
        ):
            _violation("Feed already owns a boundary placeholder")
        sequence = self._allocate_sequence()
        self._records[sequence] = _ModelRecord(
            sequence=sequence,
            feed_id=feed_id,
            grant=grant,
            source_order=source_order,
            state=_ModelState.PENDING_BOUNDARY,
        )
        self._pending_boundary_by_feed[feed_id] = sequence
        self.held += 1
        if self.held >= self.limits.high_water:
            self.pressure_paused = True
        self._assert_invariants()
        return sequence

    def start_boundary_flush(self, feed_id: uuid.UUID) -> int:
        if feed_id in self._active_by_feed:
            _violation("Feed call still owns ordering")
        if feed_id in self._feed_queues:
            _violation("queued call remains ahead of boundary")
        try:
            sequence = self._pending_boundary_by_feed.pop(feed_id)
        except KeyError:
            _violation("Feed has no pending boundary")
        record = self._records[sequence]
        record.state = _ModelState.FLUSHING_BOUNDARY
        self._flushing_boundary_by_feed[feed_id] = sequence
        self._assert_invariants()
        return sequence

    def finish_boundary_flush(self, sequence: int) -> None:
        record = self._require_record(
            sequence,
            _ModelState.FLUSHING_BOUNDARY,
        )
        del self._flushing_boundary_by_feed[record.feed_id]
        self._release(sequence)
        if record.feed_id in self._feed_queues:
            self._ensure_ready(record.feed_id)
        self._assert_invariants()

    def purge_exact(
        self,
        grant: ingestion_lease_store.LeaseGrant,
    ) -> tuple[int, ...]:
        releasable_states = {
            _ModelState.QUEUED,
            _ModelState.PENDING_BOUNDARY,
        }
        purged = tuple(
            sequence
            for sequence, record in sorted(self._records.items())
            if record.grant == grant and record.state in releasable_states
        )
        for sequence in purged:
            record = self._records[sequence]
            if record.state is _ModelState.QUEUED:
                queue = self._feed_queues[record.feed_id]
                queue.remove(sequence)
                if not queue:
                    del self._feed_queues[record.feed_id]
                    self._remove_ready(record.feed_id)
            else:
                del self._pending_boundary_by_feed[record.feed_id]
            self._release(sequence)
        self._assert_invariants()
        return purged

    def mark_fatal(self) -> None:
        self.fatal = True
        self._assert_invariants()

    def _allocate_sequence(self) -> int:
        sequence = self._next_sequence
        self._next_sequence += 1
        return sequence

    def _append_ready(self, feed_id: uuid.UUID) -> None:
        if feed_id in self._ready_members:
            _violation("duplicate ready-ring membership")
        self._ready.append(feed_id)
        self._ready_members.add(feed_id)

    def _ensure_ready(self, feed_id: uuid.UUID) -> None:
        if feed_id not in self._ready_members:
            self._append_ready(feed_id)

    def _remove_ready(self, feed_id: uuid.UUID) -> None:
        if feed_id not in self._ready_members:
            return
        self._ready_members.remove(feed_id)
        self._ready.remove(feed_id)

    def _release(self, sequence: int) -> None:
        if sequence in self._released_sequences:
            _violation("record token released more than once")
        if sequence not in self._records:
            _violation("record token is not live")
        del self._records[sequence]
        self._released_sequences.add(sequence)
        self.held -= 1
        if self.pressure_paused and self.held <= self.limits.resume_at:
            self.pressure_paused = False

    def _require_record(
        self,
        sequence: int,
        state: _ModelState,
    ) -> _ModelRecord:
        record = self._records.get(sequence)
        if record is None:
            if sequence in self._released_sequences:
                _violation("record token already released")
            _violation("record token was never registered")
        if record.state is not state:
            message = f"record is {record.state.value}, not {state.value}"
            _violation(message)
        return record

    def _counts(self) -> dict[_ModelState, int]:
        return {
            state: sum(
                record.state is state for record in self._records.values()
            )
            for state in _ModelState
        }

    def _assert_invariants(self) -> None:  # noqa: PLR0912
        counts = self._counts()
        conserved = sum(counts.values())
        if self.held != conserved:
            _violation("held conservation equation failed")
        if self.held < 0 or self.held > self.limits.capacity:
            _violation("held is outside the hard capacity")
        if len(self._ready) != len(set(self._ready)):
            _violation("ready ring contains a duplicate Feed")
        if set(self._ready) != self._ready_members:
            _violation("ready ring and membership set disagree")
        if len(self._active_by_slot) > self.limits.workers:
            _violation("fixed worker bound exceeded")
        if len(self._active_by_feed) != len(set(self._active_by_feed)):
            _violation("Feed owns multiple active calls")
        active_feeds = set(self._active_by_feed)
        flushing_feeds = set(self._flushing_boundary_by_feed)
        if active_feeds & flushing_feeds:
            _violation("Feed owns concurrent active actions")
        if self._released_sequences & self._records.keys():
            _violation("released token became live again")
        for record in self._records.values():
            if not isinstance(record.grant, ingestion_lease_store.LeaseGrant):
                _violation("record lost its complete LeaseGrant")
        for feed_id in self._ready:
            if feed_id not in self._feed_queues:
                _violation("ready Feed has no queued record")
            if feed_id in active_feeds:
                _violation("active Feed remained ready")


class TestSchedulerConstants(unittest.TestCase):
    """Tests for immutable production limits and closed record values."""

    def test_production_constants_are_exact(self) -> None:
        scheduler_types = _scheduler_types()

        self.assertEqual(scheduler_types.PRODUCTION_SHARD_COUNT, 8)
        self.assertEqual(scheduler_types.PRODUCTION_SHARD_CAPACITY, 500)
        self.assertEqual(scheduler_types.PRODUCTION_WORKERS_PER_SHARD, 4)
        self.assertEqual(scheduler_types.PRODUCTION_HIGH_WATER, 400)
        self.assertEqual(scheduler_types.PRODUCTION_RESUME_AT, 299)

    def test_private_model_limits_validate_ordering(self) -> None:
        scheduler_types = _scheduler_types()
        valid = scheduler_types._SchedulerLimits(
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
                arguments = defaults | replacement
                with self.assertRaises((TypeError, ValueError)):
                    scheduler_types._SchedulerLimits(**arguments)

    def test_scheduler_call_values_are_frozen_and_exact(self) -> None:
        scheduler_types = _scheduler_types()
        grant = _grant()
        payload = object()
        work = scheduler_types._CallWork(
            feed_id=_FEED_IDS[0],
            grant=grant,
            source_order=7,
            source_timestamp=_SOURCE_TIME,
            payload=payload,
            page_sequence=3,
        )
        record = scheduler_types._CallRecord(
            work=work,
            local_sequence=11,
        )

        self.assertIs(work.grant, grant)
        self.assertIs(work.payload, payload)
        self.assertEqual(record.local_sequence, 11)
        self.assertFalse(hasattr(work, "__dict__"))
        self.assertFalse(hasattr(record, "__dict__"))
        with self.assertRaises(dataclasses.FrozenInstanceError):
            _assign_attribute(record, "local_sequence", 12)

    def test_closed_executor_outcomes_contain_no_policy(self) -> None:
        scheduler_types = _scheduler_types()
        outcomes = (
            scheduler_types._ExecutorCompleted(),
            scheduler_types._ExecutorRetryable(),
            scheduler_types._ExecutorAuthorityLost(),
            scheduler_types._ExecutorMembershipRejected(),
            scheduler_types._ExecutorIntegrityFailure(RuntimeError("boom")),
        )
        forbidden = {
            "backoff",
            "budget",
            "credential",
            "failure_reason",
            "payload",
            "quarantine",
            "retry_after",
        }

        for outcome in outcomes:
            with self.subTest(outcome=type(outcome).__name__):
                self.assertTrue(dataclasses.is_dataclass(outcome))
                self.assertFalse(hasattr(outcome, "__dict__"))
                self.assertTrue(forbidden.isdisjoint(outcome.__slots__))


class TestConservationModel(unittest.TestCase):
    """Deterministic transition tests for the independent oracle."""

    def test_model_conservation_and_pressure_latch(self) -> None:
        model = _ReferenceModel()
        grant = _grant()

        admitted = tuple(
            model.admit_call(
                _FEED_IDS[index % 2],
                grant,
                source_order=index,
            )
            for index in range(4)
        )
        self.assertEqual(admitted, (0, 1, 2, 3))
        self.assertEqual(model.snapshot().held, 4)
        self.assertTrue(model.snapshot().pressure_paused)
        self.assertIsNone(model.admit_call(_FEED_IDS[2], grant, source_order=4))
        self.assertEqual(model.live_sequences, admitted)

        first = model.dispatch(0)
        second = model.dispatch(1)
        self.assertEqual(model.snapshot().held, 4)
        assert first is not None
        assert second is not None
        model.terminalize(first)
        self.assertTrue(model.snapshot().pressure_paused)
        model.terminalize(second)
        self.assertFalse(model.snapshot().pressure_paused)
        self.assertEqual(model.snapshot().held, 2)

        resumed = model.admit_call(
            _FEED_IDS[2],
            grant,
            source_order=4,
        )
        self.assertEqual(resumed, 4)

    def test_model_exact_full_grant_purge_preserves_successors(self) -> None:
        model = _ReferenceModel()
        old = _grant(fencing_token=7)
        successor = _grant(fencing_token=8)
        sibling = _grant(lease_key="151", fencing_token=7)
        same_fence_other_owner = _grant(
            owner_worker_id=_OTHER_OWNER_ID,
            fencing_token=7,
        )

        old_sequence = model.admit_call(_FEED_IDS[0], old, source_order=0)
        successor_sequence = model.admit_call(
            _FEED_IDS[1], successor, source_order=1
        )
        sibling_sequence = model.admit_call(
            _FEED_IDS[2], sibling, source_order=2
        )
        owner_sequence = model.admit_call(
            _FEED_IDS[3], same_fence_other_owner, source_order=3
        )

        purged = model.purge_exact(old)

        self.assertEqual(purged, (old_sequence,))
        self.assertEqual(
            model.live_sequences,
            tuple(
                typing.cast("int", sequence)
                for sequence in (
                    successor_sequence,
                    sibling_sequence,
                    owner_sequence,
                )
            ),
        )
        self.assertEqual(model.snapshot().held, 3)

    def test_model_boundary_placeholders_participate_in_held_equation(
        self,
    ) -> None:
        model = _ReferenceModel()
        sequence = model.admit_boundary(
            _FEED_IDS[0],
            _grant(),
            source_order=0,
        )
        self.assertEqual(sequence, 0)
        self.assertEqual(model.snapshot().pending_boundaries, 1)
        flushing = model.start_boundary_flush(_FEED_IDS[0])
        self.assertEqual(flushing, sequence)
        self.assertEqual(model.snapshot().flushing_boundaries, 1)
        self.assertEqual(model.snapshot().held, 1)
        model.finish_boundary_flush(flushing)
        self.assertEqual(model.snapshot().held, 0)

    def test_model_rejects_invalid_ownership_and_duplicate_release(
        self,
    ) -> None:
        model = _ReferenceModel()
        sequence = model.admit_call(
            _FEED_IDS[0],
            _grant(),
            source_order=0,
        )
        self.assertEqual(sequence, 0)
        model.admit_call(_FEED_IDS[0], _grant(), source_order=1)
        active = model.dispatch(0)
        self.assertEqual(active, sequence)

        with self.assertRaisesRegex(_ModelViolation, "active call"):
            model._dispatch_feed(_FEED_IDS[0], 1)
        model.terminalize(typing.cast("int", active))
        with self.assertRaisesRegex(_ModelViolation, "already released"):
            model.terminalize(typing.cast("int", active))

        with self.assertRaisesRegex(_ModelViolation, "duplicate ready"):
            model._append_ready(_FEED_IDS[0])

    def test_seeded_model_checks_every_transition(self) -> None:  # noqa: PLR0912
        seeds = (0xC0FFEE, 0x5EED, 0xA11CE)
        grants = (
            _grant(fencing_token=1),
            _grant(fencing_token=2),
            _grant(lease_key="151", fencing_token=1),
        )
        for seed in seeds:
            with self.subTest(seed=seed):
                generator = random.Random(seed)  # noqa: S311
                model = _ReferenceModel()
                source_order = 0
                for step in range(96):
                    command = generator.randrange(7)
                    if command in (0, 1):
                        model.admit_call(
                            generator.choice(_FEED_IDS),
                            generator.choice(grants),
                            source_order=source_order,
                        )
                        source_order += 1
                    elif command == 2:
                        free_slots = tuple(
                            slot
                            for slot in range(model.limits.workers)
                            if slot not in model._active_by_slot
                        )
                        if free_slots:
                            model.dispatch(generator.choice(free_slots))
                    elif command == 3 and model.active_sequences:
                        model.terminalize(
                            generator.choice(model.active_sequences)
                        )
                    elif command == 4:
                        model.purge_exact(generator.choice(grants))
                    elif command == 5:
                        candidates = tuple(
                            feed_id
                            for feed_id in _FEED_IDS
                            if feed_id not in model._pending_boundary_by_feed
                            and feed_id not in model._flushing_boundary_by_feed
                        )
                        if candidates:
                            model.admit_boundary(
                                generator.choice(candidates),
                                generator.choice(grants),
                                source_order=source_order,
                            )
                            source_order += 1
                    elif command == 6:
                        ready_boundaries = tuple(
                            feed_id
                            for feed_id in model._pending_boundary_by_feed
                            if feed_id not in model._active_by_feed
                            and feed_id not in model._feed_queues
                        )
                        if ready_boundaries:
                            model.start_boundary_flush(
                                generator.choice(ready_boundaries)
                            )
                        elif model.flushing_boundary_sequences:
                            model.finish_boundary_flush(
                                generator.choice(
                                    model.flushing_boundary_sequences
                                )
                            )
                    try:
                        model._assert_invariants()
                    except _ModelViolation as exc:
                        self.fail(
                            f"seed={seed} step={step} command={command}: {exc}"
                        )

                before_fatal = model.snapshot()
                model.mark_fatal()
                self.assertIsNone(
                    model.admit_call(
                        _FEED_IDS[0],
                        grants[0],
                        source_order=source_order,
                    ),
                    f"seed={seed}",
                )
                self.assertEqual(model.snapshot().held, before_fatal.held)


if __name__ == "__main__":
    unittest.main()
