"""Deterministic oracle for the private Feed-affine shard state machine."""

from __future__ import annotations

import asyncio
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


def _scheduler_shard() -> types.ModuleType:
    """Load the private authoritative shard implementation under test."""
    return importlib.import_module(
        "backend.pipeline.ingestion.feed_work_scheduler._shard"
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


@dataclasses.dataclass(frozen=True, slots=True)
class _PageCoverageSnapshot:
    """O(1) page state independent from counted shard records."""

    next_page_sequence: int
    page_sequence: int | None
    current_source_order: int | None
    pulled: int
    registered: int
    localized: int


class _PageCoverageModel:
    """Independent exact-page begin/pull/register/cover/abort oracle."""

    def __init__(self) -> None:
        self.next_page_sequence = 0
        self.page_sequence: int | None = None
        self.current_source_order: int | None = None
        self.pulled = 0
        self.registered = 0
        self.localized = 0
        self._assert_invariants()

    def snapshot(self) -> _PageCoverageSnapshot:
        return _PageCoverageSnapshot(
            next_page_sequence=self.next_page_sequence,
            page_sequence=self.page_sequence,
            current_source_order=self.current_source_order,
            pulled=self.pulled,
            registered=self.registered,
            localized=self.localized,
        )

    def begin(self, page_sequence: int) -> None:
        if self.page_sequence is not None:
            _violation("a page is already live")
        if page_sequence != self.next_page_sequence:
            _violation("page is not the exact next sequence")
        self.page_sequence = page_sequence
        self.current_source_order = None
        self.pulled = 0
        self.registered = 0
        self.localized = 0
        self._assert_invariants()

    def pull(self, source_order: int) -> None:
        if self.page_sequence is None:
            _violation("no page is live")
        if self.current_source_order is not None:
            _violation("cannot pull past the current source record")
        if source_order != self.pulled:
            _violation("source order must be contiguous")
        self.current_source_order = source_order
        self.pulled += 1
        self._assert_invariants()

    def register(self) -> None:
        if self.current_source_order is None:
            _violation("no pulled record is awaiting registration")
        self.registered += 1
        self.current_source_order = None
        self._assert_invariants()

    def localize(self) -> None:
        if self.current_source_order is None:
            _violation("no pulled record is awaiting localization")
        self.localized += 1
        self.current_source_order = None
        self._assert_invariants()

    def cover(self) -> None:
        if self.page_sequence is None:
            _violation("no page is live")
        if self.current_source_order is not None:
            _violation("cannot cover a partially handled source record")
        if self.pulled != self.registered + self.localized:
            _violation("not every pulled record owns coverage")
        self.next_page_sequence += 1
        self._clear_page()
        self._assert_invariants()

    def abort(self) -> None:
        if self.page_sequence is None:
            _violation("no page is live")
        self._clear_page()
        self._assert_invariants()

    def _clear_page(self) -> None:
        self.page_sequence = None
        self.current_source_order = None
        self.pulled = 0
        self.registered = 0
        self.localized = 0

    def _assert_invariants(self) -> None:
        if self.page_sequence is None:
            if any(
                (
                    self.current_source_order is not None,
                    self.pulled,
                    self.registered,
                    self.localized,
                )
            ):
                _violation("closed page retained coordination state")
            return
        handled = self.registered + self.localized
        expected_unhandled = int(self.current_source_order is not None)
        if self.pulled != handled + expected_unhandled:
            _violation("page counters do not conserve pulled records")


class _CloseStrength(enum.IntEnum):
    """Independent monotonic exact-lane close lattice."""

    OPEN = 0
    DRAINING = 1
    CANCELLING = 2


@dataclasses.dataclass(slots=True)
class _LaneLifecycleModel:
    """Small close oracle with one shared strongest result."""

    strength: _CloseStrength = _CloseStrength.OPEN
    active: int = 0
    result: _CloseStrength | None = None

    def request(self, strength: _CloseStrength) -> None:
        if strength < self.strength:
            return
        if self.result is not None and strength > self.result:
            _violation("a settled close result cannot be upgraded")
        self.strength = strength

    def settle(self) -> _CloseStrength | None:
        if self.active:
            return None
        self.result = self.strength
        return self.result


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
        self._retired_scopes: set[
            tuple[ingestion_lease_store.LeaseGrant, uuid.UUID]
        ] = set()
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
            or (grant, feed_id) in self._retired_scopes
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

    def retire_feed(
        self,
        grant: ingestion_lease_store.LeaseGrant,
        feed_id: uuid.UUID,
    ) -> tuple[tuple[int, ...], int | None]:
        """Retire only one exact grant/Feed scope."""
        self._retired_scopes.add((grant, feed_id))
        purged = tuple(
            sequence
            for sequence, record in sorted(self._records.items())
            if record.grant == grant
            and record.feed_id == feed_id
            and record.state
            in {_ModelState.QUEUED, _ModelState.PENDING_BOUNDARY}
        )
        for sequence in purged:
            record = self._records[sequence]
            if record.state is _ModelState.QUEUED:
                queue = self._feed_queues[feed_id]
                queue.remove(sequence)
                if not queue:
                    del self._feed_queues[feed_id]
                    self._remove_ready(feed_id)
            else:
                del self._pending_boundary_by_feed[feed_id]
            self._release(sequence)
        active = self._active_by_feed.get(feed_id)
        if active is not None and self._records[active].grant != grant:
            active = None
        self._assert_invariants()
        return purged, active

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

    def test_production_affinity_is_exact_uuid_modulo(self) -> None:
        scheduler_types = _scheduler_types()

        for feed_id in _FEED_IDS:
            with self.subTest(feed_id=feed_id):
                self.assertEqual(
                    scheduler_types._shard_index(feed_id),
                    feed_id.int % 8,
                )

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

    def test_model_feed_retirement_is_exact_and_active_is_retained(
        self,
    ) -> None:
        model = _ReferenceModel()
        old = _grant(fencing_token=7)
        successor = _grant(fencing_token=8)
        feed_id = _FEED_IDS[0]
        active = model.admit_call(feed_id, old, source_order=0)
        removed = model.admit_call(feed_id, old, source_order=1)
        successor_record = model.admit_call(
            feed_id,
            successor,
            source_order=2,
        )
        self.assertEqual(model.dispatch(0), active)

        purged, retained_active = model.retire_feed(old, feed_id)

        self.assertEqual(purged, (removed,))
        self.assertEqual(retained_active, active)
        self.assertIn(successor_record, model.live_sequences)
        self.assertIsNone(model.admit_call(feed_id, old, source_order=3))
        model.terminalize(typing.cast("int", active))
        self.assertEqual(model.dispatch(0), successor_record)

    def test_model_close_strength_upgrades_before_settlement(self) -> None:
        lifecycle = _LaneLifecycleModel(active=1)
        lifecycle.request(_CloseStrength.DRAINING)
        self.assertIsNone(lifecycle.settle())

        lifecycle.request(_CloseStrength.CANCELLING)
        lifecycle.active = 0

        self.assertEqual(lifecycle.settle(), _CloseStrength.CANCELLING)
        lifecycle.request(_CloseStrength.DRAINING)
        self.assertEqual(lifecycle.result, _CloseStrength.CANCELLING)

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
                    command = generator.randrange(8)
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
                    elif command == 7:
                        model.retire_feed(
                            generator.choice(grants),
                            generator.choice(_FEED_IDS),
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


class TestPageCoverageModel(unittest.TestCase):
    """Seeded begin/admit/coverage/abort contracts for one exact lane."""

    def test_page_model_never_pulls_past_an_unhandled_record(self) -> None:
        model = _PageCoverageModel()
        model.begin(0)
        model.pull(0)

        with self.assertRaisesRegex(
            _ModelViolation,
            "current source record",
        ):
            model.pull(1)

        self.assertEqual(
            model.snapshot(),
            _PageCoverageSnapshot(0, 0, 0, 1, 0, 0),
        )
        model.register()
        model.pull(1)
        model.localize()
        model.cover()
        self.assertEqual(
            model.snapshot(),
            _PageCoverageSnapshot(1, None, None, 0, 0, 0),
        )

    def test_seeded_page_transitions_preserve_o1_state(self) -> None:
        for seed in (0xC0A7, 0xAB07, 0xFACE):
            with self.subTest(seed=seed):
                generator = random.Random(seed)  # noqa: S311
                model = _PageCoverageModel()
                for expected_sequence in range(24):
                    model.begin(expected_sequence)
                    record_count = generator.randrange(0, 9)
                    abort_at = (
                        generator.randrange(record_count + 1)
                        if generator.randrange(4) == 0
                        else None
                    )
                    aborted = False
                    for source_order in range(record_count):
                        model.pull(source_order)
                        if abort_at == source_order:
                            model.abort()
                            aborted = True
                            break
                        if generator.randrange(5) == 0:
                            model.localize()
                        else:
                            model.register()
                        model._assert_invariants()
                    if aborted:
                        model.begin(expected_sequence)
                        model.cover()
                    else:
                        model.cover()
                    self.assertEqual(
                        model.next_page_sequence,
                        expected_sequence + 1,
                    )
                    self.assertIsNone(model.snapshot().page_sequence)


class _ImmediateExecutor:
    def __init__(self) -> None:
        self.sequences: list[int] = []

    async def execute(self, record: object) -> object:
        scheduler_types = _scheduler_types()
        self.sequences.append(record.local_sequence)
        return scheduler_types._ExecutorCompleted()


class _GateExecutor:
    def __init__(self) -> None:
        self.started: list[int] = []
        self.changed = asyncio.Event()
        self._release: dict[int, asyncio.Event] = {}

    async def execute(self, record: object) -> object:
        scheduler_types = _scheduler_types()
        sequence = record.local_sequence
        self._release.setdefault(sequence, asyncio.Event())
        self.started.append(sequence)
        self.changed.set()
        await self._release[sequence].wait()
        return scheduler_types._ExecutorCompleted()

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

    async def execute(self, record: object) -> object:
        del record
        scheduler_types = _scheduler_types()
        self.entered.set()
        try:
            await self.release.wait()
        except asyncio.CancelledError:
            self.cancellation_seen.set()
            if not self.swallow:
                raise
            current = asyncio.current_task()
            if current is None:
                self.fail_no_current_task()
            current.uncancel()
            await self.release.wait()
        return scheduler_types._ExecutorCompleted()

    def fail_no_current_task(self) -> typing.Never:
        self._raise_missing_task()

    def _raise_missing_task(self) -> typing.Never:
        message = "executor is not running in a Task"
        raise RuntimeError(message)


class _FailingExecutor:
    def __init__(self) -> None:
        self.entered = asyncio.Event()

    async def execute(self, record: object) -> object:
        del record
        self.entered.set()
        message = "unexpected executor failure"
        raise RuntimeError(message)


def _call_work(
    feed_id: uuid.UUID,
    grant: ingestion_lease_store.LeaseGrant,
    source_order: int,
    *,
    source_timestamp: datetime.datetime = _SOURCE_TIME,
) -> object:
    scheduler_types = _scheduler_types()
    return scheduler_types._CallWork(
        feed_id=feed_id,
        grant=grant,
        source_order=source_order,
        source_timestamp=source_timestamp,
        payload=object(),
        page_sequence=0,
    )


def _actual_model_projection(snapshot: object) -> _ModelSnapshot:
    return _ModelSnapshot(
        held=snapshot.held,
        queued_calls=snapshot.queued_calls,
        active_calls=snapshot.active_calls,
        pending_boundaries=snapshot.pending_boundaries,
        flushing_boundaries=snapshot.flushing_boundaries,
        pressure_paused=snapshot.pressure_paused,
        ready_feeds=snapshot.ready_feeds,
        active_feeds=snapshot.active_feeds,
        local_sequences=tuple(
            record.local_sequence for record in snapshot.records
        ),
        fatal=snapshot.fatal,
    )


class TestShardModel(unittest.IsolatedAsyncioTestCase):
    """Compares every real locked transition with the independent oracle."""

    async def test_seeded_shard_snapshot_matches_model_transitions(  # noqa: PLR0912, PLR0915
        self,
    ) -> None:
        scheduler_types = _scheduler_types()
        shard_module = _scheduler_shard()
        limits = scheduler_types._SchedulerLimits(
            shard_count=1,
            capacity=5,
            workers_per_shard=2,
            high_water=4,
            resume_at=2,
        )
        grants = (
            _grant(fencing_token=1),
            _grant(fencing_token=2),
            _grant(lease_key="151", fencing_token=1),
        )
        for seed in (0xB0A7, 0xAFF1):
            with self.subTest(seed=seed):
                generator = random.Random(seed)  # noqa: S311
                executor = _ImmediateExecutor()
                shard = shard_module._Shard(0, executor, limits=limits)
                model = _ReferenceModel()
                records: dict[int, object] = {}
                source_order = 0
                for step in range(80):
                    command = generator.randrange(5)
                    if command == 0:
                        feed_id = generator.choice(_FEED_IDS)
                        grant = generator.choice(grants)
                        expected = model.admit_call(
                            feed_id,
                            grant,
                            source_order=source_order,
                        )
                        if expected is not None:
                            record = await shard.admit(
                                _call_work(
                                    feed_id,
                                    grant,
                                    source_order,
                                )
                            )
                            self.assertEqual(record.local_sequence, expected)
                            records[expected] = record
                        source_order += 1
                    elif command == 1:
                        free_slots = tuple(
                            slot
                            for slot in range(model.limits.workers)
                            if slot not in model._active_by_slot
                        )
                        if free_slots:
                            slot = generator.choice(free_slots)
                            expected = model.dispatch(slot)
                            actual = await shard._take_next(
                                slot,
                                wait=False,
                            )
                            if expected is None:
                                self.assertIsNone(actual)
                            else:
                                self.assertIsNotNone(actual)
                                self.assertEqual(
                                    actual.local_sequence,
                                    expected,
                                )
                    elif command == 2 and model.active_sequences:
                        sequence = generator.choice(model.active_sequences)
                        slot = model._records[sequence].worker_slot
                        self.assertIsNotNone(slot)
                        model.terminalize(sequence)
                        await shard._terminalize(
                            typing.cast("int", slot),
                            records.pop(sequence),
                            scheduler_types._ExecutorCompleted(),
                        )
                    elif command == 3:
                        grant = generator.choice(grants)
                        expected = model.purge_exact(grant)
                        actual = await shard.purge_exact(grant)
                        self.assertEqual(actual.released_sequences, expected)
                        for sequence in expected:
                            records.pop(sequence)
                    elif command == 4:
                        grant = generator.choice(grants)
                        feed_id = generator.choice(_FEED_IDS)
                        expected_released, expected_active = model.retire_feed(
                            grant, feed_id
                        )
                        actual = await shard.retire_feed(grant, feed_id)
                        self.assertEqual(
                            actual.released_sequences,
                            expected_released,
                        )
                        self.assertEqual(
                            actual.active_sequence,
                            expected_active,
                        )
                        for sequence in expected_released:
                            records.pop(sequence)

                    actual_snapshot = await shard.snapshot()
                    expected_snapshot = model.snapshot()
                    self.assertEqual(
                        _actual_model_projection(actual_snapshot),
                        expected_snapshot,
                        f"seed={seed} step={step} command={command}",
                    )

    async def test_exact_purge_keeps_active_successor_and_sibling(self) -> None:
        scheduler_types = _scheduler_types()
        shard_module = _scheduler_shard()
        limits = scheduler_types._SchedulerLimits(
            shard_count=1,
            capacity=6,
            workers_per_shard=1,
            high_water=6,
            resume_at=3,
        )
        shard = shard_module._Shard(
            0,
            _ImmediateExecutor(),
            limits=limits,
        )
        old = _grant(fencing_token=7)
        successor = _grant(fencing_token=8)
        sibling = _grant(lease_key="151", fencing_token=7)
        old_record = await shard.admit(_call_work(_FEED_IDS[0], old, 0))
        successor_record = await shard.admit(
            _call_work(_FEED_IDS[0], successor, 1)
        )
        sibling_record = await shard.admit(_call_work(_FEED_IDS[1], sibling, 2))
        active_old = await shard._take_next(0, wait=False)
        self.assertIs(active_old, old_record)

        result = await shard.purge_exact(old)

        self.assertEqual(result.released_sequences, ())
        self.assertEqual(result.active_sequences, (0,))
        snapshot = await shard.snapshot()
        self.assertEqual(snapshot.held, 3)
        self.assertEqual(
            tuple(record.local_sequence for record in snapshot.records),
            (0, 1, 2),
        )
        await shard._terminalize(
            0,
            old_record,
            scheduler_types._ExecutorCompleted(),
        )
        snapshot = await shard.snapshot()
        self.assertEqual(snapshot.held, 2)
        self.assertEqual(
            tuple(record.local_sequence for record in snapshot.records),
            (
                successor_record.local_sequence,
                sibling_record.local_sequence,
            ),
        )

    async def test_exact_hysteresis_waits_without_registration(self) -> None:
        scheduler_types = _scheduler_types()
        shard_module = _scheduler_shard()
        limits = scheduler_types._SchedulerLimits(
            shard_count=8,
            capacity=500,
            workers_per_shard=4,
            high_water=400,
            resume_at=299,
        )
        shard = shard_module._Shard(
            0,
            _ImmediateExecutor(),
            limits=limits,
        )
        first_hundred = _grant(lease_key="100")
        threshold_record = _grant(lease_key="101")
        retained = _grant(lease_key="102")
        for source_order in range(400):
            if source_order < 100:
                grant = first_hundred
            elif source_order == 100:
                grant = threshold_record
            else:
                grant = retained
            await shard.admit(
                _call_work(
                    uuid.UUID(int=source_order + 1),
                    grant,
                    source_order,
                )
            )

        snapshot = await shard.snapshot()
        self.assertEqual(snapshot.held, 400)
        self.assertTrue(snapshot.pressure_paused)
        blocked = asyncio.create_task(
            shard.admit(_call_work(uuid.UUID(int=401), retained, 400))
        )
        try:
            await shard.wait_for_capacity_waiters(1)
            snapshot = await shard.snapshot()
            self.assertEqual(snapshot.held, 400)
            self.assertNotIn(
                400,
                tuple(record.local_sequence for record in snapshot.records),
            )

            released = await shard.purge_exact(first_hundred)
            self.assertEqual(len(released.released_sequences), 100)
            self.assertEqual((await shard.snapshot()).held, 300)
            self.assertFalse(blocked.done())

            released = await shard.purge_exact(threshold_record)
            self.assertEqual(len(released.released_sequences), 1)
            admitted = await asyncio.wait_for(blocked, timeout=1)
            self.assertEqual(admitted.local_sequence, 400)
            snapshot = await shard.snapshot()
            self.assertEqual(snapshot.held, 300)
            self.assertFalse(snapshot.pressure_paused)
        finally:
            if not blocked.done():
                blocked.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await blocked


class TestShardWorkers(unittest.IsolatedAsyncioTestCase):
    """Event-gated fixed-worker and integrity transition tests."""

    def _limits(self, *, workers: int = 2) -> object:
        scheduler_types = _scheduler_types()
        return scheduler_types._SchedulerLimits(
            shard_count=1,
            capacity=10,
            workers_per_shard=workers,
            high_water=10,
            resume_at=5,
        )

    async def test_fixed_workers_preserve_fifo_identity_and_fair_turns(
        self,
    ) -> None:
        shard_module = _scheduler_shard()
        executor = _GateExecutor()
        shard = shard_module._Shard(0, executor, limits=self._limits())
        grant = _grant()
        await shard.start()
        try:
            hot = _FEED_IDS[0]
            cold = _FEED_IDS[1]
            works = (
                _call_work(hot, grant, 0),
                _call_work(hot, grant, 1),
                _call_work(cold, grant, 2),
                _call_work(
                    hot,
                    grant,
                    3,
                    source_timestamp=(
                        _SOURCE_TIME - datetime.timedelta(seconds=1)
                    ),
                ),
            )
            records = tuple([await shard.admit(work) for work in works])
            await executor.wait_for_started(2)

            self.assertEqual(set(executor.started[:2]), {0, 2})
            snapshot = await shard.snapshot()
            self.assertEqual(len(snapshot.workers), 2)
            self.assertTrue(
                all(worker.task_registered for worker in snapshot.workers)
            )
            self.assertEqual(snapshot.active_calls, 2)
            self.assertEqual(len(snapshot.active_feeds), 2)

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
            self.assertEqual((await shard.snapshot()).held, 0)
        finally:
            await shard.close()

    async def test_production_start_registers_exactly_four_workers(
        self,
    ) -> None:
        shard_module = _scheduler_shard()
        shard = shard_module._Shard(0, _ImmediateExecutor())

        await shard.start()
        try:
            snapshot = await shard.snapshot()
            self.assertEqual(len(snapshot.workers), 4)
            self.assertTrue(
                all(worker.task_registered for worker in snapshot.workers)
            )
            self.assertTrue(
                all(not worker.task_done for worker in snapshot.workers)
            )
        finally:
            await shard.close()

    async def test_expected_cancellation_settles_before_slot_reuse(
        self,
    ) -> None:
        shard_module = _scheduler_shard()
        executor = _CancellationExecutor(swallow=False)
        shard = shard_module._Shard(
            0,
            executor,
            limits=self._limits(workers=1),
        )
        grant = _grant()
        await shard.start()
        try:
            await shard.admit(_call_work(_FEED_IDS[0], grant, 0))
            await asyncio.wait_for(executor.entered.wait(), timeout=1)
            cancelled = await shard.cancel_active_exact(grant)

            self.assertEqual(cancelled, (0,))
            self.assertTrue(executor.cancellation_seen.is_set())
            snapshot = await shard.snapshot()
            self.assertEqual(snapshot.held, 0)
            self.assertEqual(len(snapshot.workers), 1)
            self.assertTrue(snapshot.workers[0].task_registered)
            self.assertFalse(snapshot.workers[0].task_done)
            self.assertIsNone(snapshot.workers[0].active_sequence)
            self.assertFalse(snapshot.fatal)
        finally:
            await shard.close()

    async def test_swallowed_cancellation_fails_closed_and_retains_token(
        self,
    ) -> None:
        shard_module = _scheduler_shard()
        executor = _CancellationExecutor(swallow=True)
        limits = _scheduler_types()._SchedulerLimits(
            shard_count=1,
            capacity=2,
            workers_per_shard=1,
            high_water=1,
            resume_at=0,
        )
        shard = shard_module._Shard(0, executor, limits=limits)
        grant = _grant()
        await shard.start()
        await shard.admit(_call_work(_FEED_IDS[0], grant, 0))
        await asyncio.wait_for(executor.entered.wait(), timeout=1)
        blocked = asyncio.create_task(
            shard.admit(_call_work(_FEED_IDS[1], grant, 1))
        )
        await shard.wait_for_capacity_waiters(1)
        requests = await shard.request_cancel_exact(grant)
        self.assertEqual(len(requests), 1)
        await asyncio.wait_for(executor.cancellation_seen.wait(), timeout=1)
        failure = RuntimeError("worker swallowed cancellation")
        await shard.abandon_cancellation(requests[0].slot_id, failure)

        with self.assertRaises(shard_module._ShardFatalError):
            await blocked
        snapshot = await shard.snapshot()
        self.assertTrue(snapshot.fatal)
        self.assertFalse(snapshot.admission_open)
        self.assertEqual(snapshot.held, 1)
        self.assertEqual(snapshot.active_calls, 1)
        self.assertEqual(snapshot.workers[0].cancellation_sequence, 0)
        executor.release.set()
        await asyncio.wait_for(requests[0].task, timeout=1)
        snapshot = await shard.snapshot()
        self.assertEqual(snapshot.held, 1)
        self.assertTrue(snapshot.workers[0].task_done)

    async def test_unclassified_cancellation_wakes_waiters_and_fails_closed(
        self,
    ) -> None:
        shard_module = _scheduler_shard()
        executor = _CancellationExecutor(swallow=False)
        limits = _scheduler_types()._SchedulerLimits(
            shard_count=1,
            capacity=2,
            workers_per_shard=1,
            high_water=1,
            resume_at=0,
        )
        shard = shard_module._Shard(0, executor, limits=limits)
        grant = _grant()
        await shard.start()
        await shard.admit(_call_work(_FEED_IDS[0], grant, 0))
        await asyncio.wait_for(executor.entered.wait(), timeout=1)
        blocked = asyncio.create_task(
            shard.admit(_call_work(_FEED_IDS[1], grant, 1))
        )
        await shard.wait_for_capacity_waiters(1)
        worker_task = shard._workers[0].task
        self.assertIsNotNone(worker_task)
        typing.cast("asyncio.Task[None]", worker_task).cancel()
        await shard.wait_for_fatal()

        with self.assertRaises(shard_module._ShardFatalError):
            await blocked
        with self.assertRaises(asyncio.CancelledError):
            await typing.cast("asyncio.Task[None]", worker_task)
        snapshot = await shard.snapshot()
        self.assertTrue(snapshot.fatal)
        self.assertFalse(snapshot.admission_open)
        self.assertEqual(snapshot.held, 1)
        self.assertEqual(snapshot.active_calls, 1)
        self.assertTrue(snapshot.workers[0].task_done)

    async def test_unexpected_worker_failure_is_persistent_and_unreplaced(
        self,
    ) -> None:
        shard_module = _scheduler_shard()
        executor = _FailingExecutor()
        shard = shard_module._Shard(
            0,
            executor,
            limits=self._limits(workers=1),
        )
        await shard.start()
        await shard.admit(_call_work(_FEED_IDS[0], _grant(), 0))
        await asyncio.wait_for(executor.entered.wait(), timeout=1)
        await shard.wait_for_fatal()

        snapshot = await shard.snapshot()
        self.assertTrue(snapshot.fatal)
        self.assertFalse(snapshot.admission_open)
        self.assertEqual(snapshot.held, 1)
        self.assertEqual(snapshot.active_calls, 1)
        self.assertTrue(snapshot.workers[0].task_done)
        with self.assertRaises(shard_module._ShardFatalError):
            await shard.admit(_call_work(_FEED_IDS[1], _grant(), 1))


if __name__ == "__main__":
    unittest.main()
