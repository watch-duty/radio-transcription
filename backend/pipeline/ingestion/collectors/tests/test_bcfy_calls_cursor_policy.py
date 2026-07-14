"""Tests for the pure Broadcastify Calls cursor policy."""

from __future__ import annotations

import dataclasses
import datetime
import types
import typing
import unittest
import uuid

from backend.pipeline.ingestion.collectors.bcfy_calls import cursor_policy
from backend.pipeline.storage import feed_store, ingestion_lease_store

if typing.TYPE_CHECKING:
    import collections.abc

_NOW = datetime.datetime(2026, 7, 12, 12, 0, tzinfo=datetime.UTC)
_REPLAY_FLOOR = _NOW - datetime.timedelta(minutes=5)
_START_POS = _NOW - datetime.timedelta(minutes=2)
_NEXT_POS = _START_POS + datetime.timedelta(seconds=10)


def _grant(
    *,
    lease_key: str = "123",
    fencing_token: int = 1,
) -> ingestion_lease_store.LeaseGrant:
    """Build one deterministic complete Lease grant."""
    return ingestion_lease_store.LeaseGrant(
        source_type=feed_store.SourceType.BCFY_CALLS,
        lease_key=lease_key,
        owner_worker_id=uuid.UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
        fencing_token=fencing_token,
    )


def _cursor_state(
    cursor: cursor_policy.LeaseCursor,
) -> tuple[
    ingestion_lease_store.LeaseGrant,
    datetime.datetime | None,
    int,
    cursor_policy.PageCandidate | None,
]:
    """Capture all four observable Lease Cursor fields for rejections."""
    return (
        cursor.grant,
        cursor.pos,
        cursor.next_page_sequence,
        cursor.outstanding_candidate,
    )


def _assign_attribute(
    target: object,
    field_name: str,
    replacement: object,
) -> None:
    """Attempt assignment through the object's normal frozen guard."""
    setattr(target, field_name, replacement)


class TestBootstrapCursor(unittest.TestCase):
    """Tests for durable Feed cursor bootstrap decisions."""

    def test_bootstrap_decision_is_frozen_slotted_policy_evidence(
        self,
    ) -> None:
        decision = cursor_policy.bootstrap_cursor([], now=_NOW)

        self.assertEqual(
            tuple(field.name for field in dataclasses.fields(decision)),
            (
                "pos",
                "durable_minimum",
                "replay_floor",
                "clamped",
                "floor_reached",
            ),
        )
        self.assertFalse(hasattr(decision, "__dict__"))
        with self.assertRaises(dataclasses.FrozenInstanceError):
            _assign_attribute(decision, "clamped", replacement=True)

    def test_bootstrap_empty_or_all_null_omits_pos(self) -> None:
        for cursors in ((), (None, None)):
            with self.subTest(cursors=cursors):
                decision = cursor_policy.bootstrap_cursor(cursors, now=_NOW)

                self.assertIsNone(decision.pos)
                self.assertIsNone(decision.durable_minimum)
                self.assertEqual(decision.replay_floor, _REPLAY_FLOOR)
                self.assertFalse(decision.clamped)
                self.assertIsNone(decision.floor_reached)

    def test_bootstrap_generator_uses_minimum_non_null_once(self) -> None:
        newer = _NOW - datetime.timedelta(minutes=1)
        oldest = _NOW - datetime.timedelta(minutes=3)
        observed: list[datetime.datetime | None] = []

        def cursors() -> collections.abc.Iterator[datetime.datetime | None]:
            for cursor in (None, newer, oldest, None):
                observed.append(cursor)
                yield cursor

        cursor_generator = cursors()
        decision = cursor_policy.bootstrap_cursor(
            cursor_generator,
            now=_NOW,
        )

        self.assertEqual(observed, [None, newer, oldest, None])
        self.assertEqual(decision.pos, oldest)
        self.assertEqual(decision.durable_minimum, oldest)
        self.assertEqual(decision.replay_floor, _REPLAY_FLOOR)
        self.assertFalse(decision.clamped)
        self.assertIsNone(decision.floor_reached)
        self.assertEqual(list(cursor_generator), [])

    def test_bootstrap_clamps_minimum_older_than_replay_floor(self) -> None:
        oldest = _NOW - datetime.timedelta(minutes=20)

        decision = cursor_policy.bootstrap_cursor(
            (oldest, _NOW - datetime.timedelta(minutes=1)),
            now=_NOW,
        )

        self.assertEqual(decision.pos, _REPLAY_FLOOR)
        self.assertEqual(decision.durable_minimum, oldest)
        self.assertEqual(decision.replay_floor, _REPLAY_FLOOR)
        self.assertTrue(decision.clamped)
        assert decision.floor_reached is not None
        self.assertEqual(decision.floor_reached.requested_start, oldest)
        self.assertEqual(decision.floor_reached.floor_start, _REPLAY_FLOOR)

    def test_bootstrap_exact_floor_and_newer_minimum_are_not_clamped(
        self,
    ) -> None:
        cases = (
            _REPLAY_FLOOR,
            _NOW - datetime.timedelta(microseconds=1),
        )

        for durable_minimum in cases:
            with self.subTest(durable_minimum=durable_minimum):
                decision = cursor_policy.bootstrap_cursor(
                    (durable_minimum,),
                    now=_NOW,
                )

                self.assertEqual(decision.pos, durable_minimum)
                self.assertEqual(decision.durable_minimum, durable_minimum)
                self.assertFalse(decision.clamped)
                if durable_minimum == _REPLAY_FLOOR:
                    assert decision.floor_reached is not None
                    self.assertEqual(
                        decision.floor_reached.lost_duration_seconds,
                        0.0,
                    )
                else:
                    self.assertIsNone(decision.floor_reached)

    def test_bootstrap_preserves_inclusive_cursor_exactly(self) -> None:
        durable_minimum = datetime.datetime(
            2026,
            7,
            12,
            11,
            59,
            59,
            123456,
            tzinfo=datetime.UTC,
        )

        decision = cursor_policy.bootstrap_cursor(
            (durable_minimum,),
            now=_NOW,
        )

        self.assertEqual(decision.pos, durable_minimum)

    def test_bootstrap_rejects_invalid_utc_now(self) -> None:
        cases = (
            datetime.datetime(2026, 7, 12, 12, 0),
            datetime.datetime(
                2026,
                7,
                12,
                12,
                0,
                tzinfo=datetime.timezone(datetime.timedelta(hours=1)),
            ),
        )

        for now in cases:
            with self.subTest(now=now):
                with self.assertRaisesRegex(
                    ValueError, "now must be UTC-aware"
                ):
                    cursor_policy.bootstrap_cursor((), now=now)

    def test_bootstrap_rejects_invalid_utc_feed_cursor(self) -> None:
        cases = (
            datetime.datetime(2026, 7, 12, 11, 59),
            datetime.datetime(
                2026,
                7,
                12,
                11,
                59,
                tzinfo=datetime.timezone(datetime.timedelta(hours=-7)),
            ),
        )

        for cursor in cases:
            with self.subTest(cursor=cursor):
                with self.assertRaisesRegex(
                    ValueError,
                    "Feed cursor must be UTC-aware",
                ):
                    cursor_policy.bootstrap_cursor((cursor,), now=_NOW)


class TestReplayFloor(unittest.TestCase):
    """Tests for the one closed four-cause replay-floor decision."""

    def test_cause_vocabulary_is_exact(self) -> None:
        self.assertEqual(
            tuple(cursor_policy.ReplayFloorCause),
            (
                cursor_policy.ReplayFloorCause.BOOTSTRAP,
                cursor_policy.ReplayFloorCause.RECOVERY,
                cursor_policy.ReplayFloorCause.REPLAY_OVERRIDE,
                cursor_policy.ReplayFloorCause.OVERLOAD,
            ),
        )

    def test_every_cause_reports_exact_positive_lost_span(self) -> None:
        requested = _NOW - datetime.timedelta(minutes=20)

        for cause in cursor_policy.ReplayFloorCause:
            with self.subTest(cause=cause):
                decision = cursor_policy.apply_replay_floor(
                    requested,
                    now=_NOW,
                    cause=cause,
                )

                self.assertEqual(decision.selected_start, _REPLAY_FLOOR)
                self.assertEqual(decision.floor_start, _REPLAY_FLOOR)
                evidence = decision.floor_reached
                assert evidence is not None
                self.assertEqual(evidence.cause, cause)
                self.assertEqual(evidence.requested_start, requested)
                self.assertEqual(evidence.floor_start, _REPLAY_FLOOR)
                self.assertEqual(evidence.lost_span_start, requested)
                self.assertEqual(evidence.lost_span_end, _REPLAY_FLOOR)
                self.assertEqual(evidence.lost_duration_seconds, 900.0)

    def test_floor_equality_has_zero_duration_evidence_for_every_cause(
        self,
    ) -> None:
        for cause in cursor_policy.ReplayFloorCause:
            with self.subTest(cause=cause):
                decision = cursor_policy.apply_replay_floor(
                    _REPLAY_FLOOR,
                    now=_NOW,
                    cause=cause,
                )

                self.assertEqual(decision.selected_start, _REPLAY_FLOOR)
                evidence = decision.floor_reached
                assert evidence is not None
                self.assertEqual(evidence.cause, cause)
                self.assertEqual(evidence.lost_span_start, _REPLAY_FLOOR)
                self.assertEqual(evidence.lost_span_end, _REPLAY_FLOOR)
                self.assertEqual(evidence.lost_duration_seconds, 0.0)

    def test_above_floor_and_omitted_start_suppress_evidence(self) -> None:
        above = _REPLAY_FLOOR + datetime.timedelta(microseconds=1)

        for cause in cursor_policy.ReplayFloorCause:
            with self.subTest(cause=cause, case="above"):
                decision = cursor_policy.apply_replay_floor(
                    above,
                    now=_NOW,
                    cause=cause,
                )
                self.assertEqual(decision.selected_start, above)
                self.assertIsNone(decision.floor_reached)
            with self.subTest(cause=cause, case="omitted"):
                decision = cursor_policy.apply_replay_floor(
                    None,
                    now=_NOW,
                    cause=cause,
                )
                self.assertIsNone(decision.selected_start)
                self.assertIsNone(decision.floor_reached)

    def test_decision_and_evidence_are_frozen_slotted_values(self) -> None:
        decision = cursor_policy.apply_replay_floor(
            _REPLAY_FLOOR,
            now=_NOW,
            cause=cursor_policy.ReplayFloorCause.OVERLOAD,
        )
        assert decision.floor_reached is not None

        self.assertFalse(hasattr(decision, "__dict__"))
        self.assertFalse(hasattr(decision.floor_reached, "__dict__"))
        with self.assertRaises(dataclasses.FrozenInstanceError):
            _assign_attribute(decision, "selected_start", _NOW)
        with self.assertRaises(dataclasses.FrozenInstanceError):
            _assign_attribute(
                decision.floor_reached,
                "lost_duration_seconds",
                1.0,
            )

    def test_replay_floor_validates_utc_now_and_requested_start(self) -> None:
        naive = datetime.datetime(2026, 7, 12, 12, 0)
        cause = cursor_policy.ReplayFloorCause.RECOVERY

        with self.assertRaisesRegex(ValueError, "now must be UTC-aware"):
            cursor_policy.apply_replay_floor(None, now=naive, cause=cause)
        with self.assertRaisesRegex(
            ValueError,
            "requested_start must be UTC-aware",
        ):
            cursor_policy.apply_replay_floor(naive, now=_NOW, cause=cause)
        with self.assertRaisesRegex(TypeError, "ReplayFloorCause"):
            cursor_policy.apply_replay_floor(  # type: ignore[arg-type]
                _NOW,
                now=_NOW,
                cause="recovery",
            )


class TestPageCursorCapability(unittest.TestCase):
    """Tests for the sealed page-candidate-to-receipt capability."""

    def test_candidate_and_receipt_are_frozen_slotted_o1_values(self) -> None:
        cursor = cursor_policy.LeaseCursor(_grant(), pos=_START_POS)
        candidate = cursor.prepare(_NEXT_POS)
        receipt = cursor_policy._issue_covered_page(candidate)

        self.assertEqual(
            tuple(field.name for field in dataclasses.fields(candidate)),
            ("grant", "page_sequence", "last_pos", "_seal"),
        )
        self.assertEqual(
            tuple(field.name for field in dataclasses.fields(receipt)),
            ("grant", "page_sequence", "last_pos", "_seal"),
        )
        self.assertFalse(hasattr(candidate, "__dict__"))
        self.assertFalse(hasattr(receipt, "__dict__"))
        with self.assertRaises(dataclasses.FrozenInstanceError):
            _assign_attribute(candidate, "page_sequence", 1)
        with self.assertRaises(dataclasses.FrozenInstanceError):
            _assign_attribute(receipt, "page_sequence", 1)

    def test_no_progress_candidate_and_settlement_have_no_datetime(
        self,
    ) -> None:
        cursor = cursor_policy.LeaseCursor(_grant(), pos=_START_POS)
        candidate = cursor.prepare_no_progress()
        settlement = cursor_policy._issue_no_progress_page(candidate)

        self.assertEqual(
            tuple(field.name for field in dataclasses.fields(candidate)),
            ("grant", "page_sequence", "_seal"),
        )
        self.assertEqual(
            tuple(field.name for field in dataclasses.fields(settlement)),
            ("grant", "page_sequence", "_seal"),
        )
        self.assertFalse(hasattr(candidate, "last_pos"))
        self.assertFalse(hasattr(settlement, "last_pos"))
        self.assertFalse(hasattr(candidate, "__dict__"))
        self.assertFalse(hasattr(settlement, "__dict__"))

    def test_replayable_settlement_is_frozen_slotted_o1_value(self) -> None:
        cursor = cursor_policy.LeaseCursor(_grant(), pos=_START_POS)
        candidate = cursor.prepare(_NEXT_POS)
        settlement = cursor_policy._issue_replayable_page(candidate)

        self.assertEqual(
            tuple(field.name for field in dataclasses.fields(settlement)),
            ("grant", "page_sequence", "last_pos", "_seal"),
        )
        self.assertFalse(hasattr(settlement, "__dict__"))
        with self.assertRaises(dataclasses.FrozenInstanceError):
            _assign_attribute(settlement, "page_sequence", 1)

    def test_private_receipt_surface_is_not_publicly_exported(self) -> None:
        self.assertIn("PageCursorCandidate", cursor_policy.__all__)
        self.assertIn("NoProgressPageCandidate", cursor_policy.__all__)
        self.assertIn("PageCandidate", cursor_policy.__all__)
        self.assertIn("LeaseCursor", cursor_policy.__all__)
        self.assertNotIn("_CoveredPage", cursor_policy.__all__)
        self.assertNotIn("_NoProgressPageSettled", cursor_policy.__all__)
        self.assertNotIn("_ReplayablePageSettled", cursor_policy.__all__)
        self.assertNotIn("_issue_covered_page", cursor_policy.__all__)
        self.assertNotIn("_issue_no_progress_page", cursor_policy.__all__)
        self.assertNotIn("_issue_replayable_page", cursor_policy.__all__)

    def test_direct_candidate_and_receipt_construction_is_rejected(
        self,
    ) -> None:
        grant = _grant()
        with self.assertRaises(cursor_policy.CursorIntegrityError):
            cursor_policy.PageCursorCandidate(
                grant=grant,
                page_sequence=0,
                last_pos=_NEXT_POS,
                _seal=cursor_policy._CandidateSeal(),
            )
        with self.assertRaises(cursor_policy.CursorIntegrityError):
            cursor_policy._CoveredPage(
                grant=grant,
                page_sequence=0,
                last_pos=_NEXT_POS,
                _seal=cursor_policy._CandidateSeal(),
            )
        with self.assertRaises(cursor_policy.CursorIntegrityError):
            cursor_policy.NoProgressPageCandidate(
                grant=grant,
                page_sequence=0,
                _seal=cursor_policy._CandidateSeal(),
            )
        with self.assertRaises(cursor_policy.CursorIntegrityError):
            cursor_policy._NoProgressPageSettled(
                grant=grant,
                page_sequence=0,
                _seal=cursor_policy._CandidateSeal(),
            )
        with self.assertRaises(cursor_policy.CursorIntegrityError):
            cursor_policy._ReplayablePageSettled(
                grant=grant,
                page_sequence=0,
                last_pos=_NEXT_POS,
                _seal=cursor_policy._CandidateSeal(),
            )

    def test_private_issuer_rejects_structural_candidate(self) -> None:
        fake = types.SimpleNamespace(
            grant=_grant(),
            page_sequence=0,
            last_pos=_NEXT_POS,
        )

        with self.assertRaises(cursor_policy.CursorIntegrityError):
            cursor_policy._issue_covered_page(
                typing.cast("cursor_policy.PageCursorCandidate", fake)
            )
        with self.assertRaises(cursor_policy.CursorIntegrityError):
            cursor_policy._issue_no_progress_page(
                typing.cast("cursor_policy.NoProgressPageCandidate", fake)
            )
        with self.assertRaises(cursor_policy.CursorIntegrityError):
            cursor_policy._issue_replayable_page(
                typing.cast("cursor_policy.PageCursorCandidate", fake)
            )


class TestLeaseCursor(unittest.TestCase):
    """Tests for exact-next, grant-local Lease Cursor consumption."""

    def setUp(self) -> None:
        self.grant = _grant()
        self.cursor = cursor_policy.LeaseCursor(
            self.grant,
            pos=_START_POS,
        )

    def test_prepare_binds_exact_grant_and_does_not_advance(self) -> None:
        candidate = self.cursor.prepare(_NEXT_POS)

        self.assertEqual(candidate.grant, self.grant)
        self.assertEqual(candidate.page_sequence, 0)
        self.assertEqual(candidate.last_pos, _NEXT_POS)
        self.assertEqual(self.cursor.pos, _START_POS)
        self.assertEqual(self.cursor.next_page_sequence, 0)
        self.assertIs(self.cursor.outstanding_candidate, candidate)

    def test_prepare_rejects_regression_and_invalid_utc_without_mutation(
        self,
    ) -> None:
        cases = (
            _START_POS - datetime.timedelta(microseconds=1),
            datetime.datetime(2026, 7, 12, 11, 59),
            datetime.datetime(
                2026,
                7,
                12,
                11,
                59,
                tzinfo=datetime.timezone(datetime.timedelta(hours=1)),
            ),
        )

        for last_pos in cases:
            with self.subTest(last_pos=last_pos):
                before = _cursor_state(self.cursor)
                with self.assertRaises(cursor_policy.CursorIntegrityError):
                    self.cursor.prepare(last_pos)
                self.assertEqual(_cursor_state(self.cursor), before)

    def test_prepare_allows_only_one_outstanding_candidate(self) -> None:
        candidate = self.cursor.prepare(_NEXT_POS)
        before = _cursor_state(self.cursor)

        with self.assertRaises(cursor_policy.CursorIntegrityError):
            self.cursor.prepare(_NEXT_POS + datetime.timedelta(seconds=1))

        self.assertEqual(_cursor_state(self.cursor), before)
        self.assertIs(self.cursor.outstanding_candidate, candidate)

    def test_accept_advances_exact_candidate_once(self) -> None:
        candidate = self.cursor.prepare(_NEXT_POS)
        receipt = cursor_policy._issue_covered_page(candidate)

        accepted_pos = self.cursor.accept(receipt)

        self.assertEqual(accepted_pos, _NEXT_POS)
        self.assertEqual(self.cursor.pos, _NEXT_POS)
        self.assertEqual(self.cursor.next_page_sequence, 1)
        self.assertIsNone(self.cursor.outstanding_candidate)

        before_duplicate = _cursor_state(self.cursor)
        with self.assertRaises(cursor_policy.CursorIntegrityError):
            self.cursor.accept(receipt)
        self.assertEqual(_cursor_state(self.cursor), before_duplicate)

    def test_equal_last_pos_is_consumed_once_without_regression(self) -> None:
        candidate = self.cursor.prepare(_START_POS)
        receipt = cursor_policy._issue_covered_page(candidate)

        self.cursor.accept(receipt)

        self.assertEqual(self.cursor.pos, _START_POS)
        self.assertEqual(self.cursor.next_page_sequence, 1)
        self.assertIsNone(self.cursor.outstanding_candidate)

    def test_no_progress_consumes_sequence_without_moving_position(
        self,
    ) -> None:
        candidate = self.cursor.prepare_no_progress()
        settlement = cursor_policy._issue_no_progress_page(candidate)
        original_pos = self.cursor.pos

        result = self.cursor.accept_no_progress(settlement)

        self.assertIsNone(result)
        self.assertIs(self.cursor.pos, original_pos)
        self.assertEqual(self.cursor.next_page_sequence, 1)
        self.assertIsNone(self.cursor.outstanding_candidate)

        before_duplicate = _cursor_state(self.cursor)
        with self.assertRaises(cursor_policy.CursorIntegrityError):
            self.cursor.accept_no_progress(settlement)
        self.assertEqual(_cursor_state(self.cursor), before_duplicate)

    def test_replayable_consumes_sequence_without_moving_position(self) -> None:
        candidate = self.cursor.prepare(_NEXT_POS)
        settlement = cursor_policy._issue_replayable_page(candidate)
        old_pos = self.cursor.pos

        result = self.cursor.accept_replayable(settlement)

        self.assertIsNone(result)
        self.assertEqual(self.cursor.pos, old_pos)
        self.assertIs(self.cursor.pos, old_pos)
        self.assertEqual(self.cursor.next_page_sequence, 1)
        self.assertIsNone(self.cursor.outstanding_candidate)

        next_request_pos = self.cursor.pos
        next_candidate = self.cursor.prepare(
            _NEXT_POS + datetime.timedelta(seconds=10)
        )
        self.assertIs(next_request_pos, old_pos)
        self.assertIs(self.cursor.pos, old_pos)
        self.assertEqual(next_candidate.page_sequence, 1)

    def test_replayable_rejects_forgery_and_changed_fields_non_mutating(
        self,
    ) -> None:
        candidate = self.cursor.prepare(_NEXT_POS)
        before = _cursor_state(self.cursor)

        with self.assertRaises(cursor_policy.CursorIntegrityError):
            cursor_policy._ReplayablePageSettled(
                grant=candidate.grant,
                page_sequence=candidate.page_sequence,
                last_pos=candidate.last_pos,
                _seal=cursor_policy._CandidateSeal(),
            )
        self.assertEqual(_cursor_state(self.cursor), before)

        copied_fields_wrong_seal = cursor_policy._ReplayablePageSettled(
            grant=candidate.grant,
            page_sequence=candidate.page_sequence,
            last_pos=candidate.last_pos,
            _seal=cursor_policy._CandidateSeal(),
            _construction_key=cursor_policy._CONSTRUCTION_KEY,
        )
        changed_last_pos = cursor_policy._ReplayablePageSettled(
            grant=candidate.grant,
            page_sequence=candidate.page_sequence,
            last_pos=candidate.last_pos + datetime.timedelta(microseconds=1),
            _seal=cursor_policy._read_seal(candidate),
            _construction_key=cursor_policy._CONSTRUCTION_KEY,
        )

        for settlement in (copied_fields_wrong_seal, changed_last_pos):
            with self.subTest(settlement=settlement):
                with self.assertRaises(cursor_policy.CursorIntegrityError):
                    self.cursor.accept_replayable(settlement)
                self.assertEqual(_cursor_state(self.cursor), before)

    def test_replayable_rejects_crossed_grant_cursor_and_sequence(
        self,
    ) -> None:
        candidate = self.cursor.prepare(_NEXT_POS)
        before = _cursor_state(self.cursor)

        other_grant_cursor = cursor_policy.LeaseCursor(
            _grant(fencing_token=2),
            pos=_START_POS,
        )
        crossed_grant = cursor_policy._issue_replayable_page(
            other_grant_cursor.prepare(_NEXT_POS)
        )
        other_cursor = cursor_policy.LeaseCursor(
            self.grant,
            pos=_START_POS,
        )
        crossed_candidate = cursor_policy._issue_replayable_page(
            other_cursor.prepare(_NEXT_POS)
        )
        skipped_cursor = cursor_policy.LeaseCursor(
            self.grant,
            pos=_START_POS,
        )
        first = skipped_cursor.prepare(_NEXT_POS)
        skipped_cursor.accept_replayable(
            cursor_policy._issue_replayable_page(first)
        )
        skipped_sequence = cursor_policy._issue_replayable_page(
            skipped_cursor.prepare(_NEXT_POS + datetime.timedelta(seconds=1))
        )

        self.assertEqual(candidate, other_cursor.outstanding_candidate)
        for settlement in (
            crossed_grant,
            crossed_candidate,
            skipped_sequence,
        ):
            with self.subTest(settlement=settlement):
                with self.assertRaises(cursor_policy.CursorIntegrityError):
                    self.cursor.accept_replayable(settlement)
                self.assertEqual(_cursor_state(self.cursor), before)
                self.assertIs(self.cursor.outstanding_candidate, candidate)

    def test_replayable_rejects_no_progress_and_wrong_settlement_types(
        self,
    ) -> None:
        candidate = self.cursor.prepare(_NEXT_POS)
        before = _cursor_state(self.cursor)
        no_progress_cursor = cursor_policy.LeaseCursor(
            self.grant,
            pos=_START_POS,
        )
        no_progress = no_progress_cursor.prepare_no_progress()

        wrong_settlements = (
            cursor_policy._issue_no_progress_page(no_progress),
            cursor_policy._issue_covered_page(candidate),
            types.SimpleNamespace(
                grant=candidate.grant,
                page_sequence=candidate.page_sequence,
                last_pos=candidate.last_pos,
            ),
        )
        for settlement in wrong_settlements:
            with self.subTest(settlement=settlement):
                with self.assertRaises(cursor_policy.CursorIntegrityError):
                    self.cursor.accept_replayable(
                        typing.cast(
                            "cursor_policy._ReplayablePageSettled",
                            settlement,
                        )
                    )
                self.assertEqual(_cursor_state(self.cursor), before)
                self.assertIs(self.cursor.outstanding_candidate, candidate)

    def test_replayable_duplicate_use_is_non_mutating(self) -> None:
        candidate = self.cursor.prepare(_NEXT_POS)
        settlement = cursor_policy._issue_replayable_page(candidate)

        self.cursor.accept_replayable(settlement)
        before_duplicate = _cursor_state(self.cursor)

        with self.assertRaises(cursor_policy.CursorIntegrityError):
            self.cursor.accept_replayable(settlement)
        self.assertEqual(_cursor_state(self.cursor), before_duplicate)

    def test_no_progress_rejects_crossed_and_wrong_settlements_non_mutating(
        self,
    ) -> None:
        candidate = self.cursor.prepare_no_progress()
        before = _cursor_state(self.cursor)
        other_grant = cursor_policy.LeaseCursor(
            _grant(fencing_token=2),
            pos=_START_POS,
        )
        other_candidate = other_grant.prepare_no_progress()
        crossed = cursor_policy._issue_no_progress_page(other_candidate)
        same_grant = cursor_policy.LeaseCursor(self.grant, pos=_START_POS)
        wrong_seal = cursor_policy._issue_no_progress_page(
            same_grant.prepare_no_progress()
        )
        skipped_cursor = cursor_policy.LeaseCursor(self.grant, pos=_START_POS)
        first = skipped_cursor.prepare_no_progress()
        skipped_cursor.accept_no_progress(
            cursor_policy._issue_no_progress_page(first)
        )
        skipped = cursor_policy._issue_no_progress_page(
            skipped_cursor.prepare_no_progress()
        )

        for settlement in (crossed, wrong_seal, skipped):
            with self.subTest(settlement=settlement):
                with self.assertRaises(cursor_policy.CursorIntegrityError):
                    self.cursor.accept_no_progress(settlement)
                self.assertEqual(_cursor_state(self.cursor), before)
                self.assertIs(self.cursor.outstanding_candidate, candidate)

    def test_progress_and_no_progress_settlement_types_cannot_cross(
        self,
    ) -> None:
        no_progress = self.cursor.prepare_no_progress()
        before = _cursor_state(self.cursor)
        progress_cursor = cursor_policy.LeaseCursor(
            self.grant,
            pos=_START_POS,
        )
        progress = progress_cursor.prepare(_NEXT_POS)
        progress_before = _cursor_state(progress_cursor)

        with self.assertRaises(cursor_policy.CursorIntegrityError):
            self.cursor.accept(
                cursor_policy._issue_no_progress_page(no_progress)
            )
        self.assertEqual(_cursor_state(self.cursor), before)
        with self.assertRaises(cursor_policy.CursorIntegrityError):
            self.cursor.accept_no_progress(
                cursor_policy._issue_covered_page(progress)
            )
        self.assertEqual(_cursor_state(self.cursor), before)
        with self.assertRaises(cursor_policy.CursorIntegrityError):
            progress_cursor.accept_no_progress(
                cursor_policy._issue_no_progress_page(no_progress)
            )
        self.assertEqual(_cursor_state(progress_cursor), progress_before)

    def test_absent_or_structural_receipt_is_non_mutating(self) -> None:
        candidate = self.cursor.prepare(_NEXT_POS)
        before = _cursor_state(self.cursor)
        fake = types.SimpleNamespace(
            grant=self.grant,
            page_sequence=candidate.page_sequence,
            last_pos=candidate.last_pos,
        )

        for receipt in (None, fake):
            with self.subTest(receipt=receipt):
                with self.assertRaises(cursor_policy.CursorIntegrityError):
                    self.cursor.accept(
                        typing.cast("cursor_policy._CoveredPage", receipt)
                    )
                self.assertEqual(_cursor_state(self.cursor), before)

    def test_cross_grant_receipt_is_non_mutating(self) -> None:
        self.cursor.prepare(_NEXT_POS)
        other = cursor_policy.LeaseCursor(
            _grant(fencing_token=2),
            pos=_START_POS,
        )
        receipt = cursor_policy._issue_covered_page(other.prepare(_NEXT_POS))
        before = _cursor_state(self.cursor)

        with self.assertRaises(cursor_policy.CursorIntegrityError):
            self.cursor.accept(receipt)

        self.assertEqual(_cursor_state(self.cursor), before)

    def test_structurally_equal_wrong_candidate_is_non_mutating(self) -> None:
        target_candidate = self.cursor.prepare(_NEXT_POS)
        other = cursor_policy.LeaseCursor(self.grant, pos=_START_POS)
        other_candidate = other.prepare(_NEXT_POS)
        self.assertEqual(other_candidate, target_candidate)
        receipt = cursor_policy._issue_covered_page(other_candidate)
        before = _cursor_state(self.cursor)

        with self.assertRaises(cursor_policy.CursorIntegrityError):
            self.cursor.accept(receipt)

        self.assertEqual(_cursor_state(self.cursor), before)
        self.assertIs(self.cursor.outstanding_candidate, target_candidate)

    def test_skipped_sequence_receipt_is_non_mutating(self) -> None:
        self.cursor.prepare(_NEXT_POS)
        other = cursor_policy.LeaseCursor(self.grant, pos=_START_POS)
        first = other.prepare(_NEXT_POS)
        other.accept(cursor_policy._issue_covered_page(first))
        skipped = other.prepare(_NEXT_POS + datetime.timedelta(seconds=1))
        receipt = cursor_policy._issue_covered_page(skipped)
        before = _cursor_state(self.cursor)

        with self.assertRaises(cursor_policy.CursorIntegrityError):
            self.cursor.accept(receipt)

        self.assertEqual(_cursor_state(self.cursor), before)

    def test_regressive_receipt_is_non_mutating(self) -> None:
        self.cursor.prepare(_NEXT_POS)
        earlier = _START_POS - datetime.timedelta(minutes=2)
        other = cursor_policy.LeaseCursor(self.grant, pos=earlier)
        regressive = other.prepare(earlier + datetime.timedelta(minutes=1))
        receipt = cursor_policy._issue_covered_page(regressive)
        before = _cursor_state(self.cursor)

        with self.assertRaises(cursor_policy.CursorIntegrityError):
            self.cursor.accept(receipt)

        self.assertEqual(_cursor_state(self.cursor), before)

    def test_prepared_candidate_without_receipt_does_not_move_progress(
        self,
    ) -> None:
        candidate = self.cursor.prepare(_NEXT_POS)

        self.assertEqual(self.cursor.pos, _START_POS)
        self.assertEqual(self.cursor.next_page_sequence, 0)
        self.assertIs(self.cursor.outstanding_candidate, candidate)

    def test_restart_rebuilds_only_from_durable_feed_inputs(self) -> None:
        durable_feed_cursors = (
            _START_POS - datetime.timedelta(minutes=1),
            _START_POS,
        )
        decision = cursor_policy.bootstrap_cursor(
            durable_feed_cursors,
            now=_NOW,
        )
        old = cursor_policy.LeaseCursor(self.grant, pos=decision.pos)
        candidate = old.prepare(_NEXT_POS)
        old.accept(cursor_policy._issue_covered_page(candidate))

        restarted_decision = cursor_policy.bootstrap_cursor(
            durable_feed_cursors,
            now=_NOW,
        )
        restarted = cursor_policy.LeaseCursor(
            _grant(fencing_token=2),
            pos=restarted_decision.pos,
        )

        self.assertEqual(restarted.pos, restarted_decision.pos)
        self.assertNotEqual(restarted.pos, old.pos)
        self.assertEqual(restarted.next_page_sequence, 0)
        self.assertIsNone(restarted.outstanding_candidate)
        for persistence_name in (
            "history",
            "receipts",
            "save",
            "load",
            "serialize",
            "to_dict",
        ):
            with self.subTest(persistence_name=persistence_name):
                self.assertFalse(hasattr(restarted, persistence_name))


if __name__ == "__main__":
    unittest.main()
