"""Tests for the pure Broadcastify Calls cursor policy."""

from __future__ import annotations

import dataclasses
import datetime
import typing
import unittest
import uuid

from backend.pipeline.ingestion.collectors.bcfy_calls import cursor_policy
from backend.pipeline.storage import feed_store, ingestion_lease_store

if typing.TYPE_CHECKING:
    import collections.abc

_NOW = datetime.datetime(2026, 7, 12, 12, 0, tzinfo=datetime.UTC)
_LIVE_WINDOW_START = _NOW - datetime.timedelta(minutes=5)
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


def _assign_attribute(
    target: object,
    field_name: str,
    replacement: object,
) -> None:
    """Attempt assignment through the object's normal frozen guard."""
    setattr(target, field_name, replacement)


class TestMinimumDurableCursor(unittest.TestCase):
    """Tests for selecting the oldest durable Feed cursor."""

    def test_empty_or_all_null_input_omits_start(self) -> None:
        for cursors in ((), (None, None)):
            with self.subTest(cursors=cursors):
                self.assertIsNone(cursor_policy.minimum_durable_cursor(cursors))

    def test_generator_uses_minimum_non_null_once(self) -> None:
        newer = _NOW - datetime.timedelta(minutes=1)
        oldest = datetime.datetime(
            2026,
            7,
            12,
            11,
            56,
            59,
            123456,
            tzinfo=datetime.UTC,
        )
        observed: list[datetime.datetime | None] = []

        def cursors() -> collections.abc.Iterator[datetime.datetime | None]:
            for cursor in (None, newer, oldest, None):
                observed.append(cursor)
                yield cursor

        cursor_generator = cursors()

        selected = cursor_policy.minimum_durable_cursor(cursor_generator)

        self.assertEqual(selected, oldest)
        self.assertEqual(observed, [None, newer, oldest, None])
        self.assertEqual(list(cursor_generator), [])

    def test_rejects_invalid_utc_feed_cursor(self) -> None:
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
            with self.subTest(cursor=repr(cursor)):
                with self.assertRaisesRegex(
                    ValueError,
                    "Feed cursor must be UTC-aware",
                ):
                    cursor_policy.minimum_durable_cursor((cursor,))


class TestLiveRequestStart(unittest.TestCase):
    """Tests for Broadcastify's fixed five-minute live request window."""

    def test_older_start_is_clamped_at_request_boundary(self) -> None:
        requested = _NOW - datetime.timedelta(minutes=20)

        selected = cursor_policy.clamp_live_request_start(
            requested,
            now=_NOW,
        )

        self.assertEqual(selected, _LIVE_WINDOW_START)

    def test_exact_window_start_and_newer_start_are_unchanged(self) -> None:
        cases = (
            _LIVE_WINDOW_START,
            _LIVE_WINDOW_START + datetime.timedelta(microseconds=1),
        )

        for requested in cases:
            with self.subTest(requested=repr(requested)):
                selected = cursor_policy.clamp_live_request_start(
                    requested,
                    now=_NOW,
                )

                self.assertEqual(selected, requested)

    def test_omitted_start_remains_omitted(self) -> None:
        self.assertIsNone(
            cursor_policy.clamp_live_request_start(None, now=_NOW)
        )

    def test_validates_utc_now_and_requested_start(self) -> None:
        naive = datetime.datetime(2026, 7, 12, 12, 0)

        with self.assertRaisesRegex(ValueError, "now must be UTC-aware"):
            cursor_policy.clamp_live_request_start(None, now=naive)
        with self.assertRaisesRegex(
            ValueError,
            "requested_start must be UTC-aware",
        ):
            cursor_policy.clamp_live_request_start(naive, now=_NOW)


class TestPageCandidate(unittest.TestCase):
    """Tests for the one immutable page correlation value."""

    def test_candidate_contains_only_behavior_driving_fields(self) -> None:
        candidate = cursor_policy.LeaseCursor(
            _grant(),
            pos=_START_POS,
        ).prepare(_NEXT_POS)

        self.assertEqual(
            tuple(field.name for field in dataclasses.fields(candidate)),
            ("grant", "page_sequence", "last_pos"),
        )
        self.assertFalse(hasattr(candidate, "__dict__"))
        with self.assertRaises(dataclasses.FrozenInstanceError):
            _assign_attribute(candidate, "page_sequence", 1)

    def test_none_last_pos_represents_no_progress(self) -> None:
        candidate = cursor_policy.LeaseCursor(
            _grant(),
            pos=_START_POS,
        ).prepare(None)

        self.assertIsNone(candidate.last_pos)

    def test_constructor_enforces_sequence_and_utc_invariants(self) -> None:
        for page_sequence in (True, -1):
            with self.subTest(page_sequence=page_sequence):
                with self.assertRaises(cursor_policy.CursorIntegrityError):
                    cursor_policy.PageCandidate(
                        _grant(),
                        typing.cast("int", page_sequence),
                        _NEXT_POS,
                    )

        with self.assertRaisesRegex(
            cursor_policy.CursorIntegrityError,
            "last_pos must be UTC-aware",
        ):
            cursor_policy.PageCandidate(
                _grant(),
                0,
                datetime.datetime(2026, 7, 12, 12, 0),
            )

    def test_public_surface_has_one_candidate_and_one_outcome(self) -> None:
        self.assertEqual(
            tuple(cursor_policy.__all__),
            (
                "CursorIntegrityError",
                "CursorOutcome",
                "LeaseCursor",
                "PageCandidate",
                "clamp_live_request_start",
                "minimum_durable_cursor",
            ),
        )

    def test_outcome_vocabulary_is_exact(self) -> None:
        self.assertEqual(
            tuple(cursor_policy.CursorOutcome),
            (
                cursor_policy.CursorOutcome.COVERED,
                cursor_policy.CursorOutcome.REPLAYABLE,
                cursor_policy.CursorOutcome.NO_PROGRESS,
            ),
        )


class TestLeaseCursor(unittest.TestCase):
    """Tests for exact-next, grant-local Lease Cursor settlement."""

    def setUp(self) -> None:
        self.grant = _grant()
        self.cursor = cursor_policy.LeaseCursor(
            self.grant,
            pos=_START_POS,
        )

    def test_prepare_binds_grant_and_sequence_without_advancing(self) -> None:
        candidate = self.cursor.prepare(_NEXT_POS)

        self.assertEqual(candidate.grant, self.grant)
        self.assertEqual(candidate.page_sequence, 0)
        self.assertEqual(candidate.last_pos, _NEXT_POS)
        self.assertEqual(self.cursor.pos, _START_POS)

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
            with self.subTest(last_pos=repr(last_pos)):
                cursor = cursor_policy.LeaseCursor(
                    self.grant,
                    pos=_START_POS,
                )
                with self.assertRaises(cursor_policy.CursorIntegrityError):
                    cursor.prepare(last_pos)
                self.assertEqual(cursor.pos, _START_POS)
                self.assertEqual(cursor.prepare(_NEXT_POS).page_sequence, 0)

    def test_prepare_allows_only_one_outstanding_candidate(self) -> None:
        candidate = self.cursor.prepare(_NEXT_POS)

        with self.assertRaises(cursor_policy.CursorIntegrityError):
            self.cursor.prepare(None)

        self.cursor.settle(candidate, cursor_policy.CursorOutcome.COVERED)
        self.assertEqual(
            self.cursor.prepare(_NEXT_POS).page_sequence,
            1,
        )

    def test_covered_advances_exact_candidate_once(self) -> None:
        candidate = self.cursor.prepare(_NEXT_POS)

        result = self.cursor.settle(
            candidate,
            cursor_policy.CursorOutcome.COVERED,
        )

        self.assertIsNone(result)
        self.assertEqual(self.cursor.pos, _NEXT_POS)

        with self.assertRaises(cursor_policy.CursorIntegrityError):
            self.cursor.settle(
                candidate,
                cursor_policy.CursorOutcome.COVERED,
            )
        self.assertEqual(self.cursor.pos, _NEXT_POS)
        self.assertEqual(
            self.cursor.prepare(_NEXT_POS).page_sequence,
            1,
        )

    def test_equal_last_pos_settles_without_regression(self) -> None:
        candidate = self.cursor.prepare(_START_POS)

        self.cursor.settle(candidate, cursor_policy.CursorOutcome.COVERED)

        self.assertEqual(self.cursor.pos, _START_POS)

    def test_replayable_settles_without_advancing(self) -> None:
        candidate = self.cursor.prepare(_NEXT_POS)
        old_pos = self.cursor.pos

        self.cursor.settle(
            candidate,
            cursor_policy.CursorOutcome.REPLAYABLE,
        )

        self.assertIs(self.cursor.pos, old_pos)
        next_candidate = self.cursor.prepare(
            _NEXT_POS + datetime.timedelta(seconds=10)
        )
        self.assertEqual(next_candidate.page_sequence, 1)

    def test_no_progress_settles_without_advancing(self) -> None:
        candidate = self.cursor.prepare(None)
        old_pos = self.cursor.pos

        self.cursor.settle(
            candidate,
            cursor_policy.CursorOutcome.NO_PROGRESS,
        )

        self.assertIs(self.cursor.pos, old_pos)
        self.assertEqual(self.cursor.prepare(None).page_sequence, 1)

    def test_candidate_and_outcome_must_agree(self) -> None:
        cases = (
            (_NEXT_POS, cursor_policy.CursorOutcome.NO_PROGRESS),
            (None, cursor_policy.CursorOutcome.COVERED),
            (None, cursor_policy.CursorOutcome.REPLAYABLE),
        )

        for last_pos, outcome in cases:
            with self.subTest(last_pos=last_pos, outcome=outcome):
                cursor = cursor_policy.LeaseCursor(
                    self.grant,
                    pos=_START_POS,
                )
                candidate = cursor.prepare(last_pos)

                with self.assertRaises(cursor_policy.CursorIntegrityError):
                    cursor.settle(candidate, outcome)

                self.assertEqual(cursor.pos, _START_POS)
                valid_outcome = (
                    cursor_policy.CursorOutcome.COVERED
                    if last_pos is not None
                    else cursor_policy.CursorOutcome.NO_PROGRESS
                )
                cursor.settle(candidate, valid_outcome)
                self.assertEqual(
                    cursor.prepare(None).page_sequence,
                    1,
                )

    def test_structurally_equal_candidate_cannot_cross_cursor(self) -> None:
        candidate = self.cursor.prepare(_NEXT_POS)
        other_cursor = cursor_policy.LeaseCursor(
            self.grant,
            pos=_START_POS,
        )
        other_candidate = other_cursor.prepare(_NEXT_POS)
        self.assertEqual(candidate, other_candidate)
        self.assertIsNot(candidate, other_candidate)

        with self.assertRaises(cursor_policy.CursorIntegrityError):
            self.cursor.settle(
                other_candidate,
                cursor_policy.CursorOutcome.COVERED,
            )

        self.assertEqual(self.cursor.pos, _START_POS)
        self.cursor.settle(
            candidate,
            cursor_policy.CursorOutcome.COVERED,
        )
        self.assertEqual(self.cursor.pos, _NEXT_POS)

    def test_restart_rebuilds_only_from_durable_feed_inputs(self) -> None:
        durable_feed_cursors = (
            _START_POS - datetime.timedelta(minutes=1),
            _START_POS,
        )
        durable_minimum = cursor_policy.minimum_durable_cursor(
            durable_feed_cursors
        )
        start = cursor_policy.clamp_live_request_start(
            durable_minimum,
            now=_NOW,
        )
        old = cursor_policy.LeaseCursor(self.grant, pos=start)
        candidate = old.prepare(_NEXT_POS)
        old.settle(candidate, cursor_policy.CursorOutcome.COVERED)

        restarted_start = cursor_policy.clamp_live_request_start(
            cursor_policy.minimum_durable_cursor(durable_feed_cursors),
            now=_NOW,
        )
        restarted = cursor_policy.LeaseCursor(
            _grant(fencing_token=2),
            pos=restarted_start,
        )

        self.assertEqual(restarted.pos, restarted_start)
        self.assertNotEqual(restarted.pos, old.pos)
        self.assertEqual(restarted.prepare(None).page_sequence, 0)


if __name__ == "__main__":
    unittest.main()
