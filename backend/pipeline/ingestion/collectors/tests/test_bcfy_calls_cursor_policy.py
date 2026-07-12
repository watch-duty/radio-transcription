"""Tests for the pure Broadcastify Calls cursor policy."""

from __future__ import annotations

import dataclasses
import datetime
import typing
import unittest

from backend.pipeline.ingestion.collectors.bcfy_calls import cursor_policy

if typing.TYPE_CHECKING:
    import collections.abc

_NOW = datetime.datetime(2026, 7, 12, 12, 0, tzinfo=datetime.UTC)
_REPLAY_FLOOR = _NOW - datetime.timedelta(minutes=5)


class TestBootstrapCursor(unittest.TestCase):
    """Tests for durable Feed cursor bootstrap decisions."""

    def test_bootstrap_decision_is_frozen_slotted_policy_evidence(
        self,
    ) -> None:
        decision = cursor_policy.bootstrap_cursor([], now=_NOW)

        self.assertEqual(
            tuple(field.name for field in dataclasses.fields(decision)),
            ("pos", "durable_minimum", "replay_floor", "clamped"),
        )
        self.assertFalse(hasattr(decision, "__dict__"))
        with self.assertRaises(dataclasses.FrozenInstanceError):
            decision.clamped = True  # type: ignore[misc]

    def test_bootstrap_empty_or_all_null_omits_pos(self) -> None:
        for cursors in ((), (None, None)):
            with self.subTest(cursors=cursors):
                decision = cursor_policy.bootstrap_cursor(cursors, now=_NOW)

                self.assertIsNone(decision.pos)
                self.assertIsNone(decision.durable_minimum)
                self.assertEqual(decision.replay_floor, _REPLAY_FLOOR)
                self.assertFalse(decision.clamped)

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


if __name__ == "__main__":
    unittest.main()
