"""Closed schema and once-only contracts for Calls SID poll telemetry."""

from __future__ import annotations

import asyncio
import datetime
import unittest
import uuid
from unittest import mock

from backend.pipeline.ingestion import feed_work_scheduler, slo_contract
from backend.pipeline.ingestion.collectors.bcfy_calls import (
    cursor_policy,
    sid_processor,
    telemetry,
)
from backend.pipeline.storage import feed_store, ingestion_lease_store

_OWNER_ID = uuid.UUID("11111111-2222-3333-4444-555555555555")
_GRANT = ingestion_lease_store.LeaseGrant(
    feed_store.SourceType.BCFY_CALLS,
    "00123",
    _OWNER_ID,
    7,
)
_NOW = datetime.datetime(2026, 7, 13, 12, 0, tzinfo=datetime.UTC)


def _replay_event(
    cause: cursor_policy.ReplayFloorCause = (
        cursor_policy.ReplayFloorCause.OVERLOAD
    ),
    *,
    requested_start: datetime.datetime | None = None,
) -> telemetry.ReplayWindowTruncatedEvent:
    decision = cursor_policy.apply_replay_floor(
        requested_start or (_NOW - datetime.timedelta(minutes=10)),
        now=_NOW,
        cause=cause,
    )
    assert decision.floor_reached is not None
    return telemetry.ReplayWindowTruncatedEvent(
        _GRANT,
        decision.floor_reached,
    )


def _event(**changes: object) -> telemetry.SidPollEvidence:
    values: dict[str, object] = {
        "sid": "00123",
        "source_type": "bcfy_calls",
        "owner_worker_id": str(_OWNER_ID),
        "fencing_token": 7,
        "membership_revision": 4,
        "outcome": telemetry.SidPollOutcome.SUCCESS,
        "duration_seconds": 0.25,
        "provider_observed": False,
        "http_attempt_count": 0,
        "response_row_count": None,
        "response_byte_count": None,
        "request_pos": None,
        "response_last_pos": None,
        "response_last_pos_state": (
            telemetry.SidPollResponseLastPosState.NOT_OBSERVED
        ),
        "matched_call_count": 0,
        "deduplicated_call_count": 0,
        "admitted_record_count": 0,
        "admitted_cohort_count": 0,
        "lease_cursor_lag_seconds": 0.0,
        "maximum_feed_cursor_lag_seconds": 0.0,
        "null_feed_cursor_count": 0,
        "total_queue_wait_seconds": 0.0,
        "maximum_queue_wait_seconds": 0.0,
        "oldest_queue_age_seconds": 0.0,
        "pressure_encountered": False,
        "pressure_wait_count": 0,
        "pressure_wait_seconds": 0.0,
        "maximum_held_count": 0,
        "maximum_queue_depth": 0,
        "maximum_worker_utilization_numerator": 0,
        "worker_utilization_denominator": 0,
        "early_flush_attempt_count": 0,
        "final_flush_attempt_count": 0,
        "total_flush_latency_seconds": 0.0,
        "maximum_flush_latency_seconds": 0.0,
        "participating_feed_count": 0,
        "successful_feed_count": 0,
        "failed_feed_count": 0,
        "item_to_feed_promotion_count": 0,
        "lifecycle_scope": telemetry.SidPollLifecycleScope.NONE,
        "lifecycle_reason": None,
        "fence_rejection_count": 0,
        "membership_rejection_count": 0,
        "terminal_record_count": 0,
        "replay_blocked_record_count": 0,
    }
    values.update(changes)
    return telemetry.SidPollEvidence(**values)  # type: ignore[arg-type]


class TestSidPollSchema(unittest.TestCase):
    """Pin the bounded scalar schema and its cross-field invariants."""

    def test_schema_version_one_is_exact_and_scalar(self) -> None:
        payload = telemetry.sid_poll_json_fields(_event())

        self.assertEqual(
            set(payload),
            set(slo_contract.BCFY_CALLS_SID_POLL_REQUIRED_FIELDS),
        )
        self.assertEqual(payload["schema_version"], 1)
        self.assertEqual(payload["event_type"], "bcfy_calls_sid_poll")
        self.assertTrue(
            all(
                value is None or isinstance(value, (str, int, float, bool))
                for value in payload.values()
            )
        )
        lowered_keys = {key.lower() for key in payload}
        for forbidden in (
            "url",
            "token",
            "access_token",
            "auth_token",
            "payload",
            "calls",
            "feed_ids",
            "shards",
            "percentile",
        ):
            self.assertNotIn(forbidden, lowered_keys)

    def test_optional_additions_do_not_change_required_consumer_contract(
        self,
    ) -> None:
        future_payload = telemetry.sid_poll_json_fields(_event()) | {
            "future_optional_scalar": 1
        }

        self.assertLessEqual(
            set(slo_contract.BCFY_CALLS_SID_POLL_REQUIRED_FIELDS),
            set(future_payload),
        )
        self.assertEqual(
            slo_contract.BCFY_CALLS_SID_POLL_OPTIONAL_FIELDS,
            (),
        )

    def test_provider_observation_null_contract_is_closed(self) -> None:
        with self.assertRaisesRegex(ValueError, "response evidence"):
            _event(response_row_count=1)
        with self.assertRaisesRegex(ValueError, "lacks scalar evidence"):
            _event(
                provider_observed=True,
                response_last_pos_state=(
                    telemetry.SidPollResponseLastPosState.MISSING
                ),
            )

        observed = _event(
            provider_observed=True,
            http_attempt_count=3,
            response_row_count=2,
            response_byte_count=128,
            response_last_pos=1_783_908_001,
            response_last_pos_state=(
                telemetry.SidPollResponseLastPosState.VALID
            ),
        )
        self.assertEqual(observed.http_attempt_count, 3)

    def test_queue_pressure_and_verdict_invariants_are_closed(self) -> None:
        with self.assertRaisesRegex(ValueError, "pressure flag"):
            _event(pressure_encountered=True)
        with self.assertRaisesRegex(ValueError, "promotion counts"):
            _event(
                participating_feed_count=1,
                failed_feed_count=1,
                item_to_feed_promotion_count=2,
            )

        direct_only = _event(
            participating_feed_count=1,
            failed_feed_count=1,
            item_to_feed_promotion_count=0,
            lifecycle_scope=telemetry.SidPollLifecycleScope.FAILED_FEEDS,
            lifecycle_reason="source_unreachable",
        )
        self.assertEqual(direct_only.item_to_feed_promotion_count, 0)


class TestReplayWindowTruncatedTelemetry(unittest.TestCase):
    """Pin the separate CRITICAL scalar replay-window event."""

    def test_replay_window_truncated_schema_is_exact_and_distinct(self) -> None:
        payload = telemetry.replay_window_truncated_json_fields(
            _replay_event(),
        )

        self.assertEqual(
            set(payload),
            set(
                slo_contract.BCFY_CALLS_REPLAY_WINDOW_TRUNCATED_REQUIRED_FIELDS
            ),
        )
        self.assertEqual(
            payload["event_type"],
            "bcfy_calls_replay_window_truncated",
        )
        self.assertEqual(payload["schema_version"], 1)
        self.assertNotEqual(
            payload["event_type"],
            slo_contract.EVENT_TYPE_BCFY_CALLS_SID_POLL,
        )
        self.assertNotEqual(
            payload["event_type"],
            slo_contract.EVENT_TYPE_BCFY_CALLS_MISSING_CALL,
        )
        self.assertTrue(
            all(
                isinstance(value, (str, int, float))
                for value in payload.values()
            )
        )
        lowered_keys = {key.lower() for key in payload}
        for forbidden in (
            "url",
            "call",
            "calls",
            "row_count",
            "feed_id",
            "feed_ids",
            "raw_payload",
            "outcome",
        ):
            self.assertNotIn(forbidden, lowered_keys)

    def test_floor_equality_payload_preserves_exact_zero_duration(self) -> None:
        floor = _NOW - datetime.timedelta(minutes=5)

        for cause in cursor_policy.ReplayFloorCause:
            with self.subTest(cause=cause):
                payload = telemetry.replay_window_truncated_json_fields(
                    _replay_event(cause, requested_start=floor)
                )
                expected = floor.isoformat()
                self.assertEqual(payload["cause"], cause.value)
                self.assertEqual(payload["requested_start"], expected)
                self.assertEqual(payload["floor_start"], expected)
                self.assertEqual(payload["lost_span_start"], expected)
                self.assertEqual(payload["lost_span_end"], expected)
                self.assertEqual(payload["lost_duration_seconds"], 0.0)

    def test_emitter_uses_critical_and_contains_logger_failure(self) -> None:
        event = _replay_event()
        with mock.patch.object(telemetry.logger, "critical") as critical:
            telemetry.emit_replay_window_truncated(event)
        critical.assert_called_once()
        self.assertEqual(
            critical.call_args.kwargs["extra"]["json_fields"]["cause"],
            "overload",
        )

        with mock.patch.object(
            telemetry.logger,
            "critical",
            side_effect=RuntimeError("logging unavailable"),
        ):
            telemetry.emit_replay_window_truncated(event)


class TestSidPollAccumulator(unittest.TestCase):
    """Verify exact owner evidence, once-only close, and best-effort emit."""

    def test_scheduler_evidence_is_copied_without_reconstruction(self) -> None:
        accumulator = sid_processor._SidPollAccumulator(_GRANT, 10.0)
        accumulator.observe_scheduler(
            feed_work_scheduler.SchedulerPageEvidence(
                admitted_record_count=3,
                admitted_cohort_count=2,
                terminal_record_count=3,
                replay_blocked_record_count=1,
                total_queue_wait_seconds=4.0,
                maximum_queue_wait_seconds=2.0,
                oldest_queue_age_seconds=1.5,
                pressure_encountered=True,
                pressure_wait_count=1,
                pressure_wait_seconds=0.5,
                maximum_held_count=4,
                maximum_queue_depth=3,
                maximum_worker_utilization_numerator=2,
                worker_utilization_denominator=4,
                early_flush_attempt_count=1,
                final_flush_attempt_count=1,
                total_flush_latency_seconds=0.75,
                maximum_flush_latency_seconds=0.5,
                fence_rejection_count=1,
                member_rejection_count=2,
            )
        )
        emitted: list[telemetry.SidPollEvidence] = []

        event = accumulator.close(
            telemetry.SidPollOutcome.SUCCESS,
            finished_monotonic=11.0,
            emitter=emitted.append,
        )

        self.assertEqual(emitted, [event])
        self.assertEqual(event.oldest_queue_age_seconds, 1.5)
        self.assertEqual(event.maximum_queue_depth, 3)
        self.assertEqual(event.membership_rejection_count, 2)
        self.assertEqual(event.item_to_feed_promotion_count, 0)
        with self.assertRaises(feed_work_scheduler.CohortIntegrityError):
            accumulator.close(
                telemetry.SidPollOutcome.SUCCESS,
                finished_monotonic=12.0,
                emitter=emitted.append,
            )

    def test_emitter_failure_is_contained_after_once_only_close(self) -> None:
        accumulator = sid_processor._SidPollAccumulator(_GRANT, 10.0)

        event = accumulator.close(
            telemetry.SidPollOutcome.PROVIDER_FAILED,
            finished_monotonic=10.5,
            emitter=lambda _event: (_ for _ in ()).throw(
                RuntimeError("telemetry unavailable")
            ),
        )

        self.assertTrue(accumulator.closed)
        self.assertEqual(
            event.outcome, telemetry.SidPollOutcome.PROVIDER_FAILED
        )

    def test_zero_queue_has_explicit_zero_oldest_age(self) -> None:
        event = sid_processor._SidPollAccumulator(_GRANT, 1.0).close(
            telemetry.SidPollOutcome.MEMBERSHIP_UNCERTAIN,
            finished_monotonic=1.0,
            emitter=lambda _event: None,
        )

        self.assertEqual(event.oldest_queue_age_seconds, 0.0)
        self.assertFalse(event.provider_observed)
        self.assertEqual(event.http_attempt_count, 0)
        self.assertIsNone(event.response_row_count)


class TestProcessorCancellationHandoff(unittest.TestCase):
    """Pin the runner's once-only exact cancellation-object handoff."""

    def test_identical_repeat_is_idempotent_but_replacement_fails(self) -> None:
        handoff = sid_processor.ProcessorCancellationHandoff(_GRANT)
        first = asyncio.CancelledError("first")
        handoff._record(
            sid_processor.RunnerCancellationCause.RAW_RUNNER_CANCELLATION,
            first,
        )
        handoff._record(
            sid_processor.RunnerCancellationCause.RAW_RUNNER_CANCELLATION,
            first,
        )

        self.assertIs(handoff.cancelled_error, first)
        self.assertIs(
            handoff.cause,
            sid_processor.RunnerCancellationCause.RAW_RUNNER_CANCELLATION,
        )
        with self.assertRaises(feed_work_scheduler.CohortIntegrityError):
            handoff._record(
                sid_processor.RunnerCancellationCause.EXACT_GRANT_LOSS,
                asyncio.CancelledError("replacement"),
            )


if __name__ == "__main__":
    unittest.main()
