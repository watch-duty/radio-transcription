from __future__ import annotations

import logging
import unittest
from typing import Any, cast

from backend.pipeline.ingestion.collectors.batch_outcome import (
    BatchOutcome,
    emit_batch_unproductive,
)
from backend.pipeline.ingestion.slo_contract import (
    EVENT_TYPE_BATCH_UNPRODUCTIVE,
)


class TestBatchOutcome(unittest.TestCase):
    def test_records_item_attempt(self) -> None:
        outcome = BatchOutcome()

        outcome.record_item_attempt()

        self.assertEqual(outcome.attempted, 1)

    def test_records_chunk_produced(self) -> None:
        outcome = BatchOutcome()

        outcome.record_chunk_produced()

        self.assertEqual(outcome.produced, 1)

    def test_attempted_without_produced_is_unproductive(self) -> None:
        outcome = BatchOutcome(attempted=2, produced=0)

        self.assertTrue(outcome.is_unproductive)

    def test_zero_attempts_is_not_unproductive(self) -> None:
        outcome = BatchOutcome(attempted=0, produced=0)

        self.assertFalse(outcome.is_unproductive)

    def test_any_produced_chunk_is_productive(self) -> None:
        outcome = BatchOutcome(attempted=2, produced=1)

        self.assertFalse(outcome.is_unproductive)


class TestEmitBatchUnproductive(unittest.TestCase):
    def test_emits_structured_warning_for_unproductive_batch(self) -> None:
        logger = logging.getLogger(
            "backend.pipeline.ingestion.collectors.tests.batch_outcome"
        )
        feed: dict[str, Any] = {
            "id": "feed-id",
            "source_type": "fire_notifications",
        }

        with self.assertLogs(logger, level="WARNING") as cm:
            emit_batch_unproductive(
                logger,
                feed,
                BatchOutcome(attempted=3, produced=0),
                reason="downloads_failing",
            )

        record = cast("Any", cm.records[0])
        self.assertEqual(record.getMessage(), "Batch unproductive")
        self.assertEqual(
            set(record.json_fields.keys()),
            {
                "event_type",
                "feed_id",
                "source_type",
                "attempted",
                "produced",
                "reason",
            },
        )
        self.assertEqual(
            record.json_fields["event_type"],
            EVENT_TYPE_BATCH_UNPRODUCTIVE,
        )
        self.assertEqual(record.json_fields["feed_id"], "feed-id")
        self.assertEqual(
            record.json_fields["source_type"],
            "fire_notifications",
        )
        self.assertEqual(record.json_fields["attempted"], 3)
        self.assertEqual(record.json_fields["produced"], 0)
        self.assertEqual(record.json_fields["reason"], "downloads_failing")

    def test_does_not_emit_for_zero_attempt_batch(self) -> None:
        logger = logging.getLogger(
            "backend.pipeline.ingestion.collectors.tests.batch_outcome"
        )
        feed: dict[str, Any] = {
            "id": "feed-id",
            "source_type": "fire_notifications",
        }

        with self.assertNoLogs(logger, level="WARNING"):
            emit_batch_unproductive(
                logger,
                feed,
                BatchOutcome(attempted=0, produced=0),
                reason="downloads_failing",
            )

    def test_does_not_emit_for_productive_batch(self) -> None:
        logger = logging.getLogger(
            "backend.pipeline.ingestion.collectors.tests.batch_outcome"
        )
        feed: dict[str, Any] = {
            "id": "feed-id",
            "source_type": "fire_notifications",
        }

        with self.assertNoLogs(logger, level="WARNING"):
            emit_batch_unproductive(
                logger,
                feed,
                BatchOutcome(attempted=3, produced=1),
                reason="downloads_failing",
            )


if __name__ == "__main__":
    unittest.main()
