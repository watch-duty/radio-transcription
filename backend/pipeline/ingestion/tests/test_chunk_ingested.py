"""LOG-01: chunk_ingested INFO emit after bookmark-ok — golden-file tests.

Covers D-11 (json_fields wrapper), D-15 (conditional latency_clamped),
D-16 (absent processing_latency_sec when receipt_time is None). Test matrix:

    1. Stamped + unclamped  -> common-case golden (4 keys)
    2. Stamped + clamped    -> clock-skew golden (5 keys, latency_clamped=True)
    3. Unstamped            -> unstamped golden (3 keys, no latency fields)
    4. Bookmark fails       -> no emit (fence-violation path skips the log)

All 4 tests exercise the real _process_feed loop with mocked GCS / Pub/Sub
/ FeedStore so the assertion is on the record the emit actually produces,
not on a unit-extracted helper.
"""

from __future__ import annotations

import asyncio
import datetime
import json
import logging
import pathlib
import unittest
import uuid
from typing import Any, cast
from unittest import mock

import aiohttp

from backend.pipeline.common.constants import CHUNK_DURATION_SECONDS
from backend.pipeline.ingestion.collector_runtime import CollectorRuntime
from backend.pipeline.ingestion.models import CapturedChunk, CaptureResources
from backend.pipeline.storage.feed_store import (
    FeedStatusReason,
    LeasedFeed,
    SourceType,
)
from backend.pipeline.storage.settings import AlloyDBSettings

_WORKER_ID = uuid.UUID("11111111-2222-3333-4444-555555555555")
_FEED_ID = uuid.UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")

_FEED = LeasedFeed(
    id=_FEED_ID,
    name="Test Feed",
    source_type=SourceType.BCFY_FEEDS,
    last_processed_filename=None,
    last_bookmark_time=None,
    fencing_token=1,
    source_feed_id="123",
)

_GOLDEN_DIR = pathlib.Path(__file__).parent / "golden"


def _make_settings(**overrides: object) -> mock.MagicMock:
    """Mirror of test_collector_runtime._make_settings: real-valued mock CollectorSettings."""
    defaults: dict[str, object] = {
        "worker_id": _WORKER_ID,
        "max_feeds_per_worker": 250,
        "lease_poll_interval_sec": 5.0,
        "heartbeat_interval_sec": 15.0,
        "heartbeat_stall_timeout_sec": 45.0,
        "graceful_shutdown_timeout_sec": 10.0,
        "audio_staging_bucket": "test-bucket",
        "continuous_pubsub_topic_path": "projects/p/topics/t",
        "db": AlloyDBSettings(
            host="10.0.0.1",
            port=6432,
            user="user",
            db_name="db",
            password="pass",
            pool_min_size=2,
            pool_max_size=5,
            command_timeout_sec=30.0,
            connect_timeout_sec=10.0,
        ),
        "google_cloud_project": None,
        "feed_failure_threshold": 3,
        "abandonment_window_sec": 60.0,
        "gcs_upload_max_retries": 3,
        "gcs_upload_retry_base_delay_sec": 0.5,
        "gcs_upload_retry_max_delay_sec": 8.0,
        "bookmark_max_retries": 2,
        "bookmark_retry_base_delay_sec": 0.5,
        "bookmark_retry_max_delay_sec": 4.0,
        "pubsub_publish_max_retries": 2,
        "pubsub_publish_retry_base_delay_sec": 0.5,
        "pubsub_publish_retry_max_delay_sec": 4.0,
        "health_check_port": 8080,
        "health_check_startup_grace_sec": 120.0,
    }
    defaults.update(overrides)
    m = mock.MagicMock()
    m.configure_mock(**defaults)
    return m


def _mock_pubsub_publish(
    message_id: str = "test-message-id",
) -> mock._patch:
    return mock.patch(
        "backend.pipeline.ingestion.collector_runtime."
        "gcp_helper.publish_audio_chunk",
        new_callable=mock.AsyncMock,
        return_value=message_id,
    )


def _mock_upload_audio(gcs_path: str = "gs://b/p") -> mock._patch:
    return mock.patch(
        "backend.pipeline.ingestion.collector_runtime."
        "gcp_helper.upload_staged_audio",
        new_callable=mock.AsyncMock,
        return_value=gcs_path,
    )


def _make_chunk(
    receipt_time: datetime.datetime | None,
    session_id: str | None = None,
) -> CapturedChunk:
    """Build a CapturedChunk with a given receipt_time (or None)."""
    now = datetime.datetime.now(datetime.UTC)
    return CapturedChunk(
        audio_bytes=b"audio",
        chunk_start_time=now,
        chunk_end_time=now + datetime.timedelta(seconds=CHUNK_DURATION_SECONDS),
        session_id=session_id or str(uuid.uuid4()),
        receipt_time=receipt_time,
    )


def _build_runtime_for_one_chunk(
    chunk: CapturedChunk,
    *,
    bookmark_ok: bool,
) -> CollectorRuntime:
    """Construct a runtime wired to yield a single chunk then stop.

    When bookmark_ok=True we set shutdown so _process_feed returns cleanly
    after the emit (before asking for more chunks). When bookmark_ok=False
    the emit is unreachable: the fence-violation branch triggers os._exit,
    which the caller mocks away.
    """

    async def _one_chunk(
        feed: LeasedFeed,
        shutdown: asyncio.Event,
        _resources: CaptureResources,
    ):
        yield chunk

    rt = CollectorRuntime(capture_fn=_one_chunk, settings=_make_settings())
    rt._shutdown = asyncio.Event()
    rt._lease_lost = asyncio.Event()
    rt._capture_resources = CaptureResources(
        http_session=mock.AsyncMock(spec=aiohttp.ClientSession),
    )
    rt._store = mock.AsyncMock()
    rt._store.update_feed_progress.return_value = bookmark_ok
    rt._releasing_feeds = set()
    return rt


class TestChunkIngestedEmit(unittest.IsolatedAsyncioTestCase):
    """LOG-01: chunk_ingested INFO emit after bookmark-ok."""

    async def _emit_records(
        self,
        chunk: CapturedChunk,
        *,
        bookmark_ok: bool = True,
    ) -> list[logging.LogRecord]:
        rt = _build_runtime_for_one_chunk(chunk, bookmark_ok=bookmark_ok)

        with (
            _mock_upload_audio(),
            _mock_pubsub_publish(),
            self.assertLogs(
                "backend.pipeline.ingestion.collector_runtime",
                level=logging.INFO,
            ) as cm,
        ):
            if bookmark_ok:
                await rt._process_feed(_FEED)
            else:
                # Real os._exit(1) never returns. Mocking it to a plain no-op
                # would let control fall into the emit block (a test artifact),
                # so we raise SystemExit from the mock to faithfully model the
                # "process dies here" semantics.
                with (
                    mock.patch(
                        "backend.pipeline.ingestion.collector_runtime.os._exit",
                        side_effect=SystemExit(1),
                    ),
                    mock.patch("logging.shutdown"),
                    self.assertRaises(SystemExit),
                ):
                    await rt._process_feed(_FEED)

        return [r for r in cm.records if r.getMessage() == "Chunk ingested"]

    async def test_emits_chunk_ingested_with_stamped_receipt(self) -> None:
        """Stamped + non-negative latency -> 4-key payload, no latency_clamped."""
        receipt = datetime.datetime.now(
            datetime.UTC,
        ) - datetime.timedelta(seconds=1.25)
        chunk = _make_chunk(receipt)

        records = await self._emit_records(chunk)

        self.assertEqual(len(records), 1)
        rec = cast("Any", records[0])
        self.assertEqual(rec.levelname, "INFO")
        self.assertTrue(rec.name.startswith("backend.pipeline.ingestion."))
        self.assertEqual(rec.json_fields["event_type"], "chunk_ingested")
        self.assertEqual(rec.json_fields["feed_id"], str(_FEED_ID))
        self.assertEqual(rec.json_fields["source_type"], SourceType.BCFY_FEEDS)
        latency = rec.json_fields["processing_latency_sec"]
        self.assertIsInstance(latency, float)
        self.assertGreaterEqual(latency, 1.2)
        # Round-to-2-decimals + wall-clock slack.
        self.assertLessEqual(latency, 2.0)
        self.assertNotIn("latency_clamped", rec.json_fields)

        golden = json.loads(
            (_GOLDEN_DIR / "chunk_ingested.json").read_text(),
        )
        self.assertEqual(
            set(rec.json_fields.keys()),
            set(golden["expected_keys"]),
        )

    async def test_emits_clamped_when_now_earlier_than_receipt(self) -> None:
        """Future-stamped receipt (raw latency < 0) -> 5-key payload with clamp."""
        # 10 s in the future -> raw latency is negative, clamp fires.
        future_receipt = datetime.datetime.now(
            datetime.UTC,
        ) + datetime.timedelta(seconds=10)
        chunk = _make_chunk(future_receipt)

        records = await self._emit_records(chunk)

        self.assertEqual(len(records), 1)
        rec = cast("Any", records[0])
        self.assertEqual(rec.json_fields["processing_latency_sec"], 0.0)
        self.assertTrue(rec.json_fields["latency_clamped"])

        golden = json.loads(
            (_GOLDEN_DIR / "chunk_ingested_clamped.json").read_text(),
        )
        self.assertEqual(
            set(rec.json_fields.keys()),
            set(golden["expected_keys"]),
        )

    async def test_emits_without_latency_when_receipt_time_none(self) -> None:
        """Unstamped receipt -> 3-key payload; no latency fields at all."""
        chunk = _make_chunk(receipt_time=None)

        records = await self._emit_records(chunk)

        self.assertEqual(len(records), 1)
        rec = cast("Any", records[0])
        self.assertNotIn("processing_latency_sec", rec.json_fields)
        self.assertNotIn("latency_clamped", rec.json_fields)
        self.assertEqual(rec.json_fields["event_type"], "chunk_ingested")

        golden = json.loads(
            (_GOLDEN_DIR / "chunk_ingested_unstamped.json").read_text(),
        )
        self.assertEqual(
            set(rec.json_fields.keys()),
            set(golden["expected_keys"]),
        )

    async def test_no_emit_when_bookmark_fails(self) -> None:
        """Fence-violation branch runs instead of emit -> zero chunk_ingested records."""
        chunk = _make_chunk(datetime.datetime.now(datetime.UTC))

        records = await self._emit_records(chunk, bookmark_ok=False)

        # bookmark_ok=False -> fence-violation branch takes over BEFORE
        # the emit block is reached.
        self.assertEqual(records, [])

    async def test_no_emit_when_publish_fails_after_bookmark(self) -> None:
        """Post-bookmark publish failure records failure and skips emit."""
        chunk = _make_chunk(datetime.datetime.now(datetime.UTC))
        rt = _build_runtime_for_one_chunk(chunk, bookmark_ok=True)
        store = cast("Any", rt._store)
        store.report_feed_failure.return_value = "failing"

        with (
            _mock_upload_audio(),
            mock.patch(
                "backend.pipeline.ingestion.collector_runtime."
                "gcp_helper.publish_audio_chunk",
                mock.AsyncMock(side_effect=RuntimeError("pubsub boom")),
            ),
            self.assertLogs(
                "backend.pipeline.ingestion.collector_runtime",
                level=logging.INFO,
            ) as cm,
        ):
            await rt._process_feed(_FEED)

        store.update_feed_progress.assert_awaited_once()
        store.report_feed_failure.assert_awaited_once()
        kwargs = store.report_feed_failure.await_args.kwargs
        self.assertEqual(kwargs["reason"], "pubsub_publish_failed")
        self.assertIs(
            kwargs["status_reason"],
            FeedStatusReason.SYSTEM_PIPELINE_ERROR,
        )
        records = [
            record
            for record in cm.records
            if record.getMessage() == "Chunk ingested"
        ]
        self.assertEqual(records, [])


if __name__ == "__main__":
    unittest.main()
