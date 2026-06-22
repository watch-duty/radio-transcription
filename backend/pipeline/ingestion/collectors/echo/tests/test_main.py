"""Tests for the Echo audio ingestion handler."""

from __future__ import annotations

import logging
import os
import subprocess
import sys
import uuid
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest
from google.api_core.exceptions import NotFound

from backend.pipeline.ingestion.collectors.echo.main import (
    SEGMENTED_PUBSUB_TOPIC_PATH,
    _handle,
    _parse_timestamp,
)
from backend.pipeline.schema_types.segmented_audio_pb2 import SegmentedAudio
from backend.pipeline.storage.feed_store import FeedStatus, FeedStatusReason
from backend.pipeline.storage.sync_feed_store import SyncFeedStore


# ---------------------------------------------------------------------------
# _parse_timestamp
# ---------------------------------------------------------------------------
class TestParseTimestamp:
    def test_standard_path(self) -> None:
        name = "fire-ca_almaden_valley/20260326/fire_20260326_143022.mp3"
        result = _parse_timestamp(name)
        assert result == datetime(2026, 3, 26, 14, 30, 22, tzinfo=UTC)

    def test_channel_with_underscores(self) -> None:
        name = (
            "fire_station-ca_almaden/20260326/fire_station_20260326_090000.mp3"
        )
        result = _parse_timestamp(name)
        assert result == datetime(2026, 3, 26, 9, 0, 0, tzinfo=UTC)

    def test_midnight(self) -> None:
        name = "ch-loc/20260101/ch_20260101_000000.mp3"
        result = _parse_timestamp(name)
        assert result == datetime(2026, 1, 1, 0, 0, 0, tzinfo=UTC)

    def test_timezone_is_utc(self) -> None:
        name = "ch-loc/20260326/ch_20260326_143022.mp3"
        result = _parse_timestamp(name)
        assert result.tzinfo is UTC

    def test_malformed_filename_too_few_parts(self) -> None:
        name = "ch-loc/20260326/badname.mp3"
        with pytest.raises(ValueError, match="Cannot parse timestamp"):
            _parse_timestamp(name)

    def test_malformed_filename_bad_date(self) -> None:
        name = "ch-loc/20260326/ch_notadate_143022.mp3"
        with pytest.raises(ValueError, match="does not match format"):
            _parse_timestamp(name)


# ---------------------------------------------------------------------------
# _handle (sync handler)
# ---------------------------------------------------------------------------
class TestHandle:
    @pytest.fixture
    def mock_store(self) -> MagicMock:
        """A mock SyncFeedStore."""
        store = MagicMock(spec=SyncFeedStore)
        store.resolve_echo_feed.return_value = None
        return store

    @pytest.fixture
    def _patch_globals(self, mock_store):
        """Patch global state used by _handle."""
        mock_publisher = MagicMock()
        mock_publisher.publish.return_value.result.return_value = "msg-id"

        with (
            patch(
                "backend.pipeline.ingestion.collectors.echo.main.feed_store",
                mock_store,
            ),
            patch(
                "backend.pipeline.ingestion.collectors.echo.main.gcs_client"
            ) as mock_gcs,
            patch(
                "backend.pipeline.ingestion.collectors.echo.main.pubsub_client"
            ) as mock_pubsub,
            patch(
                "backend.pipeline.ingestion.collectors.echo.main.get_audio_duration"
            ) as mock_get_duration,
        ):
            mock_pubsub.get_publisher.return_value = mock_publisher
            mock_gcs.bucket.return_value.blob.return_value.download_as_bytes.return_value = b"mp3-placeholder"
            mock_gcs.bucket.return_value.blob.return_value.upload_from_string = MagicMock()
            mock_get_duration.return_value = 15000

            yield {
                "store": mock_store,
                "gcs": mock_gcs,
                "pubsub": mock_pubsub,
                "publisher": mock_publisher,
                "get_duration": mock_get_duration,
            }

    def _make_event(
        self, name: str = "fire-ca/20260326/fire_20260326_143022.mp3"
    ) -> MagicMock:
        event = MagicMock()
        event.data = {
            "name": name,
            "bucket": "wd-echo-recordings-prod",
        }
        return event

    def _set_feed(self, mock_store: MagicMock, feed: dict | None) -> None:
        """Configure mock_store to return a feed row from resolve."""
        if feed is not None and "created_at" not in feed:
            feed = {**feed, "created_at": datetime(2026, 1, 1, tzinfo=UTC)}
        mock_store.resolve_echo_feed.return_value = feed

    def _assert_failure_recorded(
        self,
        mock_store: MagicMock,
        feed_id: uuid.UUID,
        *,
        reason: str,
        status_reason: FeedStatusReason,
    ) -> None:
        mock_store.record_failure.assert_called_once_with(
            feed_id,
            reason=reason,
            status_reason=status_reason,
        )

    def _assert_non_budgeted_failure_recorded(
        self,
        mock_store: MagicMock,
        feed_id: uuid.UUID,
        *,
        status_reason: FeedStatusReason,
    ) -> None:
        mock_store.record_non_budgeted_failure.assert_called_once_with(
            feed_id,
            status_reason=status_reason,
        )
        mock_store.record_failure.assert_not_called()

    @pytest.mark.usefixtures("_patch_globals")
    def test_skips_non_mp3(self, mock_store) -> None:
        event = self._make_event(name="fire-ca/20260326/notes.txt")
        _handle(event)
        mock_store.resolve_echo_feed.assert_not_called()
        mock_store.record_failure.assert_not_called()
        mock_store.record_non_budgeted_failure.assert_not_called()
        mock_store.record_heartbeat.assert_not_called()

    @pytest.mark.usefixtures("_patch_globals")
    def test_skips_unexpected_path_structure(self, mock_store) -> None:
        event = self._make_event(name="fire-ca/fire_20260326_143022.mp3")

        _handle(event)

        mock_store.resolve_echo_feed.assert_not_called()
        mock_store.record_failure.assert_not_called()
        mock_store.record_non_budgeted_failure.assert_not_called()
        mock_store.record_heartbeat.assert_not_called()

    @pytest.mark.usefixtures("_patch_globals")
    def test_skips_unknown_channel(self, mock_store) -> None:
        self._set_feed(mock_store, None)
        _handle(self._make_event())
        mock_store.resolve_echo_feed.assert_called_once()
        mock_store.record_failure.assert_not_called()
        mock_store.record_non_budgeted_failure.assert_not_called()
        mock_store.record_heartbeat.assert_not_called()

    @pytest.mark.usefixtures("_patch_globals")
    def test_quarantined_feed_drops_event(self, mock_store) -> None:
        self._set_feed(
            mock_store,
            {
                "id": uuid.uuid4(),
                "name": "Central Fire",
                "status": FeedStatus.QUARANTINED,
            },
        )
        _handle(self._make_event())
        mock_store.record_heartbeat.assert_not_called()
        mock_store.record_failure.assert_not_called()
        mock_store.record_non_budgeted_failure.assert_not_called()

    @pytest.mark.usefixtures("_patch_globals")
    def test_skips_deactivated_feed(self, mock_store) -> None:
        self._set_feed(
            mock_store,
            {
                "id": uuid.uuid4(),
                "name": "Central Fire",
                "status": FeedStatus.DEACTIVATED,
            },
        )
        _handle(self._make_event())
        mock_store.record_heartbeat.assert_not_called()
        mock_store.record_failure.assert_not_called()
        mock_store.record_non_budgeted_failure.assert_not_called()

    @pytest.mark.usefixtures("_patch_globals")
    def test_successful_processing(self, mock_store, _patch_globals) -> None:
        feed_id = uuid.uuid4()
        self._set_feed(
            mock_store,
            {
                "id": feed_id,
                "name": "Central Fire",
                "status": FeedStatus.ACTIVE,
            },
        )

        _handle(self._make_event())

        # Verify MP3 uploaded
        gcs = _patch_globals["gcs"]
        upload_call = (
            gcs.bucket.return_value.blob.return_value.upload_from_string
        )
        upload_call.assert_called_once()
        uploaded_bytes = upload_call.call_args[0][0]
        assert uploaded_bytes == b"mp3-placeholder"

        # Verify AudioChunk published
        pub = _patch_globals["publisher"]
        pub.publish.assert_called_once()
        publish_args, call_kwargs = pub.publish.call_args
        assert publish_args[0] == SEGMENTED_PUBSUB_TOPIC_PATH
        assert call_kwargs["source_type"] == "echo"
        chunk = SegmentedAudio()
        chunk.ParseFromString(publish_args[1])
        assert chunk.feed_id == str(feed_id)

        # Verify heartbeat recorded
        mock_store.record_heartbeat.assert_called_once_with(feed_id)
        _patch_globals["get_duration"].assert_called_once_with(
            b"mp3-placeholder",
            input_format="mp3",
        )

    @pytest.mark.usefixtures("_patch_globals")
    def test_drops_historical_recording(
        self, mock_store, _patch_globals, caplog
    ) -> None:
        feed_id = uuid.uuid4()
        self._set_feed(
            mock_store,
            {
                "id": feed_id,
                "name": "Central Fire",
                "status": FeedStatus.ACTIVE,
                "created_at": datetime(2026, 3, 27, tzinfo=UTC),
            },
        )
        caplog.set_level(
            logging.INFO,
            logger="backend.pipeline.ingestion.collectors.echo.main",
        )
        _handle(self._make_event())
        mock_store.record_heartbeat.assert_not_called()
        _patch_globals["publisher"].publish.assert_not_called()
        assert "Skipping historical Echo recording" in caplog.text

    @pytest.mark.usefixtures("_patch_globals")
    def test_does_not_drop_same_second_recording(
        self, mock_store, _patch_globals, caplog
    ) -> None:
        feed_id = uuid.uuid4()
        self._set_feed(
            mock_store,
            {
                "id": feed_id,
                "name": "Central Fire",
                "status": FeedStatus.ACTIVE,
                "created_at": datetime(
                    2026, 3, 26, 14, 30, 22, 123456, tzinfo=UTC
                ),
            },
        )
        caplog.set_level(
            logging.INFO,
            logger="backend.pipeline.ingestion.collectors.echo.main",
        )
        # Event is at 2026-03-26 14:30:22 (which matches the feed's created_at second)
        _handle(self._make_event("fire-ca/20260326/fire_20260326_143022.mp3"))
        # It should not be dropped, so it should attempt to publish
        _patch_globals["publisher"].publish.assert_called_once()
        assert "Skipping historical Echo recording" not in caplog.text

    @pytest.mark.usefixtures("_patch_globals")
    def test_filename_timestamp_wins_over_gcs_time_created(
        self, mock_store, _patch_globals
    ) -> None:
        """Verifies Echo recording time comes from the filename, not upload time."""
        feed_id = uuid.uuid4()
        self._set_feed(
            mock_store,
            {
                "id": feed_id,
                "name": "Central Fire",
                "status": FeedStatus.ACTIVE,
            },
        )

        event = self._make_event(
            name="fire-ca/20260531/Middlebury_Regional_EMS_20260531_002818.mp3"
        )
        event.data["timeCreated"] = "2026-05-31T01:03:57.708000Z"

        _handle(event)

        pub = _patch_globals["publisher"]
        pub.publish.assert_called_once()
        publish_args, _ = pub.publish.call_args
        chunk = SegmentedAudio()
        chunk.ParseFromString(publish_args[1])

        expected_ts = datetime(2026, 5, 31, 0, 28, 18, tzinfo=UTC)
        assert chunk.start_timestamp.ToDatetime(UTC) == expected_ts

    @pytest.mark.usefixtures("_patch_globals")
    def test_gcs_time_created_fallback_for_unparseable_filename(
        self, mock_store, _patch_globals
    ) -> None:
        """Verifies GCS time is only a fallback when the filename lacks a timestamp."""
        feed_id = uuid.uuid4()
        self._set_feed(
            mock_store,
            {
                "id": feed_id,
                "name": "Central Fire",
                "status": FeedStatus.ACTIVE,
            },
        )

        event = self._make_event(name="fire-ca/20260326/badname.mp3")
        event.data["timeCreated"] = "2026-05-19T16:48:12.184784Z"

        _handle(event)

        pub = _patch_globals["publisher"]
        pub.publish.assert_called_once()
        publish_args, _ = pub.publish.call_args
        chunk = SegmentedAudio()
        chunk.ParseFromString(publish_args[1])

        expected_ts = datetime(2026, 5, 19, 16, 47, 57, 184784, tzinfo=UTC)
        assert chunk.start_timestamp.ToDatetime(UTC) == expected_ts

    @pytest.mark.usefixtures("_patch_globals")
    def test_gcs_not_found_skips_gracefully(
        self, mock_store, _patch_globals
    ) -> None:
        feed_id = uuid.uuid4()
        self._set_feed(
            mock_store,
            {
                "id": feed_id,
                "name": "Central Fire",
                "status": FeedStatus.ACTIVE,
            },
        )

        gcs = _patch_globals["gcs"]
        gcs.bucket.return_value.blob.return_value.download_as_bytes.side_effect = NotFound(
            "Object deleted"
        )

        _handle(self._make_event())

        mock_store.record_heartbeat.assert_not_called()
        mock_store.record_failure.assert_not_called()
        mock_store.record_non_budgeted_failure.assert_not_called()
        _patch_globals["publisher"].publish.assert_not_called()

    @pytest.mark.usefixtures("_patch_globals")
    def test_download_failure_records_non_budgeted_status(
        self, mock_store, _patch_globals, caplog
    ) -> None:
        feed_id = uuid.uuid4()
        self._set_feed(
            mock_store,
            {
                "id": feed_id,
                "name": "Central Fire",
                "status": FeedStatus.ACTIVE,
            },
        )

        gcs = _patch_globals["gcs"]
        gcs.bucket.return_value.blob.return_value.download_as_bytes.side_effect = Exception(
            "GCS error"
        )
        caplog.set_level(
            logging.ERROR,
            logger="backend.pipeline.ingestion.collectors.echo.main",
        )

        _handle(self._make_event())

        self._assert_non_budgeted_failure_recorded(
            mock_store,
            feed_id,
            status_reason=FeedStatusReason.SYSTEM_PIPELINE_ERROR,
        )
        mock_store.record_heartbeat.assert_not_called()
        _patch_globals["publisher"].publish.assert_not_called()
        assert "will return success for object notification" in caplog.text
        assert "echo_recording_download_failed" in caplog.text
        assert "GCS error" in caplog.text
        assert "Traceback" in caplog.text

    @pytest.mark.usefixtures("_patch_globals")
    def test_duration_failure_records_collector_reason(
        self, mock_store, _patch_globals, caplog
    ) -> None:
        feed_id = uuid.uuid4()
        self._set_feed(
            mock_store,
            {
                "id": feed_id,
                "name": "Central Fire",
                "external_id": "ext-id",
                "status": FeedStatus.ACTIVE,
            },
        )
        expected_reason = (
            "ffprobe exited with code 1; "
            "Invalid data found when processing input"
        )
        _patch_globals[
            "get_duration"
        ].side_effect = subprocess.CalledProcessError(
            1,
            ["ffprobe"],
            stderr=b"Invalid data found when processing input\n",
        )
        caplog.set_level(
            logging.WARNING,
            logger="backend.pipeline.ingestion.collectors.echo.main",
        )

        _handle(self._make_event())

        self._assert_non_budgeted_failure_recorded(
            mock_store,
            feed_id,
            status_reason=FeedStatusReason.SYSTEM_COLLECTOR_ERROR,
        )
        mock_store.record_heartbeat.assert_not_called()
        _patch_globals["publisher"].publish.assert_not_called()
        assert expected_reason in caplog.text
        assert "will return success for object notification" in caplog.text
        assert "Traceback" in caplog.text

    @pytest.mark.usefixtures("_patch_globals")
    def test_duration_failure_records_generic_exception_reason(
        self, mock_store, _patch_globals, caplog
    ) -> None:
        feed_id = uuid.uuid4()
        self._set_feed(
            mock_store,
            {
                "id": feed_id,
                "name": "Central Fire",
                "external_id": "ext-id",
                "status": FeedStatus.ACTIVE,
            },
        )
        expected_reason = "ValueError: bad mp3"
        _patch_globals["get_duration"].side_effect = ValueError("bad mp3")
        caplog.set_level(
            logging.WARNING,
            logger="backend.pipeline.ingestion.collectors.echo.main",
        )

        _handle(self._make_event())

        self._assert_non_budgeted_failure_recorded(
            mock_store,
            feed_id,
            status_reason=FeedStatusReason.SYSTEM_COLLECTOR_ERROR,
        )
        mock_store.record_heartbeat.assert_not_called()
        _patch_globals["publisher"].publish.assert_not_called()
        assert expected_reason in caplog.text
        assert "will return success for object notification" in caplog.text
        assert "Traceback" in caplog.text

    @pytest.mark.usefixtures("_patch_globals")
    def test_staging_upload_failure_records_pipeline_reason(
        self, mock_store, _patch_globals
    ) -> None:
        feed_id = uuid.uuid4()
        self._set_feed(
            mock_store,
            {
                "id": feed_id,
                "name": "Central Fire",
                "external_id": "ext-id",
                "status": FeedStatus.ACTIVE,
            },
        )
        upload_call = _patch_globals[
            "gcs"
        ].bucket.return_value.blob.return_value.upload_from_string
        upload_call.side_effect = Exception("upload error")

        _handle(self._make_event())

        self._assert_non_budgeted_failure_recorded(
            mock_store,
            feed_id,
            status_reason=FeedStatusReason.SYSTEM_PIPELINE_ERROR,
        )
        mock_store.record_heartbeat.assert_not_called()
        _patch_globals["publisher"].publish.assert_not_called()

    @pytest.mark.usefixtures("_patch_globals")
    def test_non_budgeted_recording_db_error_keeps_return_success_policy(
        self, mock_store, _patch_globals
    ) -> None:
        feed_id = uuid.uuid4()
        self._set_feed(
            mock_store,
            {
                "id": feed_id,
                "name": "Central Fire",
                "status": FeedStatus.ACTIVE,
            },
        )

        gcs = _patch_globals["gcs"]
        gcs.bucket.return_value.blob.return_value.download_as_bytes.side_effect = Exception(
            "Original error"
        )

        mock_store.record_non_budgeted_failure.side_effect = Exception(
            "DB error"
        )

        with patch(
            "backend.pipeline.ingestion.collectors.echo.main.logger"
        ) as mock_logger:
            _handle(self._make_event())

        self._assert_non_budgeted_failure_recorded(
            mock_store,
            feed_id,
            status_reason=FeedStatusReason.SYSTEM_PIPELINE_ERROR,
        )
        assert mock_logger.exception.call_count == 2
        first_call, second_call = mock_logger.exception.call_args_list
        log_args, log_kwargs = first_call
        assert log_args[:4] == (
            "Echo processing failure will return success for "
            "object notification: "
            "feed=%s status_reason=%s reason=%s",
            feed_id,
            "system_pipeline_error",
            "echo_recording_download_failed",
        )
        assert log_kwargs == {}
        second_args, second_kwargs = second_call
        assert second_args == ("Failed to record failure for feed %s", feed_id)
        assert second_kwargs == {}

    @pytest.mark.usefixtures("_patch_globals")
    def test_malformed_filename_skips_gracefully(self, mock_store) -> None:
        self._set_feed(
            mock_store,
            {
                "id": uuid.uuid4(),
                "name": "Central Fire",
                "status": FeedStatus.ACTIVE,
            },
        )

        _handle(self._make_event(name="fire-ca/20260326/badname.mp3"))

        # Only the resolve query — no heartbeat or failure recording
        mock_store.resolve_echo_feed.assert_called_once()
        mock_store.record_heartbeat.assert_not_called()
        mock_store.record_failure.assert_not_called()
        mock_store.record_non_budgeted_failure.assert_not_called()

    @pytest.mark.usefixtures("_patch_globals")
    def test_malformed_filename_with_invalid_gcs_time_skips_gracefully(
        self, mock_store, _patch_globals
    ) -> None:
        self._set_feed(
            mock_store,
            {
                "id": uuid.uuid4(),
                "name": "Central Fire",
                "status": FeedStatus.ACTIVE,
            },
        )
        event = self._make_event(name="fire-ca/20260326/badname.mp3")
        event.data["timeCreated"] = "not-a-timestamp"

        _handle(event)

        mock_store.record_heartbeat.assert_not_called()
        mock_store.record_failure.assert_not_called()
        mock_store.record_non_budgeted_failure.assert_not_called()
        _patch_globals["publisher"].publish.assert_not_called()

    @pytest.mark.usefixtures("_patch_globals")
    def test_publish_failure_records_in_db(
        self, mock_store, _patch_globals
    ) -> None:
        feed_id = uuid.uuid4()
        self._set_feed(
            mock_store,
            {
                "id": feed_id,
                "name": "Central Fire",
                "status": FeedStatus.ACTIVE,
            },
        )

        pub = _patch_globals["publisher"]
        pub.publish.return_value.result.side_effect = Exception("Pub/Sub error")

        _handle(self._make_event())

        self._assert_non_budgeted_failure_recorded(
            mock_store,
            feed_id,
            status_reason=FeedStatusReason.SYSTEM_PIPELINE_ERROR,
        )
        mock_store.record_heartbeat.assert_not_called()

    @pytest.mark.usefixtures("_patch_globals")
    def test_publisher_factory_failure_records_pipeline_reason(
        self, mock_store, _patch_globals
    ) -> None:
        feed_id = uuid.uuid4()
        self._set_feed(
            mock_store,
            {
                "id": feed_id,
                "name": "Central Fire",
                "external_id": "ext-id",
                "status": FeedStatus.ACTIVE,
            },
        )
        _patch_globals["pubsub"].get_publisher.side_effect = Exception(
            "publisher error"
        )

        _handle(self._make_event())

        self._assert_non_budgeted_failure_recorded(
            mock_store,
            feed_id,
            status_reason=FeedStatusReason.SYSTEM_PIPELINE_ERROR,
        )
        mock_store.record_heartbeat.assert_not_called()

    @pytest.mark.usefixtures("_patch_globals")
    def test_heartbeat_failure_records_pipeline_reason(
        self, mock_store
    ) -> None:
        feed_id = uuid.uuid4()
        self._set_feed(
            mock_store,
            {
                "id": feed_id,
                "name": "Central Fire",
                "external_id": "ext-id",
                "status": FeedStatus.ACTIVE,
            },
        )
        mock_store.record_heartbeat.side_effect = Exception("heartbeat error")

        _handle(self._make_event())

        self._assert_non_budgeted_failure_recorded(
            mock_store,
            feed_id,
            status_reason=FeedStatusReason.SYSTEM_PIPELINE_ERROR,
        )

    @pytest.mark.usefixtures("_patch_globals")
    def test_unwrapped_bug_records_unexpected_reason(self, mock_store) -> None:
        feed_id = uuid.uuid4()
        self._set_feed(
            mock_store,
            {
                "id": feed_id,
                "name": "Central Fire",
                "external_id": "ext-id",
                "status": FeedStatus.ACTIVE,
            },
        )

        with patch(
            "backend.pipeline.ingestion.collectors.echo.main._parse_timestamp",
            side_effect=RuntimeError("unexpected bug"),
        ):
            with pytest.raises(RuntimeError, match="unexpected bug"):
                _handle(self._make_event())

        self._assert_non_budgeted_failure_recorded(
            mock_store,
            feed_id,
            status_reason=FeedStatusReason.SYSTEM_UNEXPECTED_ERROR,
        )

    @pytest.mark.usefixtures("_patch_globals")
    def test_unwrapped_bug_reraises_after_non_budgeted_record(
        self, mock_store
    ) -> None:
        feed_id = uuid.uuid4()
        self._set_feed(
            mock_store,
            {
                "id": feed_id,
                "name": "Central Fire",
                "external_id": "ext-id",
                "status": FeedStatus.ACTIVE,
            },
        )
        message = "token=secret-value " + ("x" * 300)

        with patch(
            "backend.pipeline.ingestion.collectors.echo.main._parse_timestamp",
            side_effect=RuntimeError(message),
        ):
            with pytest.raises(RuntimeError, match=message):
                _handle(self._make_event())

        self._assert_non_budgeted_failure_recorded(
            mock_store,
            feed_id,
            status_reason=FeedStatusReason.SYSTEM_UNEXPECTED_ERROR,
        )

    # -----------------------------------------------------------------
    # Dual-write to dev recordings bucket (best-effort mirror)
    # -----------------------------------------------------------------
    def _active_feed(self) -> dict:
        return {
            "id": uuid.uuid4(),
            "name": "Central Fire",
            "status": FeedStatus.ACTIVE,
        }

    @pytest.mark.usefixtures("_patch_globals")
    def test_dual_write_disabled_when_env_unset(
        self, mock_store, _patch_globals
    ) -> None:
        self._set_feed(mock_store, self._active_feed())

        _handle(self._make_event())

        # DEV_RECORDINGS_BUCKET is None by default (conftest does not set it),
        # so copy_blob must never be invoked.
        gcs = _patch_globals["gcs"]
        gcs.bucket.return_value.copy_blob.assert_not_called()

    @pytest.mark.usefixtures("_patch_globals")
    def test_dual_write_enabled_when_env_set(
        self, mock_store, _patch_globals
    ) -> None:
        self._set_feed(mock_store, self._active_feed())

        with patch(
            "backend.pipeline.ingestion.collectors.echo.main.DEV_RECORDINGS_BUCKET",
            "wd-echo-recordings-dev",
        ):
            _handle(self._make_event())

        gcs = _patch_globals["gcs"]
        gcs.bucket.return_value.copy_blob.assert_called_once()
        # Third positional arg to copy_blob is the new_name; verify it
        # matches the source object name (we preserve the path).
        copy_args = gcs.bucket.return_value.copy_blob.call_args
        assert copy_args[0][2] == "fire-ca/20260326/fire_20260326_143022.mp3"
        # timeout kwarg is set so the rewrite cannot hang past the
        # Cloud Run request deadline.
        assert copy_args.kwargs["timeout"] == 30

    @pytest.mark.usefixtures("_patch_globals")
    def test_dual_write_failure_does_not_fail_handler(
        self, mock_store, _patch_globals
    ) -> None:
        feed = self._active_feed()
        self._set_feed(mock_store, feed)

        # Capture call order across both mocks so the heartbeat-before-mirror
        # invariant is verified explicitly — guards against a future refactor
        # that swaps the two lines and silently suppresses the heartbeat.
        call_order: list[str] = []
        mock_store.record_heartbeat.side_effect = lambda _fid: (
            call_order.append("heartbeat")
        )

        gcs = _patch_globals["gcs"]

        def _failing_copy(*_args: object, **_kw: object) -> None:
            call_order.append("copy_blob")
            msg = "Cross-project IAM denied"
            raise RuntimeError(msg)

        gcs.bucket.return_value.copy_blob.side_effect = _failing_copy

        with patch(
            "backend.pipeline.ingestion.collectors.echo.main.DEV_RECORDINGS_BUCKET",
            "wd-echo-recordings-dev",
        ):
            # Must complete without raising — mirror failure is best-effort.
            _handle(self._make_event())

        # Ordering invariant: heartbeat fires first; mirror is best-effort after.
        assert call_order == ["heartbeat", "copy_blob"], call_order
        # Prod path completed (heartbeat recorded) and the feed was NOT
        # punished for the dev-side failure.
        mock_store.record_heartbeat.assert_called_once_with(feed["id"])
        mock_store.record_failure.assert_not_called()
        mock_store.record_non_budgeted_failure.assert_not_called()


# ---------------------------------------------------------------------------
# Module-import fail-fast (D-03)
# ---------------------------------------------------------------------------
class TestModuleImportFailFast:
    """Verify echo.main raises at import when required env is missing.

    Uses a fresh `subprocess` rather than re-importing the module in-process:
    `setup_logging()` registers a global OpenTelemetry TracerProvider, which
    cannot be cleanly torn down between in-process imports. A subprocess gives
    a clean import-from-scratch and avoids "Overriding of current
    TracerProvider" warnings polluting test output.
    """

    def test_module_import_raises_on_missing_env(self) -> None:
        # Inherit the parent process's environment (so PYTHONPATH,
        # VIRTUAL_ENV, HOME, locale settings, etc. are preserved and the
        # subprocess can find the package), then drop ONLY the env vars
        # this test wants to verify the module fail-fasts on. conftest.py
        # sets these for the in-process test suite; popping them in the
        # spawned subprocess ensures _require_env raises.
        clean_env = os.environ.copy()
        clean_env.pop("AUDIO_STAGING_BUCKET", None)
        clean_env.pop("SEGMENTED_PUBSUB_TOPIC_PATH", None)

        result = subprocess.run(
            [
                sys.executable,
                "-c",
                "import backend.pipeline.ingestion.collectors.echo.main",
            ],
            env=clean_env,
            capture_output=True,
            text=True,
            check=False,
        )
        assert result.returncode != 0, (
            f"expected non-zero exit, got {result.returncode}; "
            f"stdout={result.stdout!r}; stderr={result.stderr!r}"
        )
        # First _require_env call in main.py is AUDIO_STAGING_BUCKET, so the
        # first ValueError surfaces the former.
        assert "AUDIO_STAGING_BUCKET" in result.stderr, (
            f"expected AUDIO_STAGING_BUCKET in stderr; got {result.stderr!r}"
        )
        assert "Required environment variable" in result.stderr, (
            f"expected canonical _require_env message; got {result.stderr!r}"
        )

    def test_module_import_accepts_segmented_topic(self) -> None:
        clean_env = os.environ.copy()
        clean_env["AUDIO_STAGING_BUCKET"] = "test-staging-bucket"
        clean_env["SEGMENTED_PUBSUB_TOPIC_PATH"] = (
            "projects/test/topics/segmented-audio-test"
        )

        result = subprocess.run(
            [
                sys.executable,
                "-c",
                "import backend.pipeline.ingestion.collectors.echo.main",
            ],
            env=clean_env,
            capture_output=True,
            text=True,
            check=False,
        )

        assert result.returncode == 0, (
            f"expected import to succeed; "
            f"stdout={result.stdout!r}; stderr={result.stderr!r}"
        )
