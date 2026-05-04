"""Tests for the Echo audio ingestion handler."""

from __future__ import annotations

import os
import subprocess
import sys
import uuid
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest
from google.api_core.exceptions import NotFound

from backend.pipeline.ingestion.collectors.echo.main import (
    _handle,
    _parse_timestamp,
)
from backend.pipeline.schema_types.raw_audio_chunk_pb2 import AudioChunk
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
        mock_store.resolve_echo_feed.return_value = feed

    @pytest.mark.usefixtures("_patch_globals")
    def test_skips_non_mp3(self, mock_store) -> None:
        event = self._make_event(name="fire-ca/20260326/notes.txt")
        _handle(event)
        mock_store.resolve_echo_feed.assert_not_called()

    @pytest.mark.usefixtures("_patch_globals")
    def test_skips_unknown_channel(self, mock_store) -> None:
        self._set_feed(mock_store, None)
        _handle(self._make_event())
        mock_store.resolve_echo_feed.assert_called_once()

    @pytest.mark.usefixtures("_patch_globals")
    def test_quarantined_feed_drops_event(self, mock_store) -> None:
        self._set_feed(
            mock_store,
            {
                "id": uuid.uuid4(),
                "name": "Central Fire",
                "external_id": "ext-id",
                "status": "quarantined",
                "failure_count": 5,
            },
        )
        _handle(self._make_event())
        mock_store.record_heartbeat.assert_not_called()
        mock_store.record_failure.assert_not_called()

    @pytest.mark.usefixtures("_patch_globals")
    def test_skips_deactivated_feed(self, mock_store) -> None:
        self._set_feed(
            mock_store,
            {
                "id": uuid.uuid4(),
                "name": "Central Fire",
                "external_id": "ext-id",
                "status": "deactivated",
                "failure_count": 0,
            },
        )
        _handle(self._make_event())

    @pytest.mark.usefixtures("_patch_globals")
    def test_successful_processing(self, mock_store, _patch_globals) -> None:
        feed_id = uuid.uuid4()
        self._set_feed(
            mock_store,
            {
                "id": feed_id,
                "name": "Central Fire",
                "external_id": "ext-id",
                "status": "active",
                "failure_count": 0,
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
        assert call_kwargs["source_type"] == "echo"
        chunk = AudioChunk()
        chunk.ParseFromString(publish_args[1])
        assert chunk.feed_id == str(feed_id)

        # Verify heartbeat recorded
        mock_store.record_heartbeat.assert_called_once_with(feed_id)

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
                "external_id": "ext-id",
                "status": "active",
                "failure_count": 0,
            },
        )

        gcs = _patch_globals["gcs"]
        gcs.bucket.return_value.blob.return_value.download_as_bytes.side_effect = NotFound(
            "Object deleted"
        )

        _handle(self._make_event())

        mock_store.record_heartbeat.assert_not_called()
        mock_store.record_failure.assert_not_called()
        _patch_globals["publisher"].publish.assert_not_called()

    @pytest.mark.usefixtures("_patch_globals")
    def test_failure_records_in_db(self, mock_store, _patch_globals) -> None:
        feed_id = uuid.uuid4()
        self._set_feed(
            mock_store,
            {
                "id": feed_id,
                "name": "Central Fire",
                "external_id": "ext-id",
                "status": "active",
                "failure_count": 0,
            },
        )

        gcs = _patch_globals["gcs"]
        gcs.bucket.return_value.blob.return_value.download_as_bytes.side_effect = Exception(
            "GCS error"
        )

        with pytest.raises(Exception, match="GCS error"):
            _handle(self._make_event())

        mock_store.record_failure.assert_called_once_with(feed_id)

    @pytest.mark.usefixtures("_patch_globals")
    def test_failure_recording_db_error_preserves_original(
        self, mock_store, _patch_globals
    ) -> None:
        feed_id = uuid.uuid4()
        self._set_feed(
            mock_store,
            {
                "id": feed_id,
                "name": "Central Fire",
                "external_id": "ext-id",
                "status": "active",
                "failure_count": 0,
            },
        )

        gcs = _patch_globals["gcs"]
        gcs.bucket.return_value.blob.return_value.download_as_bytes.side_effect = Exception(
            "Original error"
        )

        mock_store.record_failure.side_effect = Exception("DB error")

        with pytest.raises(Exception, match="Original error"):
            _handle(self._make_event())

    @pytest.mark.usefixtures("_patch_globals")
    def test_malformed_filename_skips_gracefully(self, mock_store) -> None:
        self._set_feed(
            mock_store,
            {
                "id": uuid.uuid4(),
                "name": "Central Fire",
                "external_id": "ext-id",
                "status": "active",
                "failure_count": 0,
            },
        )

        _handle(self._make_event(name="fire-ca/20260326/badname.mp3"))

        # Only the resolve query — no heartbeat or failure recording
        mock_store.resolve_echo_feed.assert_called_once()
        mock_store.record_heartbeat.assert_not_called()
        mock_store.record_failure.assert_not_called()

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
                "external_id": "ext-id",
                "status": "active",
                "failure_count": 0,
            },
        )

        pub = _patch_globals["publisher"]
        pub.publish.return_value.result.side_effect = Exception("Pub/Sub error")

        with pytest.raises(Exception, match="Pub/Sub error"):
            _handle(self._make_event())

        mock_store.record_failure.assert_called_once_with(feed_id)


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
        clean_env.pop("RAW_AUDIO_TOPIC", None)

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
        # First _require_env call in main.py is AUDIO_STAGING_BUCKET
        # (statement order is fixed by D-01 / plan 01-01 Task 1: the
        # declaration sequence at module top is AUDIO_STAGING_BUCKET then
        # RAW_AUDIO_TOPIC, so the first ValueError surfaces the former).
        assert "AUDIO_STAGING_BUCKET" in result.stderr, (
            f"expected AUDIO_STAGING_BUCKET in stderr; got {result.stderr!r}"
        )
        assert "Required environment variable" in result.stderr, (
            f"expected canonical _require_env message; got {result.stderr!r}"
        )
