"""Tests for the Echo audio ingestion handler."""

from __future__ import annotations

import io
import shutil
import uuid
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest
from pydub import AudioSegment

from backend.pipeline.ingestion.collectors.echo.main import (
    _convert_to_flac,
    _handle,
    _parse_timestamp,
)

_FAKE_FLAC = b"fLaC" + b"\x00" * 64
_ffmpeg_available = shutil.which("ffmpeg") is not None


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
# _convert_to_flac
# ---------------------------------------------------------------------------
@pytest.mark.skipif(not _ffmpeg_available, reason="ffmpeg not available")
class TestConvertToFlac:
    def _make_mp3_bytes(
        self, *, sample_rate: int = 8000, duration_ms: int = 1000
    ) -> bytes:
        """Generate a minimal MP3 for testing."""
        audio = AudioSegment.silent(
            duration=duration_ms, frame_rate=sample_rate
        )
        buf = io.BytesIO()
        audio.export(buf, format="mp3")
        return buf.getvalue()

    def test_converts_to_flac(self) -> None:
        mp3_bytes = self._make_mp3_bytes()
        flac_bytes = _convert_to_flac(mp3_bytes)
        audio = AudioSegment.from_file(io.BytesIO(flac_bytes), format="flac")
        assert audio.frame_rate == 16000
        assert audio.channels == 1
        assert audio.sample_width == 2

    def test_upsamples_from_8khz(self) -> None:
        mp3_bytes = self._make_mp3_bytes(sample_rate=8000)
        flac_bytes = _convert_to_flac(mp3_bytes)
        audio = AudioSegment.from_file(io.BytesIO(flac_bytes), format="flac")
        assert audio.frame_rate == 16000

    def test_output_is_valid_flac(self) -> None:
        mp3_bytes = self._make_mp3_bytes()
        flac_bytes = _convert_to_flac(mp3_bytes)
        assert len(flac_bytes) > 0
        assert flac_bytes[:4] == b"fLaC"


# ---------------------------------------------------------------------------
# _handle (sync handler)
# ---------------------------------------------------------------------------
class TestHandle:
    @pytest.fixture
    def mock_conn(self) -> MagicMock:
        """A mock psycopg connection supporting context manager protocol."""
        conn = MagicMock()
        conn.__enter__ = MagicMock(return_value=conn)
        conn.__exit__ = MagicMock(return_value=False)
        cursor = MagicMock()
        conn.execute.return_value = cursor
        cursor.fetchone.return_value = None
        return conn

    @pytest.fixture
    def _patch_globals(self, mock_conn):
        """Patch global state used by _handle."""
        mock_publisher = MagicMock()
        mock_publisher.publish.return_value.result.return_value = "msg-id"

        with (
            patch(
                "backend.pipeline.ingestion.collectors.echo.main._connect_db",
                return_value=mock_conn,
            ),
            patch(
                "backend.pipeline.ingestion.collectors.echo.main.gcs_client"
            ) as mock_gcs,
            patch(
                "backend.pipeline.ingestion.collectors.echo.main.pubsub_client"
            ) as mock_pubsub,
            patch(
                "backend.pipeline.ingestion.collectors.echo.main._convert_to_flac",
                return_value=_FAKE_FLAC,
            ),
        ):
            mock_pubsub.get_publisher.return_value = mock_publisher
            mock_gcs.bucket.return_value.blob.return_value.download_as_bytes.return_value = b"mp3-placeholder"
            mock_gcs.bucket.return_value.blob.return_value.upload_from_string = MagicMock()

            yield {
                "conn": mock_conn,
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

    def _set_feed(self, mock_conn: MagicMock, feed: dict | None) -> None:
        """Configure mock_conn to return a feed row from the resolve query."""
        mock_conn.execute.return_value.fetchone.return_value = feed

    @pytest.mark.usefixtures("_patch_globals")
    def test_skips_non_mp3(self, mock_conn) -> None:
        event = self._make_event(name="fire-ca/20260326/notes.txt")
        _handle(event)
        mock_conn.execute.assert_not_called()

    @pytest.mark.usefixtures("_patch_globals")
    def test_skips_unknown_channel(self, mock_conn) -> None:
        self._set_feed(mock_conn, None)
        _handle(self._make_event())
        mock_conn.execute.assert_called_once()

    @pytest.mark.usefixtures("_patch_globals")
    def test_quarantined_feed_raises_for_retry(self, mock_conn) -> None:
        self._set_feed(
            mock_conn,
            {
                "id": uuid.uuid4(),
                "status": "quarantined",
                "failure_count": 5,
            },
        )
        with pytest.raises(RuntimeError, match="quarantined"):
            _handle(self._make_event())

    @pytest.mark.usefixtures("_patch_globals")
    def test_skips_deactivated_feed(self, mock_conn) -> None:
        self._set_feed(
            mock_conn,
            {
                "id": uuid.uuid4(),
                "status": "deactivated",
                "failure_count": 0,
            },
        )
        _handle(self._make_event())

    @pytest.mark.usefixtures("_patch_globals")
    def test_successful_processing(self, mock_conn, _patch_globals) -> None:
        feed_id = uuid.uuid4()
        self._set_feed(
            mock_conn,
            {
                "id": feed_id,
                "status": "active",
                "failure_count": 0,
            },
        )

        _handle(self._make_event())

        # Verify FLAC uploaded
        gcs = _patch_globals["gcs"]
        upload_call = (
            gcs.bucket.return_value.blob.return_value.upload_from_string
        )
        upload_call.assert_called_once()
        flac_bytes = upload_call.call_args[0][0]
        assert flac_bytes[:4] == b"fLaC"

        # Verify AudioChunk published
        pub = _patch_globals["publisher"]
        pub.publish.assert_called_once()
        call_kwargs = pub.publish.call_args.kwargs
        assert call_kwargs["feed_id"] == str(feed_id)
        assert call_kwargs["ordering_key"] == str(feed_id)
        assert call_kwargs["source_type"] == "echo"

        # Verify heartbeat written (second _connect_db call)
        assert mock_conn.execute.call_count >= 2
        heartbeat_sql = mock_conn.execute.call_args_list[-1][0][0]
        assert "last_heartbeat" in heartbeat_sql

    @pytest.mark.usefixtures("_patch_globals")
    def test_failure_records_in_db(self, mock_conn, _patch_globals) -> None:
        feed_id = uuid.uuid4()
        self._set_feed(
            mock_conn,
            {
                "id": feed_id,
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

        # Verify failure recorded — the last execute call should be the failure SQL
        last_sql = mock_conn.execute.call_args_list[-1][0][0]
        assert "failure_count + 1" in last_sql

    @pytest.mark.usefixtures("_patch_globals")
    def test_failure_recording_db_error_preserves_original(
        self, mock_conn, _patch_globals
    ) -> None:
        feed_id = uuid.uuid4()
        cursor = MagicMock()
        cursor.fetchone.return_value = {
            "id": feed_id,
            "status": "active",
            "failure_count": 0,
        }

        gcs = _patch_globals["gcs"]
        gcs.bucket.return_value.blob.return_value.download_as_bytes.side_effect = Exception(
            "Original error"
        )

        # First execute (feed resolution) succeeds, second (failure recording) fails
        mock_conn.execute.side_effect = [cursor, Exception("DB error")]

        with pytest.raises(Exception, match="Original error"):
            _handle(self._make_event())

    @pytest.mark.usefixtures("_patch_globals")
    def test_malformed_filename_skips_gracefully(self, mock_conn) -> None:
        self._set_feed(
            mock_conn,
            {
                "id": uuid.uuid4(),
                "status": "active",
                "failure_count": 0,
            },
        )

        _handle(self._make_event(name="fire-ca/20260326/badname.mp3"))

        # Only the resolve query — no heartbeat or failure recording
        assert mock_conn.execute.call_count == 1

    @pytest.mark.usefixtures("_patch_globals")
    def test_corrupt_audio_skips_gracefully(
        self, mock_conn, _patch_globals
    ) -> None:
        self._set_feed(
            mock_conn,
            {
                "id": uuid.uuid4(),
                "status": "active",
                "failure_count": 0,
            },
        )

        with patch(
            "backend.pipeline.ingestion.collectors.echo.main._convert_to_flac",
            side_effect=Exception("ffmpeg decode error"),
        ):
            _handle(self._make_event())

        # Only the resolve query — no failure recording
        assert mock_conn.execute.call_count == 1
        _patch_globals["publisher"].publish.assert_not_called()

    @pytest.mark.usefixtures("_patch_globals")
    def test_publish_failure_records_in_db(
        self, mock_conn, _patch_globals
    ) -> None:
        feed_id = uuid.uuid4()
        self._set_feed(
            mock_conn,
            {
                "id": feed_id,
                "status": "active",
                "failure_count": 0,
            },
        )

        pub = _patch_globals["publisher"]
        pub.publish.return_value.result.side_effect = Exception("Pub/Sub error")

        with pytest.raises(Exception, match="Pub/Sub error"):
            _handle(self._make_event())

        last_sql = mock_conn.execute.call_args_list[-1][0][0]
        assert "failure_count + 1" in last_sql
