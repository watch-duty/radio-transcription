"""Tests for the Echo audio ingestion Cloud Function."""

from __future__ import annotations

import asyncio
import io
import shutil
import uuid
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

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
        # FLAC magic number: "fLaC"
        assert flac_bytes[:4] == b"fLaC"


# ---------------------------------------------------------------------------
# _handle (async handler)
# ---------------------------------------------------------------------------
class TestHandle:
    @pytest.fixture
    def mock_pool(self) -> AsyncMock:
        return AsyncMock()

    @pytest.fixture
    def _patch_globals(self, mock_pool):
        """Patch global state used by _handle."""
        with (
            patch(
                "backend.pipeline.ingestion.collectors.echo.main._get_pool",
                new_callable=AsyncMock,
                return_value=mock_pool,
            ),
            patch(
                "backend.pipeline.ingestion.collectors.echo.main.gcs_client"
            ) as mock_gcs,
            patch(
                "backend.pipeline.ingestion.collectors.echo.main.publisher"
            ) as mock_pub,
            patch(
                "backend.pipeline.ingestion.collectors.echo.main._convert_to_flac",
                return_value=_FAKE_FLAC,
            ),
        ):
            mock_gcs.bucket.return_value.blob.return_value.download_as_bytes.return_value = b"mp3-placeholder"
            mock_gcs.bucket.return_value.blob.return_value.upload_from_string = MagicMock()

            yield {
                "pool": mock_pool,
                "gcs": mock_gcs,
                "publisher": mock_pub,
            }

    def _make_event(
        self, name: str = "fire-ca/20260326/fire_20260326_143022.mp3"
    ):
        event = MagicMock()
        event.data = {
            "name": name,
            "bucket": "wd-echo-recordings-prod",
        }
        return event

    @pytest.mark.usefixtures("_patch_globals")
    def test_skips_non_mp3(self, mock_pool) -> None:
        event = self._make_event(name="fire-ca/20260326/notes.txt")
        asyncio.run(_handle(event))
        mock_pool.fetchrow.assert_not_called()

    @pytest.mark.usefixtures("_patch_globals")
    def test_skips_unknown_channel(self, mock_pool) -> None:
        mock_pool.fetchrow.return_value = None
        event = self._make_event()
        asyncio.run(_handle(event))
        mock_pool.fetchrow.assert_called_once()

    @pytest.mark.usefixtures("_patch_globals")
    def test_skips_quarantined_feed(self, mock_pool) -> None:
        mock_pool.fetchrow.return_value = {
            "id": uuid.uuid4(),
            "status": "quarantined",
            "failure_count": 5,
        }
        event = self._make_event()
        asyncio.run(_handle(event))

    @pytest.mark.usefixtures("_patch_globals")
    def test_skips_deactivated_feed(self, mock_pool) -> None:
        mock_pool.fetchrow.return_value = {
            "id": uuid.uuid4(),
            "status": "deactivated",
            "failure_count": 0,
        }
        event = self._make_event()
        asyncio.run(_handle(event))

    @pytest.mark.usefixtures("_patch_globals")
    def test_successful_processing(self, mock_pool, _patch_globals) -> None:
        feed_id = uuid.uuid4()
        mock_pool.fetchrow.return_value = {
            "id": feed_id,
            "status": "active",
            "failure_count": 0,
        }

        event = self._make_event()
        asyncio.run(_handle(event))

        # Verify FLAC uploaded to canonical bucket
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
        call_kwargs = pub.publish.call_args
        assert call_kwargs.kwargs["feed_id"] == str(feed_id)
        assert call_kwargs.kwargs["ordering_key"] == str(feed_id)
        assert call_kwargs.kwargs["source_type"] == "echo"

        # Verify conditional reset called
        mock_pool.execute.assert_called_once()

    @pytest.mark.usefixtures("_patch_globals")
    def test_failure_records_in_db(self, mock_pool, _patch_globals) -> None:
        feed_id = uuid.uuid4()
        mock_pool.fetchrow.return_value = {
            "id": feed_id,
            "status": "active",
            "failure_count": 0,
        }

        # Make GCS download fail
        gcs = _patch_globals["gcs"]
        gcs.bucket.return_value.blob.return_value.download_as_bytes.side_effect = Exception(
            "GCS error"
        )

        event = self._make_event()
        with pytest.raises(Exception, match="GCS error"):
            asyncio.run(_handle(event))

        # Verify failure recorded in DB
        mock_pool.execute.assert_called_once()
        sql = mock_pool.execute.call_args[0][0]
        assert "failure_count + 1" in sql

    @pytest.mark.usefixtures("_patch_globals")
    def test_failure_recording_db_error_preserves_original(
        self, mock_pool, _patch_globals
    ) -> None:
        feed_id = uuid.uuid4()
        mock_pool.fetchrow.return_value = {
            "id": feed_id,
            "status": "active",
            "failure_count": 0,
        }

        # Make GCS download fail
        gcs = _patch_globals["gcs"]
        gcs.bucket.return_value.blob.return_value.download_as_bytes.side_effect = Exception(
            "Original error"
        )
        # Make DB failure recording also fail
        mock_pool.execute.side_effect = Exception("DB error")

        event = self._make_event()
        with pytest.raises(Exception, match="Original error"):
            asyncio.run(_handle(event))

    @pytest.mark.usefixtures("_patch_globals")
    def test_malformed_filename_records_failure(
        self, mock_pool, _patch_globals
    ) -> None:
        feed_id = uuid.uuid4()
        mock_pool.fetchrow.return_value = {
            "id": feed_id,
            "status": "active",
            "failure_count": 0,
        }

        event = self._make_event(name="fire-ca/20260326/badname.mp3")
        with pytest.raises(ValueError, match="Cannot parse timestamp"):
            asyncio.run(_handle(event))

        # Verify failure recorded
        mock_pool.execute.assert_called_once()
