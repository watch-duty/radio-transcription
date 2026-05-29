import datetime
import unittest
from unittest.mock import AsyncMock

from fastapi import status
from fastapi.testclient import TestClient

from backend.pipeline.common.auth import verify_oidc_token
from backend.services.audio_segments.main import app
from backend.services.audio_segments.models import (
    AudioClassification,
    AudioSegment,
)

_SEGMENT_ID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
_FEED_ID = "bbbbbbbb-cccc-dddd-eeee-ffffffffffff"


async def skip_auth() -> dict[str, str]:
    """Mock dependency to bypass authentication in tests."""
    return {"sub": "test@example.com", "email": "test@example.com"}


class TestAudioSegmentsAPI(unittest.TestCase):
    def setUp(self) -> None:
        """Set up a test client and dependency overrides before each test."""
        self.mock_service = AsyncMock()
        app.state.audio_segment_service = self.mock_service

        app.dependency_overrides[verify_oidc_token] = skip_auth
        self.client = TestClient(app)

    def tearDown(self) -> None:
        """Clean up after each test."""
        app.dependency_overrides.clear()

    def test_list_audio_segments_success(self) -> None:
        """Test listing audio segments successfully."""
        mock_segment = AudioSegment(
            id=_SEGMENT_ID,
            feed_id=_FEED_ID,
            classification=AudioClassification.SPEECH_DETECTED,
            start_timestamp=datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC),
            end_timestamp=datetime.datetime(
                2026, 1, 1, 0, 1, tzinfo=datetime.UTC
            ),
            missing_prior_context=False,
            missing_post_context=False,
            source_audio_uris=["gs://bucket/audio1.ogg"],
            canonical_audio_uri="gs://bucket/canonical.ogg",
            start_audio_offset=datetime.timedelta(seconds=5),
            end_audio_offset=datetime.timedelta(seconds=10),
            playback_audio_uri=None,
            created_at=datetime.datetime(2026, 1, 1, 0, 2, tzinfo=datetime.UTC),
            annotations=[],
        )
        self.mock_service.list_audio_segments.return_value = [mock_segment]

        response = self.client.get(
            "/v1/audio_segments", params={"feed_ids": [_FEED_ID]}
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        data = response.json()
        self.assertEqual(len(data), 1)
        self.assertEqual(data[0]["id"], _SEGMENT_ID)
        self.mock_service.list_audio_segments.assert_called_once_with(
            [_FEED_ID]
        )

    def test_create_audio_segment_success(self) -> None:
        """Test creating an audio segment successfully."""
        payload = {
            "feed_id": _FEED_ID,
            "classification": "SPEECH_DETECTED",
            "start_timestamp": "2026-01-01T00:00:00Z",
            "end_timestamp": "2026-01-01T00:01:00Z",
            "missing_prior_context": False,
            "missing_post_context": False,
            "source_audio_uris": ["gs://bucket/audio1.ogg"],
            "canonical_audio_uri": "gs://bucket/canonical.ogg",
            "start_audio_offset": "PT5S",  # ISO 8601 duration format for 5s
            "end_audio_offset": "PT10S",
            "playback_audio_uri": None,
        }
        mock_segment = AudioSegment(
            id=_SEGMENT_ID,
            feed_id=_FEED_ID,
            classification=AudioClassification.SPEECH_DETECTED,
            start_timestamp=datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC),
            end_timestamp=datetime.datetime(
                2026, 1, 1, 0, 1, tzinfo=datetime.UTC
            ),
            missing_prior_context=False,
            missing_post_context=False,
            source_audio_uris=["gs://bucket/audio1.ogg"],
            canonical_audio_uri="gs://bucket/canonical.ogg",
            start_audio_offset=datetime.timedelta(seconds=5),
            end_audio_offset=datetime.timedelta(seconds=10),
            playback_audio_uri=None,
            created_at=datetime.datetime(2026, 1, 1, 0, 2, tzinfo=datetime.UTC),
            annotations=[],
        )
        self.mock_service.create_audio_segment.return_value = mock_segment

        response = self.client.post("/v1/audio_segments", json=payload)
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        data = response.json()
        self.assertEqual(data["id"], _SEGMENT_ID)
        self.mock_service.create_audio_segment.assert_called_once()

    def test_create_audio_segment_parsing_error(self) -> None:
        """Test creating an audio segment with invalid data formats."""
        payload = {
            "feed_id": _FEED_ID,
            "classification": "BAD_ENUM_VALUE",
            "start_timestamp": "not-a-timestamp",
        }
        response = self.client.post("/v1/audio_segments", json=payload)
        self.assertEqual(
            response.status_code, status.HTTP_422_UNPROCESSABLE_ENTITY
        )


if __name__ == "__main__":
    unittest.main()
