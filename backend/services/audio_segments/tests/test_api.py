import datetime
import unittest
import uuid
from unittest.mock import AsyncMock

from fastapi import status
from fastapi.testclient import TestClient

from backend.pipeline.common.auth import verify_oidc_token
from backend.services.audio_segments.main import app
from backend.services.audio_segments.models import (
    AudioClassification,
    AudioSegment,
)


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
        segment_id = str(uuid.uuid4())
        feed_id = str(uuid.uuid4())
        mock_segment = AudioSegment(
            id=segment_id,
            feed_id=feed_id,
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
            "/v1/audio_segments", params={"feed_ids": [feed_id]}
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        data = response.json()
        self.assertEqual(len(data), 1)
        self.assertEqual(data[0]["id"], segment_id)
        self.mock_service.list_audio_segments.assert_called_once_with([feed_id])


if __name__ == "__main__":
    unittest.main()
