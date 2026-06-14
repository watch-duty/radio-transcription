import datetime
import unittest
from unittest.mock import AsyncMock

from fastapi import status
from fastapi.testclient import TestClient

from backend.pipeline.common.auth import verify_oidc_token
from backend.pipeline.storage.audio_segment_store import SortOrder
from backend.services.audio_segments.main import app
from backend.services.audio_segments.models import (
    AnnotationType,
    AudioClassification,
    AudioSegment,
    AudioSegmentSummary,
    EvaluationAnnotation,
    EvaluationAnnotationData,
    ListAudioSegmentsResponse,
    ListAudioSegmentSummariesResponse,
    TranscriptAnnotation,
    TranscriptAnnotationData,
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
            classification=AudioClassification.SPEECH,
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
        self.mock_service.list_audio_segments.return_value = (
            ListAudioSegmentsResponse(segments=[mock_segment])
        )

        response = self.client.get(
            "/v1/audio_segments", params={"feed_ids": [_FEED_ID]}
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        data = response.json()
        self.assertEqual(len(data["segments"]), 1)
        self.assertEqual(data["segments"][0]["id"], _SEGMENT_ID)
        self.mock_service.list_audio_segments.assert_called_once_with(
            feed_ids=[_FEED_ID],
            limit=100,
            next_token=None,
            start_time=None,
            end_time=None,
            order=SortOrder.DESC,
            is_alert=None,
        )

    def test_list_audio_segments_with_filters(self) -> None:
        """Test listing audio segments with all filters."""
        self.mock_service.list_audio_segments.return_value = (
            ListAudioSegmentsResponse(segments=[])
        )

        start_time = "2026-01-01T00:00:00Z"
        end_time = "2026-01-01T01:00:00Z"

        response = self.client.get(
            "/v1/audio_segments",
            params={
                "feed_ids": [_FEED_ID],
                "limit": 10,
                "next_token": "token123",
                "start_time": start_time,
                "end_time": end_time,
                "order": "asc",
                "is_alert": True,
            },
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.mock_service.list_audio_segments.assert_called_once_with(
            feed_ids=[_FEED_ID],
            limit=10,
            next_token="token123",
            start_time=datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC),
            end_time=datetime.datetime(2026, 1, 1, 1, tzinfo=datetime.UTC),
            order=SortOrder.ASC,
            is_alert=True,
        )

    def test_list_audio_segment_summaries_defaults_end_time_to_now(
        self,
    ) -> None:
        """end_time defaults to 'now' server-side when omitted."""
        summary = AudioSegmentSummary(
            id=_SEGMENT_ID,
            feed_id=_FEED_ID,
            start_timestamp=datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC),
            end_timestamp=datetime.datetime(
                2026, 1, 1, 0, 1, tzinfo=datetime.UTC
            ),
            classification=AudioClassification.SPEECH,
            is_alert=False,
        )
        self.mock_service.list_audio_segment_summaries.return_value = (
            ListAudioSegmentSummariesResponse(segments=[summary])
        )

        before = datetime.datetime.now(datetime.UTC)
        response = self.client.get(
            "/v1/audio_segment_summaries",
            params={
                "feed_ids": [_FEED_ID],
                "start_time": "2026-01-01T00:00:00Z",
            },
        )
        after = datetime.datetime.now(datetime.UTC)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        data = response.json()
        self.assertEqual(len(data["segments"]), 1)
        self.assertEqual(data["segments"][0]["id"], _SEGMENT_ID)
        self.assertIsNone(data["next_token"])

        kwargs = self.mock_service.list_audio_segment_summaries.call_args.kwargs
        self.assertEqual(kwargs["feed_ids"], [_FEED_ID])
        self.assertEqual(
            kwargs["start_time"],
            datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC),
        )
        self.assertIsNone(kwargs["is_alert"])
        self.assertTrue(before <= kwargs["end_time"] <= after)

    def test_list_audio_segment_summaries_with_filters(self) -> None:
        """Explicit window and is_alert are forwarded as given."""
        self.mock_service.list_audio_segment_summaries.return_value = (
            ListAudioSegmentSummariesResponse(segments=[])
        )

        response = self.client.get(
            "/v1/audio_segment_summaries",
            params={
                "feed_ids": [_FEED_ID],
                "start_time": "2026-01-01T00:00:00Z",
                "end_time": "2026-01-01T01:00:00Z",
                "is_alert": True,
            },
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.mock_service.list_audio_segment_summaries.assert_called_once_with(
            start_time=datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC),
            end_time=datetime.datetime(2026, 1, 1, 1, tzinfo=datetime.UTC),
            feed_ids=[_FEED_ID],
            limit=100,
            next_token=None,
            is_alert=True,
        )

    def test_list_audio_segment_summaries_full_page_returns_token(self) -> None:
        """A full page returns next_token for the next page."""
        self.mock_service.list_audio_segment_summaries.return_value = (
            ListAudioSegmentSummariesResponse(
                segments=[], next_token="cursor-123"
            )
        )

        response = self.client.get(
            "/v1/audio_segment_summaries",
            params={
                "feed_ids": [_FEED_ID],
                "start_time": "2026-01-01T00:00:00Z",
            },
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        data = response.json()
        self.assertEqual(data["next_token"], "cursor-123")

    def test_list_audio_segment_summaries_resume_forwards_token(self) -> None:
        """A supplied next_token is forwarded to resume the window."""
        self.mock_service.list_audio_segment_summaries.return_value = (
            ListAudioSegmentSummariesResponse(segments=[])
        )

        response = self.client.get(
            "/v1/audio_segment_summaries",
            params={
                "feed_ids": [_FEED_ID],
                "start_time": "2026-01-01T00:00:00Z",
                "next_token": "cursor-123",
            },
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        kwargs = self.mock_service.list_audio_segment_summaries.call_args.kwargs
        self.assertEqual(kwargs["next_token"], "cursor-123")

    def test_list_audio_segment_summaries_invalid_window_returns_400(
        self,
    ) -> None:
        """A window the service rejects surfaces as a 400."""
        self.mock_service.list_audio_segment_summaries.side_effect = ValueError(
            "end_time must not be before start_time."
        )

        response = self.client.get(
            "/v1/audio_segment_summaries",
            params={
                "feed_ids": [_FEED_ID],
                "start_time": "2026-01-02T00:00:00Z",
                "end_time": "2026-01-01T00:00:00Z",
            },
        )

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn("must not be before", response.json()["detail"])

    def test_list_audio_segment_summaries_requires_start_time(self) -> None:
        """start_time is a required query parameter."""
        response = self.client.get(
            "/v1/audio_segment_summaries", params={"feed_ids": [_FEED_ID]}
        )
        self.assertEqual(
            response.status_code, status.HTTP_422_UNPROCESSABLE_CONTENT
        )

    def test_create_audio_segment_success(self) -> None:
        """Test creating an audio segment successfully."""
        payload = {
            "id": _SEGMENT_ID,
            "feed_id": _FEED_ID,
            "classification": "SPEECH",
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
            classification=AudioClassification.SPEECH,
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
            response.status_code, status.HTTP_422_UNPROCESSABLE_CONTENT
        )

    def test_add_transcript_annotation_success(self) -> None:
        """Test adding a transcript annotation successfully."""
        payload = {
            "type": "TRANSCRIPT",
            "data": {"text": "hello test", "errors": []},
        }
        mock_annotation = TranscriptAnnotation(
            audio_segment_id=_SEGMENT_ID,
            type=AnnotationType.TRANSCRIPT,
            data=TranscriptAnnotationData(text="hello test", errors=[]),
            created_at=datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC),
        )
        self.mock_service.add_annotation.return_value = mock_annotation

        response = self.client.post(
            f"/v1/audio_segments/{_SEGMENT_ID}/annotations", json=payload
        )
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        data = response.json()
        self.assertEqual(data["audio_segment_id"], _SEGMENT_ID)
        self.assertEqual(data["type"], "TRANSCRIPT")
        self.assertEqual(data["data"]["text"], "hello test")
        self.mock_service.add_annotation.assert_called_once()

    def test_add_evaluation_annotation_success(self) -> None:
        """Test adding an evaluation annotation successfully."""
        payload = {
            "type": "EVALUATION",
            "data": {"decisions": ["rule-1"], "errors": ["err-1"]},
        }
        mock_annotation = EvaluationAnnotation(
            audio_segment_id=_SEGMENT_ID,
            type=AnnotationType.EVALUATION,
            data=EvaluationAnnotationData(
                decisions=["rule-1"], errors=["err-1"]
            ),
            created_at=datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC),
        )
        self.mock_service.add_annotation.return_value = mock_annotation

        response = self.client.post(
            f"/v1/audio_segments/{_SEGMENT_ID}/annotations", json=payload
        )
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        data = response.json()
        self.assertEqual(data["audio_segment_id"], _SEGMENT_ID)
        self.assertEqual(data["type"], "EVALUATION")
        self.assertEqual(data["data"]["decisions"], ["rule-1"])
        self.mock_service.add_annotation.assert_called_once()

    def test_add_annotation_segment_not_found(self) -> None:
        """Test adding an annotation to a non-existent segment."""
        payload = {
            "type": "TRANSCRIPT",
            "data": {"text": "hello test", "errors": []},
        }
        self.mock_service.add_annotation.side_effect = ValueError(
            f"Audio segment {_SEGMENT_ID} does not exist."
        )

        response = self.client.post(
            f"/v1/audio_segments/{_SEGMENT_ID}/annotations", json=payload
        )
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        data = response.json()
        self.assertIn("does not exist", data["detail"])

    def test_add_annotation_parsing_error(self) -> None:
        """Test adding an annotation with invalid payload formats."""
        payload = {
            "type": "TRANSCRIPT",
            "data": {"bad_field": "hello test"},
        }
        response = self.client.post(
            f"/v1/audio_segments/{_SEGMENT_ID}/annotations", json=payload
        )
        self.assertEqual(
            response.status_code, status.HTTP_422_UNPROCESSABLE_CONTENT
        )


if __name__ == "__main__":
    unittest.main()
