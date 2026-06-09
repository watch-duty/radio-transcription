import datetime
import unittest
from unittest.mock import AsyncMock

from fastapi import status
from fastapi.testclient import TestClient

from backend.pipeline.common.auth import verify_oidc_token
from backend.pipeline.schema_types.evaluated_transcribed_audio_pb2 import (
    EvaluatedTranscribedAudio,
)
from backend.pipeline.storage.transcript_store import (
    PaginatedTranscripts,
    SortOrder,
)
from backend.services.transcripts.main import app
from backend.services.transcripts.service import TranscriptService

_SEGMENT_ID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
_FEED_ID = "bbbbbbbb-cccc-dddd-eeee-ffffffffffff"


async def skip_auth() -> dict[str, str]:
    """Mock dependency to bypass authentication in tests."""
    return {"sub": "test@example.com", "email": "test@example.com"}


def _make_transcript_msg() -> EvaluatedTranscribedAudio:
    """Helper to create a populated EvaluatedTranscribedAudio message."""
    msg = EvaluatedTranscribedAudio()
    msg.segment_id = _SEGMENT_ID
    msg.feed_id = _FEED_ID
    msg.transcript = "Hello world"
    msg.start_timestamp.FromDatetime(
        datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC)
    )
    msg.end_timestamp.FromDatetime(
        datetime.datetime(2026, 1, 1, 0, 1, tzinfo=datetime.UTC)
    )
    msg.source_audio_uris.append("gs://bucket/audio1.ogg")
    msg.canonical_audio_uri = "gs://bucket/canonical.ogg"
    msg.start_audio_offset.FromTimedelta(datetime.timedelta(seconds=5))
    msg.end_audio_offset.FromTimedelta(datetime.timedelta(seconds=10))
    msg.evaluation_decisions.append("rule-1")
    return msg


class TestTranscriptsAPI(unittest.TestCase):
    def setUp(self) -> None:
        """Set up a test client and dependency overrides before each test."""
        self.mock_store = AsyncMock()
        # Use real TranscriptService initialized with mocked store
        self.service = TranscriptService(self.mock_store)
        app.state.transcript_service = self.service

        app.dependency_overrides[verify_oidc_token] = skip_auth
        self.client = TestClient(app)

    def tearDown(self) -> None:
        """Clean up after each test."""
        app.dependency_overrides.clear()

    def test_create_transcript_success(self) -> None:
        """Test creating a transcript successfully."""
        payload = {
            "segment_id": _SEGMENT_ID,
            "feed_id": _FEED_ID,
            "transcript": "Hello world",
            "start_timestamp": "2026-01-01T00:00:00Z",
            "end_timestamp": "2026-01-01T00:01:00Z",
            "source_audio_uris": ["gs://bucket/audio1.ogg"],
            "canonical_audio_uri": "gs://bucket/canonical.ogg",
            "start_audio_offset": "5s",
            "end_audio_offset": "10s",
            "evaluation_decisions": ["rule-1"],
        }
        mock_msg = _make_transcript_msg()
        self.mock_store.create_transcript.return_value = mock_msg

        response = self.client.post("/v1/transcripts", json=payload)

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        data = response.json()
        self.assertEqual(data["segment_id"], _SEGMENT_ID)
        self.mock_store.create_transcript.assert_called_once()

    def test_create_transcript_renests_rule_annotations(self) -> None:
        """The flat text_match list is re-nested into the proto's oneof wrapper."""
        payload = {
            "transmission_id": _TRANSMISSION_ID,
            "feed_id": _FEED_ID,
            "transcript": "Hello world",
            "start_timestamp": "2026-01-01T00:00:00Z",
            "end_timestamp": "2026-01-01T00:01:00Z",
            "source_audio_uris": ["gs://bucket/audio1.ogg"],
            "evaluation_decisions": ["rule-1"],
            "rule_annotations": {
                "rule-1": {
                    "text_match": [
                        {"start": 0, "end": 5, "matched_text": "Hello"}
                    ],
                }
            },
        }
        self.mock_store.create_transcript.return_value = _make_transcript_msg()

        response = self.client.post("/v1/transcripts", json=payload)

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        sent_msg = self.mock_store.create_transcript.call_args.args[0]
        self.assertEqual(len(sent_msg.rule_annotations), 1)
        annotation = sent_msg.rule_annotations["rule-1"]
        self.assertEqual(annotation.WhichOneof("annotation"), "text_match")
        self.assertEqual(len(annotation.text_match.spans), 1)
        self.assertEqual(annotation.text_match.spans[0].matched_text, "Hello")

    def test_create_transcript_parsing_error(self) -> None:
        """Test creating a transcript with invalid JSON schema."""
        payload = {
            "segment_id": _SEGMENT_ID,
            "feed_id": _FEED_ID,
            "transcript": "bad transcript",
            "start_audio_offset": "not-a-duration",
        }

        response = self.client.post("/v1/transcripts", json=payload)

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn("Invalid transcript data", response.json()["detail"])

    def test_get_transcript_success(self) -> None:
        """Test fetching an existing transcript."""
        mock_msg = _make_transcript_msg()
        self.mock_store.get_transcript.return_value = mock_msg

        response = self.client.get(f"/v1/transcripts/{_SEGMENT_ID}")

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.json()["segment_id"], _SEGMENT_ID)
        self.mock_store.get_transcript.assert_called_once_with(_SEGMENT_ID)

    def test_get_transcript_not_found(self) -> None:
        """Test fetching a non-existent transcript returns 404."""
        self.mock_store.get_transcript.return_value = None

        response = self.client.get(f"/v1/transcripts/{_SEGMENT_ID}")

        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

    def test_list_transcripts_success(self) -> None:
        """Test listing all transcripts."""
        mock_msg = _make_transcript_msg()
        self.mock_store.list_transcripts.return_value = PaginatedTranscripts(
            [mock_msg], None
        )

        response = self.client.get("/v1/transcripts")

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        data = response.json()
        self.assertIn("transcripts", data)
        self.assertEqual(len(data["transcripts"]), 1)
        self.assertEqual(data["transcripts"][0]["segment_id"], _SEGMENT_ID)

    def test_list_transcripts_by_feed_id(self) -> None:
        """Test listing transcripts filtered by feed ID."""
        mock_msg = _make_transcript_msg()
        self.mock_store.list_transcripts_by_feed_id.return_value = (
            PaginatedTranscripts([mock_msg], None)
        )

        response = self.client.get(f"/v1/transcripts?feed_id={_FEED_ID}")

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        data = response.json()
        self.assertIn("transcripts", data)
        self.assertEqual(len(data["transcripts"]), 1)
        self.mock_store.list_transcripts_by_feed_id.assert_called_once_with(
            _FEED_ID,
            limit=100,
            next_token=None,
            start_time=None,
            end_time=None,
            order=SortOrder.DESC,
            is_alert=None,
        )

    def test_list_transcripts_is_alert_true(self) -> None:
        """Test listing transcripts filtered by is_alert=true."""
        mock_msg = _make_transcript_msg()
        self.mock_store.list_transcripts.return_value = PaginatedTranscripts(
            [mock_msg], None
        )

        response = self.client.get("/v1/transcripts?is_alert=true")

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        data = response.json()
        self.assertIn("transcripts", data)
        self.assertEqual(len(data["transcripts"]), 1)
        self.mock_store.list_transcripts.assert_called_once_with(
            limit=100,
            next_token=None,
            start_time=None,
            end_time=None,
            order=SortOrder.DESC,
            is_alert=True,
        )

    def test_list_transcripts_is_alert_false(self) -> None:
        """Test listing transcripts filtered by is_alert=false."""
        mock_msg = _make_transcript_msg()
        self.mock_store.list_transcripts.return_value = PaginatedTranscripts(
            [mock_msg], None
        )

        response = self.client.get("/v1/transcripts?is_alert=false")

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        data = response.json()
        self.assertIn("transcripts", data)
        self.assertEqual(len(data["transcripts"]), 1)
        self.mock_store.list_transcripts.assert_called_once_with(
            limit=100,
            next_token=None,
            start_time=None,
            end_time=None,
            order=SortOrder.DESC,
            is_alert=False,
        )

    def test_list_transcripts_by_feed_id_is_alert(self) -> None:
        """Test listing transcripts by feed ID with is_alert=true."""
        mock_msg = _make_transcript_msg()
        self.mock_store.list_transcripts_by_feed_id.return_value = (
            PaginatedTranscripts([mock_msg], None)
        )

        response = self.client.get(
            f"/v1/transcripts?feed_id={_FEED_ID}&is_alert=true"
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        data = response.json()
        self.assertIn("transcripts", data)
        self.assertEqual(len(data["transcripts"]), 1)
        self.mock_store.list_transcripts_by_feed_id.assert_called_once_with(
            _FEED_ID,
            limit=100,
            next_token=None,
            start_time=None,
            end_time=None,
            order=SortOrder.DESC,
            is_alert=True,
        )

    def test_delete_transcript_success(self) -> None:
        """Test deleting a transcript successfully."""
        self.mock_store.delete_transcript.return_value = True

        response = self.client.delete(f"/v1/transcripts/{_SEGMENT_ID}")

        self.assertEqual(response.status_code, status.HTTP_204_NO_CONTENT)
        self.mock_store.delete_transcript.assert_called_once_with(_SEGMENT_ID)

    def test_delete_transcript_not_found(self) -> None:
        """Test deleting a non-existent transcript returns 404."""
        self.mock_store.delete_transcript.return_value = False

        response = self.client.delete(f"/v1/transcripts/{_SEGMENT_ID}")

        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)


if __name__ == "__main__":
    unittest.main()
