"""Unit tests for the TranscriptionEventProcessor class."""

import base64
import unittest
from unittest.mock import MagicMock

import grpc
import requests
from cloudevents.http.event import CloudEvent
from google.api_core.exceptions import PermissionDenied, ServiceUnavailable

from backend.pipeline.schema_types.normalized_audio_pb2 import (
    NormalizedAudio,
)
from backend.pipeline.schema_types.transcribed_audio_pb2 import (
    TranscribedAudio,
)
from backend.pipeline.transcription.processor import (
    TranscriptionEventProcessor,
)
from backend.services.audio_segments import models as audio_segments_models


class TranscriptionEventProcessorTest(unittest.TestCase):
    def test_process_event_success(self) -> None:
        """Verifies successful end-to-end claim-check Pub/Sub CloudEvent processing."""
        # Setup mocks
        mock_transcriber = MagicMock()
        mock_transcriber.transcribe.return_value = "Hello world"

        mock_publisher = MagicMock()
        mock_future = MagicMock()
        mock_future.result.return_value = "msg-12345"
        mock_publisher.publish.return_value = mock_future
        mock_publisher.topic_path.return_value = (
            "projects/test-proj/topics/egress"
        )
        mock_audio_segments_client = MagicMock()

        # Build dummy claim proto
        claim = NormalizedAudio(
            segment_id="tx-1111",
            feed_id="feed-2222",
            missing_prior_context=False,
            missing_post_context=False,
            source_audio_uris=["gs://bucket/raw1.flac"],
            canonical_audio_uri="gs://bucket/normalized.flac",
            playback_audio_uri="gs://bucket/normalized.m4a",
            feed_name="Test Feed",
            start_timestamp={"seconds": 1000, "nanos": 1000000},
            end_timestamp={"seconds": 1005, "nanos": 2000000},
            start_audio_offset={"seconds": 0, "nanos": 0},
            end_audio_offset={"seconds": 5, "nanos": 0},
        )

        # Serialize and wrap in Pub/Sub envelope
        data_bytes = claim.SerializeToString()
        envelope = {
            "message": {
                "data": base64.b64encode(data_bytes).decode("utf-8"),
                "attributes": {
                    "traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
                },
                "messageId": "msg-1",
            }
        }

        cloud_event = CloudEvent(
            attributes={
                "type": "google.cloud.pubsub.topic.v1.messagePublished",
                "source": "test-source",
            },
            data=envelope,
        )

        processor = TranscriptionEventProcessor(
            project_id="test-proj",
            output_topic="projects/test-proj/topics/egress",
            transcriber=mock_transcriber,
            publisher=mock_publisher,
            audio_segments_client=mock_audio_segments_client,
        )

        # Run process_event
        processor.process_event(cloud_event)

        # Verify transcriber was invoked with GCS reference
        mock_transcriber.transcribe.assert_called_once_with(
            uri="gs://bucket/normalized.flac",
            duration_ms=5001,  # (1005 * 1000 + 2) - (1000 * 1000 + 1) = 5001 ms
        )

        # Verify final egress publishing was called with correctly serialized TranscribedAudio proto
        mock_publisher.publish.assert_called_once()
        call_args = mock_publisher.publish.call_args
        self.assertEqual(call_args.kwargs["ordering_key"], "feed-2222")
        self.assertEqual(
            call_args.kwargs["traceparent"],
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
        )

        # Deserialize output data passed to publish
        out_proto = TranscribedAudio()
        out_proto.ParseFromString(call_args.kwargs["data"])
        self.assertEqual(out_proto.transcript, "Hello world")
        self.assertEqual(out_proto.segment_id, "tx-1111")
        self.assertEqual(out_proto.feed_name, "Test Feed")
        self.assertEqual(
            out_proto.canonical_audio_uri, "gs://bucket/normalized.flac"
        )
        self.assertEqual(
            out_proto.playback_audio_uri, "gs://bucket/normalized.m4a"
        )

        # Verify add_audio_segment_annotation was called
        mock_audio_segments_client.add_audio_segment_annotation.assert_called_once_with(
            audio_segment_id="tx-1111",
            annotation_type=audio_segments_models.AnnotationType.TRANSCRIPT,
            data={
                "text": "Hello world",
                "errors": [],
            },
        )

    def test_process_event_empty_transcription(self) -> None:
        """Verifies behavior when speech API returns empty transcription."""
        # Setup mocks
        mock_transcriber = MagicMock()
        mock_transcriber.transcribe.return_value = ""

        mock_publisher = MagicMock()
        mock_future = MagicMock()
        mock_future.result.return_value = "msg-12345"
        mock_publisher.publish.return_value = mock_future
        mock_publisher.topic_path.return_value = (
            "projects/test-proj/topics/egress"
        )

        mock_audio_segments_client = MagicMock()

        # Build dummy claim proto
        claim = NormalizedAudio(
            segment_id="tx-1111",
            feed_id="feed-2222",
            missing_prior_context=False,
            missing_post_context=False,
            source_audio_uris=["gs://bucket/raw1.flac"],
            canonical_audio_uri="gs://bucket/normalized.flac",
            playback_audio_uri="gs://bucket/normalized.m4a",
            feed_name="Test Feed",
            start_timestamp={"seconds": 1000, "nanos": 1000000},
            end_timestamp={"seconds": 1005, "nanos": 2000000},
        )

        # Serialize and wrap in Pub/Sub envelope
        data_bytes = claim.SerializeToString()
        envelope = {
            "message": {
                "data": base64.b64encode(data_bytes).decode("utf-8"),
                "attributes": {
                    "traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
                },
                "messageId": "msg-1",
            }
        }

        cloud_event = CloudEvent(
            attributes={
                "type": "google.cloud.pubsub.topic.v1.messagePublished",
                "source": "test-source",
            },
            data=envelope,
        )

        processor = TranscriptionEventProcessor(
            project_id="test-proj",
            output_topic="projects/test-proj/topics/egress",
            transcriber=mock_transcriber,
            publisher=mock_publisher,
            audio_segments_client=mock_audio_segments_client,
        )

        # Run process_event
        processor.process_event(cloud_event)

        # Verify add_audio_segment_annotation was called with empty string and no errors
        mock_audio_segments_client.add_audio_segment_annotation.assert_called_once_with(
            audio_segment_id="tx-1111",
            annotation_type=audio_segments_models.AnnotationType.TRANSCRIPT,
            data={
                "text": "",
                "errors": [],
            },
        )
        mock_publisher.publish.assert_not_called()

    def test_process_event_transcribe_error_silent_drop(self) -> None:
        """Verifies that a permanent exception raised during transcription is caught and silently dropped."""
        mock_transcriber = MagicMock()
        mock_transcriber.transcribe.side_effect = ValueError(
            "Audio payload too long for synchronous API"
        )

        mock_publisher = MagicMock()
        mock_publisher.topic_path.return_value = (
            "projects/test-proj/topics/egress"
        )
        mock_audio_segments_client = MagicMock()

        claim = NormalizedAudio(
            segment_id="tx-1111",
            feed_id="feed-2222",
            source_audio_uris=["gs://bucket/raw1.flac"],
            canonical_audio_uri="gs://bucket/normalized.flac",
            playback_audio_uri="gs://bucket/normalized.m4a",
            feed_name="Test Feed",
            start_timestamp={"seconds": 1000, "nanos": 0},
            end_timestamp={"seconds": 1005, "nanos": 0},
        )

        data_bytes = claim.SerializeToString()
        envelope = {
            "message": {
                "data": base64.b64encode(data_bytes).decode("utf-8"),
                "attributes": {},
            }
        }

        cloud_event = CloudEvent(
            attributes={
                "type": "google.cloud.pubsub.topic.v1.messagePublished",
                "source": "test-source",
            },
            data=envelope,
        )

        processor = TranscriptionEventProcessor(
            project_id="test-proj",
            output_topic="projects/test-proj/topics/egress",
            transcriber=mock_transcriber,
            publisher=mock_publisher,
            audio_segments_client=mock_audio_segments_client,
        )

        # Permanent transcription exception must be caught gracefully without propagating
        processor.process_event(cloud_event)

        # Egress publishing must never be called (event silently dropped)
        mock_publisher.publish.assert_not_called()

        # Verify annotation was written with the permanent failure error
        mock_audio_segments_client.add_audio_segment_annotation.assert_called_once()
        call_data = mock_audio_segments_client.add_audio_segment_annotation.call_args.kwargs[
            "data"
        ]
        self.assertEqual(call_data["text"], "")
        self.assertIn("Permanent Failure", call_data["errors"][0])

    def test_process_event_transient_error_propagates(self) -> None:
        """Verifies that a transient exception raised during transcription propagates so Pub/Sub retries."""

        class MockGrpcCallError(grpc.RpcError, grpc.Call):
            """Mock exception implementing both RpcError and grpc.Call."""

            def code(self) -> grpc.StatusCode:
                return grpc.StatusCode.UNAVAILABLE

        mock_transcriber = MagicMock()
        grpc_err = MockGrpcCallError()
        mock_transcriber.transcribe.side_effect = grpc_err

        mock_publisher = MagicMock()
        mock_publisher.topic_path.return_value = (
            "projects/test-proj/topics/egress"
        )
        mock_audio_segments_client = MagicMock()

        claim = NormalizedAudio(
            segment_id="tx-1111",
            feed_id="feed-2222",
            source_audio_uris=["gs://bucket/raw1.flac"],
            canonical_audio_uri="gs://bucket/normalized.flac",
            playback_audio_uri="gs://bucket/normalized.m4a",
            feed_name="Test Feed",
            start_timestamp={"seconds": 1000, "nanos": 0},
            end_timestamp={"seconds": 1005, "nanos": 0},
        )

        data_bytes = claim.SerializeToString()
        envelope = {
            "message": {
                "data": base64.b64encode(data_bytes).decode("utf-8"),
                "attributes": {},
            }
        }

        cloud_event = CloudEvent(
            attributes={
                "type": "google.cloud.pubsub.topic.v1.messagePublished",
                "source": "test-source",
            },
            data=envelope,
        )

        processor = TranscriptionEventProcessor(
            project_id="test-proj",
            output_topic="projects/test-proj/topics/egress",
            transcriber=mock_transcriber,
            publisher=mock_publisher,
            audio_segments_client=mock_audio_segments_client,
        )

        # Transient error must be propagated/raised to trigger retry
        with self.assertRaises(grpc.RpcError):
            processor.process_event(cloud_event)

        # Egress publishing must never be called
        mock_publisher.publish.assert_not_called()

        # The annotation is still written with the transient failure message
        mock_audio_segments_client.add_audio_segment_annotation.assert_called_once()
        call_data = mock_audio_segments_client.add_audio_segment_annotation.call_args.kwargs[
            "data"
        ]
        self.assertIn("Transient Failure", call_data["errors"][0])

    def test_process_event_audio_too_long_permanent_failure(self) -> None:
        """Verifies that when audio duration exceeds the transcriber's limit,
        the transcriber raises ValueError and it is treated as a permanent failure.
        """
        mock_transcriber = MagicMock()
        mock_transcriber.transcribe.side_effect = ValueError(
            "Audio payload too long for synchronous API"
        )
        mock_publisher = MagicMock()
        mock_audio_segments_client = MagicMock()

        claim = NormalizedAudio(
            segment_id="tx-1111",
            feed_id="feed-2222",
            source_audio_uris=["gs://bucket/raw1.flac"],
            canonical_audio_uri="gs://bucket/normalized.flac",
            playback_audio_uri="gs://bucket/normalized.m4a",
            feed_name="Test Feed",
            start_timestamp={"seconds": 1000, "nanos": 0},
            end_timestamp={"seconds": 1065, "nanos": 0},
        )

        data_bytes = claim.SerializeToString()
        envelope = {
            "message": {
                "data": base64.b64encode(data_bytes).decode("utf-8"),
                "attributes": {},
            }
        }

        cloud_event = CloudEvent(
            attributes={
                "type": "google.cloud.pubsub.topic.v1.messagePublished",
                "source": "test-source",
            },
            data=envelope,
        )

        processor = TranscriptionEventProcessor(
            project_id="test-proj",
            output_topic="projects/test-proj/topics/egress",
            transcriber=mock_transcriber,
            publisher=mock_publisher,
            audio_segments_client=mock_audio_segments_client,
        )

        # Must return cleanly without raising, acknowledging the message
        processor.process_event(cloud_event)

        # Transcriber is called and raises the ValueError
        mock_transcriber.transcribe.assert_called_once()
        mock_publisher.publish.assert_not_called()

        # Annotation should specify that a permanent failure occurred due to the ValueError
        mock_audio_segments_client.add_audio_segment_annotation.assert_called_once()
        call_data = mock_audio_segments_client.add_audio_segment_annotation.call_args.kwargs[
            "data"
        ]
        self.assertEqual(call_data["text"], "")
        self.assertIn("Permanent Failure", call_data["errors"][0])
        self.assertIn("Audio payload too long", call_data["errors"][0])

    def test_process_event_google_api_transient_error_propagates(self) -> None:
        """Verifies that a transient GoogleAPICallError propagates to trigger a retry."""
        mock_transcriber = MagicMock()
        mock_transcriber.transcribe.side_effect = ServiceUnavailable(
            "Transient backend error"
        )

        mock_publisher = MagicMock()
        mock_publisher.topic_path.return_value = (
            "projects/test-proj/topics/egress"
        )
        mock_audio_segments_client = MagicMock()

        claim = NormalizedAudio(
            segment_id="tx-1111",
            feed_id="feed-2222",
            source_audio_uris=["gs://bucket/raw1.flac"],
            canonical_audio_uri="gs://bucket/normalized.flac",
            playback_audio_uri="gs://bucket/normalized.m4a",
            feed_name="Test Feed",
            start_timestamp={"seconds": 1000, "nanos": 0},
            end_timestamp={"seconds": 1005, "nanos": 0},
        )

        data_bytes = claim.SerializeToString()
        envelope = {
            "message": {
                "data": base64.b64encode(data_bytes).decode("utf-8"),
                "attributes": {},
            }
        }

        cloud_event = CloudEvent(
            attributes={
                "type": "google.cloud.pubsub.topic.v1.messagePublished",
                "source": "test-source",
            },
            data=envelope,
        )

        processor = TranscriptionEventProcessor(
            project_id="test-proj",
            output_topic="projects/test-proj/topics/egress",
            transcriber=mock_transcriber,
            publisher=mock_publisher,
            audio_segments_client=mock_audio_segments_client,
        )

        # ServiceUnavailable (GoogleAPICallError with code 503) must propagate
        with self.assertRaises(ServiceUnavailable):
            processor.process_event(cloud_event)

        mock_publisher.publish.assert_not_called()
        mock_audio_segments_client.add_audio_segment_annotation.assert_called_once()
        call_data = mock_audio_segments_client.add_audio_segment_annotation.call_args.kwargs[
            "data"
        ]
        self.assertIn("Transient Failure", call_data["errors"][0])

    def test_process_event_google_api_permanent_error_silent_drop(self) -> None:
        """Verifies that a permanent GoogleAPICallError is caught and acknowledged without retry."""
        mock_transcriber = MagicMock()
        mock_transcriber.transcribe.side_effect = PermissionDenied(
            "GCP Permission Denied"
        )

        mock_publisher = MagicMock()
        mock_publisher.topic_path.return_value = (
            "projects/test-proj/topics/egress"
        )
        mock_audio_segments_client = MagicMock()

        claim = NormalizedAudio(
            segment_id="tx-1111",
            feed_id="feed-2222",
            source_audio_uris=["gs://bucket/raw1.flac"],
            canonical_audio_uri="gs://bucket/normalized.flac",
            playback_audio_uri="gs://bucket/normalized.m4a",
            feed_name="Test Feed",
            start_timestamp={"seconds": 1000, "nanos": 0},
            end_timestamp={"seconds": 1005, "nanos": 0},
        )

        data_bytes = claim.SerializeToString()
        envelope = {
            "message": {
                "data": base64.b64encode(data_bytes).decode("utf-8"),
                "attributes": {},
            }
        }

        cloud_event = CloudEvent(
            attributes={
                "type": "google.cloud.pubsub.topic.v1.messagePublished",
                "source": "test-source",
            },
            data=envelope,
        )

        processor = TranscriptionEventProcessor(
            project_id="test-proj",
            output_topic="projects/test-proj/topics/egress",
            transcriber=mock_transcriber,
            publisher=mock_publisher,
            audio_segments_client=mock_audio_segments_client,
        )

        # PermissionDenied (GoogleAPICallError with code 403) must be caught and swallowed cleanly
        processor.process_event(cloud_event)

        mock_publisher.publish.assert_not_called()
        mock_audio_segments_client.add_audio_segment_annotation.assert_called_once()
        call_data = mock_audio_segments_client.add_audio_segment_annotation.call_args.kwargs[
            "data"
        ]
        self.assertEqual(call_data["text"], "")
        self.assertIn("Permanent Failure", call_data["errors"][0])

    def test_process_event_requests_timeout_transient_error_propagates(
        self,
    ) -> None:
        """Verifies that requests.exceptions.Timeout during transcription propagates to trigger a retry."""
        mock_transcriber = MagicMock()
        mock_transcriber.transcribe.side_effect = requests.exceptions.Timeout(
            "Request timed out"
        )

        mock_publisher = MagicMock()
        mock_audio_segments_client = MagicMock()

        claim = NormalizedAudio(
            segment_id="tx-1111",
            feed_id="feed-2222",
            source_audio_uris=["gs://bucket/raw1.flac"],
            canonical_audio_uri="gs://bucket/normalized.flac",
            playback_audio_uri="gs://bucket/normalized.m4a",
            feed_name="Test Feed",
            start_timestamp={"seconds": 1000, "nanos": 0},
            end_timestamp={"seconds": 1005, "nanos": 0},
        )

        data_bytes = claim.SerializeToString()
        envelope = {
            "message": {
                "data": base64.b64encode(data_bytes).decode("utf-8"),
                "attributes": {},
            }
        }

        cloud_event = CloudEvent(
            attributes={
                "type": "google.cloud.pubsub.topic.v1.messagePublished",
                "source": "test-source",
            },
            data=envelope,
        )

        processor = TranscriptionEventProcessor(
            project_id="test-proj",
            output_topic="projects/test-proj/topics/egress",
            transcriber=mock_transcriber,
            publisher=mock_publisher,
            audio_segments_client=mock_audio_segments_client,
        )

        with self.assertRaises(requests.exceptions.Timeout):
            processor.process_event(cloud_event)

        mock_publisher.publish.assert_not_called()
        mock_audio_segments_client.add_audio_segment_annotation.assert_called_once()
        call_data = mock_audio_segments_client.add_audio_segment_annotation.call_args.kwargs[
            "data"
        ]
        self.assertIn("Transient Failure", call_data["errors"][0])

    def test_process_event_requests_connection_error_transient_error_propagates(
        self,
    ) -> None:
        """Verifies that requests.exceptions.ConnectionError during transcription propagates to trigger a retry."""
        mock_transcriber = MagicMock()
        mock_transcriber.transcribe.side_effect = (
            requests.exceptions.ConnectionError("Connection refused")
        )

        mock_publisher = MagicMock()
        mock_audio_segments_client = MagicMock()

        claim = NormalizedAudio(
            segment_id="tx-1111",
            feed_id="feed-2222",
            source_audio_uris=["gs://bucket/raw1.flac"],
            canonical_audio_uri="gs://bucket/normalized.flac",
            playback_audio_uri="gs://bucket/normalized.m4a",
            feed_name="Test Feed",
            start_timestamp={"seconds": 1000, "nanos": 0},
            end_timestamp={"seconds": 1005, "nanos": 0},
        )

        data_bytes = claim.SerializeToString()
        envelope = {
            "message": {
                "data": base64.b64encode(data_bytes).decode("utf-8"),
                "attributes": {},
            }
        }

        cloud_event = CloudEvent(
            attributes={
                "type": "google.cloud.pubsub.topic.v1.messagePublished",
                "source": "test-source",
            },
            data=envelope,
        )

        processor = TranscriptionEventProcessor(
            project_id="test-proj",
            output_topic="projects/test-proj/topics/egress",
            transcriber=mock_transcriber,
            publisher=mock_publisher,
            audio_segments_client=mock_audio_segments_client,
        )

        with self.assertRaises(requests.exceptions.ConnectionError):
            processor.process_event(cloud_event)

        mock_publisher.publish.assert_not_called()
        mock_audio_segments_client.add_audio_segment_annotation.assert_called_once()
        call_data = mock_audio_segments_client.add_audio_segment_annotation.call_args.kwargs[
            "data"
        ]
        self.assertIn("Transient Failure", call_data["errors"][0])

    def test_process_event_requests_http_500_transient_error_propagates(
        self,
    ) -> None:
        """Verifies that requests.exceptions.HTTPError (500) during transcription propagates to trigger a retry."""
        mock_transcriber = MagicMock()

        mock_resp = MagicMock()
        mock_resp.status_code = 500
        http_err = requests.exceptions.HTTPError(
            "500 Server Error", response=mock_resp
        )
        mock_transcriber.transcribe.side_effect = http_err

        mock_publisher = MagicMock()
        mock_audio_segments_client = MagicMock()

        claim = NormalizedAudio(
            segment_id="tx-1111",
            feed_id="feed-2222",
            source_audio_uris=["gs://bucket/raw1.flac"],
            canonical_audio_uri="gs://bucket/normalized.flac",
            playback_audio_uri="gs://bucket/normalized.m4a",
            feed_name="Test Feed",
            start_timestamp={"seconds": 1000, "nanos": 0},
            end_timestamp={"seconds": 1005, "nanos": 0},
        )

        data_bytes = claim.SerializeToString()
        envelope = {
            "message": {
                "data": base64.b64encode(data_bytes).decode("utf-8"),
                "attributes": {},
            }
        }

        cloud_event = CloudEvent(
            attributes={
                "type": "google.cloud.pubsub.topic.v1.messagePublished",
                "source": "test-source",
            },
            data=envelope,
        )

        processor = TranscriptionEventProcessor(
            project_id="test-proj",
            output_topic="projects/test-proj/topics/egress",
            transcriber=mock_transcriber,
            publisher=mock_publisher,
            audio_segments_client=mock_audio_segments_client,
        )

        with self.assertRaises(requests.exceptions.HTTPError):
            processor.process_event(cloud_event)

        mock_publisher.publish.assert_not_called()
        mock_audio_segments_client.add_audio_segment_annotation.assert_called_once()
        call_data = mock_audio_segments_client.add_audio_segment_annotation.call_args.kwargs[
            "data"
        ]
        self.assertIn("Transient Failure", call_data["errors"][0])

    def test_process_event_requests_http_400_permanent_error_silent_drop(
        self,
    ) -> None:
        """Verifies that requests.exceptions.HTTPError (400) during transcription is caught and silently dropped."""
        mock_transcriber = MagicMock()

        mock_resp = MagicMock()
        mock_resp.status_code = 400
        http_err = requests.exceptions.HTTPError(
            "400 Bad Request", response=mock_resp
        )
        mock_transcriber.transcribe.side_effect = http_err

        mock_publisher = MagicMock()
        mock_audio_segments_client = MagicMock()

        claim = NormalizedAudio(
            segment_id="tx-1111",
            feed_id="feed-2222",
            source_audio_uris=["gs://bucket/raw1.flac"],
            canonical_audio_uri="gs://bucket/normalized.flac",
            playback_audio_uri="gs://bucket/normalized.m4a",
            feed_name="Test Feed",
            start_timestamp={"seconds": 1000, "nanos": 0},
            end_timestamp={"seconds": 1005, "nanos": 0},
        )

        data_bytes = claim.SerializeToString()
        envelope = {
            "message": {
                "data": base64.b64encode(data_bytes).decode("utf-8"),
                "attributes": {},
            }
        }

        cloud_event = CloudEvent(
            attributes={
                "type": "google.cloud.pubsub.topic.v1.messagePublished",
                "source": "test-source",
            },
            data=envelope,
        )

        processor = TranscriptionEventProcessor(
            project_id="test-proj",
            output_topic="projects/test-proj/topics/egress",
            transcriber=mock_transcriber,
            publisher=mock_publisher,
            audio_segments_client=mock_audio_segments_client,
        )

        processor.process_event(cloud_event)

        mock_publisher.publish.assert_not_called()
        mock_audio_segments_client.add_audio_segment_annotation.assert_called_once()
        call_data = mock_audio_segments_client.add_audio_segment_annotation.call_args.kwargs[
            "data"
        ]
        self.assertEqual(call_data["text"], "")
        self.assertIn("Permanent Failure", call_data["errors"][0])
