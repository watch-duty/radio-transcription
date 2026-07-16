"""Unit tests for the TranscriptionEventProcessor class."""

import base64
import unittest
from concurrent.futures import Future
from unittest.mock import MagicMock, patch

import grpc
import httpx
import requests
from cloudevents.http.event import CloudEvent
from google.api_core.exceptions import (
    GoogleAPICallError,
    PermissionDenied,
    RetryError,
    ServiceUnavailable,
)
from google.genai import errors as genai_errors
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
    InMemorySpanExporter,
)
from opentelemetry.trace import StatusCode

from backend.pipeline.common import tracing_utils
from backend.pipeline.common.clients.audio_segments_client import (
    AsyncAudioSegmentsClient,
)
from backend.pipeline.common.exceptions import PartialTranscriptionError
from backend.pipeline.schema_types.normalized_audio_pb2 import (
    NormalizedAudio,
)
from backend.pipeline.schema_types.transcribed_audio_pb2 import (
    TranscribedAudio,
)
from backend.pipeline.transcription.enums import TranscriptionStatus
from backend.pipeline.transcription.processor import (
    CHIRP_UNINTELLIGIBLE_MARKER,
    TranscriptionEventProcessor,
    is_transient_exception,
)
from backend.pipeline.transcription.transcribers.base import Transcriber
from backend.pipeline.transcription.transcribers.gemini import (
    GeminiTransientTranscriptionError,
)
from backend.services.audio_segments import models as audio_segments_models


class TranscriptionEventProcessorTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.record_pipeline_stage_patch = patch(
            "backend.pipeline.transcription.processor.record_pipeline_stage"
        )
        self.mock_record_pipeline_stage = (
            self.record_pipeline_stage_patch.start()
        )

    def tearDown(self) -> None:
        self.record_pipeline_stage_patch.stop()

    async def test_process_event_success(self) -> None:
        """Verifies successful end-to-end claim-check Pub/Sub CloudEvent processing."""
        # Setup mocks
        mock_transcriber = MagicMock(spec=Transcriber)
        mock_transcriber.transcribe.return_value = "Hello world"

        mock_publisher = MagicMock()
        mock_future = Future()
        mock_future.set_result("msg-12345")
        mock_publisher.publish.return_value = mock_future
        mock_publisher.topic_path.return_value = (
            "projects/test-proj/topics/egress"
        )
        mock_audio_segments_client = MagicMock(spec=AsyncAudioSegmentsClient)

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
        await processor.process_event(cloud_event)

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
        self.mock_record_pipeline_stage.assert_any_call(
            "transcription", "start"
        )
        self.mock_record_pipeline_stage.assert_any_call(
            "transcription_status", TranscriptionStatus.ATTEMPTS
        )
        self.mock_record_pipeline_stage.assert_any_call(
            "transcription_status", TranscriptionStatus.SUCCESS
        )
        self.mock_record_pipeline_stage.assert_any_call(
            "transcription", "success"
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

    async def test_process_event_partial_transcription(self) -> None:
        """Verifies behavior when speech API returns a partial transcription error."""
        # Setup mocks
        mock_transcriber = MagicMock(spec=Transcriber)
        mock_transcriber.transcribe.side_effect = PartialTranscriptionError(
            partial_text="This is a partial", reason="MAX_TOKENS"
        )

        mock_publisher = MagicMock()
        mock_future = Future()
        mock_future.set_result("msg-12345")
        mock_publisher.publish.return_value = mock_future
        mock_publisher.topic_path.return_value = (
            "projects/test-proj/topics/egress"
        )

        mock_audio_segments_client = MagicMock(spec=AsyncAudioSegmentsClient)

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
        await processor.process_event(cloud_event)

        # Verify add_audio_segment_annotation was called with partial text and error
        mock_audio_segments_client.add_audio_segment_annotation.assert_called_once_with(
            audio_segment_id="tx-1111",
            annotation_type=audio_segments_models.AnnotationType.TRANSCRIPT,
            data={
                "text": "This is a partial",
                "errors": ["Partial transcription (MAX_TOKENS)"],
            },
        )
        self.mock_record_pipeline_stage.assert_any_call(
            "transcription", "success"
        )
        self.mock_record_pipeline_stage.assert_any_call(
            "transcription_status", TranscriptionStatus.PARTIAL
        )

    async def test_process_event_empty_transcription(self) -> None:
        """Verifies behavior when speech API returns empty transcription."""
        # Setup mocks
        mock_transcriber = MagicMock(spec=Transcriber)
        mock_transcriber.transcribe.return_value = ""

        mock_publisher = MagicMock()
        mock_future = Future()
        mock_future.set_result("msg-12345")
        mock_publisher.publish.return_value = mock_future
        mock_publisher.topic_path.return_value = (
            "projects/test-proj/topics/egress"
        )

        mock_audio_segments_client = MagicMock(spec=AsyncAudioSegmentsClient)

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
        await processor.process_event(cloud_event)

        # Verify add_audio_segment_annotation was called with error
        mock_audio_segments_client.add_audio_segment_annotation.assert_called_once_with(
            audio_segment_id="tx-1111",
            annotation_type=audio_segments_models.AnnotationType.TRANSCRIPT,
            data={
                "text": "",
                "errors": [],
            },
        )
        self.mock_record_pipeline_stage.assert_any_call(
            "transcription", "start"
        )
        self.mock_record_pipeline_stage.assert_any_call(
            "transcription_status", TranscriptionStatus.ATTEMPTS
        )
        self.mock_record_pipeline_stage.assert_any_call(
            "transcription_status", TranscriptionStatus.EMPTY
        )
        self.mock_record_pipeline_stage.assert_any_call(
            "transcription", "success"
        )

    async def test_process_event_unintelligible_marker_transcription(
        self,
    ) -> None:
        """Verifies behavior when speech API returns the unintelligible marker explicitly."""
        # Setup mocks
        mock_transcriber = MagicMock(spec=Transcriber)
        mock_transcriber.transcribe.return_value = CHIRP_UNINTELLIGIBLE_MARKER

        mock_publisher = MagicMock()
        mock_future = Future()
        mock_future.set_result("msg-12345")
        mock_publisher.publish.return_value = mock_future
        mock_publisher.topic_path.return_value = (
            "projects/test-proj/topics/egress"
        )

        mock_audio_segments_client = MagicMock(spec=AsyncAudioSegmentsClient)

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

        await processor.process_event(cloud_event)

        # Verify add_audio_segment_annotation was called without errors
        mock_audio_segments_client.add_audio_segment_annotation.assert_called_once_with(
            audio_segment_id="tx-1111",
            annotation_type=audio_segments_models.AnnotationType.TRANSCRIPT,
            data={
                "text": CHIRP_UNINTELLIGIBLE_MARKER,
                "errors": [],
            },
        )
        self.mock_record_pipeline_stage.assert_any_call(
            "transcription", "start"
        )
        self.mock_record_pipeline_stage.assert_any_call(
            "transcription_status", TranscriptionStatus.ATTEMPTS
        )
        self.mock_record_pipeline_stage.assert_any_call(
            "transcription_status", TranscriptionStatus.UNINTELLIGIBLE
        )
        self.mock_record_pipeline_stage.assert_any_call(
            "transcription", "success"
        )

    async def test_process_event_transcribe_error_silent_drop(self) -> None:
        """Verifies that a permanent exception raised during transcription is caught and silently dropped."""
        mock_transcriber = MagicMock(spec=Transcriber)
        mock_transcriber.transcribe.side_effect = ValueError(
            "Audio payload too long for synchronous API"
        )

        mock_publisher = MagicMock()
        mock_publisher.topic_path.return_value = (
            "projects/test-proj/topics/egress"
        )
        mock_audio_segments_client = MagicMock(spec=AsyncAudioSegmentsClient)

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
        await processor.process_event(cloud_event)

        self.mock_record_pipeline_stage.assert_any_call(
            "transcription", "start"
        )
        self.mock_record_pipeline_stage.assert_any_call(
            "transcription_status", TranscriptionStatus.ATTEMPTS
        )
        self.mock_record_pipeline_stage.assert_any_call(
            "transcription", "error"
        )
        self.mock_record_pipeline_stage.assert_any_call(
            "transcription_status", TranscriptionStatus.PERMANENT_ERROR
        )

        # Egress publishing must never be called (event silently dropped)
        mock_publisher.publish.assert_not_called()

        # Verify annotation was written with the permanent failure error
        mock_audio_segments_client.add_audio_segment_annotation.assert_called_once()
        call_data = mock_audio_segments_client.add_audio_segment_annotation.call_args.kwargs[
            "data"
        ]
        self.assertEqual(call_data["text"], "")
        self.assertIn("Permanent Failure", call_data["errors"][0])

    async def test_process_event_transient_error_propagates(self) -> None:
        """Verifies that a transient exception raised during transcription propagates so Pub/Sub retries."""

        class MockGrpcCallError(grpc.RpcError, grpc.Call):
            """Mock exception implementing both RpcError and grpc.Call."""

            def code(self) -> grpc.StatusCode:
                return grpc.StatusCode.UNAVAILABLE

        mock_transcriber = MagicMock(spec=Transcriber)
        grpc_err = MockGrpcCallError()
        mock_transcriber.transcribe.side_effect = grpc_err

        mock_publisher = MagicMock()
        mock_publisher.topic_path.return_value = (
            "projects/test-proj/topics/egress"
        )
        mock_audio_segments_client = MagicMock(spec=AsyncAudioSegmentsClient)

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
            await processor.process_event(cloud_event)

        # Egress publishing must never be called
        mock_publisher.publish.assert_not_called()

        # The annotation is NOT written for transient failures
        mock_audio_segments_client.add_audio_segment_annotation.assert_not_called()

    async def test_process_event_transient_failure_then_success(self) -> None:
        """Verifies a two-delivery sequence: a transient failure propagates without writing annotations, and a subsequent retry succeeds and writes the final transcript annotation."""
        mock_transcriber = MagicMock(spec=Transcriber)
        # 1st call raises transient, 2nd call returns success
        mock_transcriber.transcribe.side_effect = [
            GeminiTransientTranscriptionError("Transient API drop"),
            "Engine 41 responding",
        ]

        mock_publisher = MagicMock()
        mock_future = Future()
        mock_future.set_result("msg-12345")
        mock_publisher.publish.return_value = mock_future
        mock_publisher.topic_path.return_value = (
            "projects/test-proj/topics/egress"
        )

        mock_audio_segments_client = MagicMock(spec=AsyncAudioSegmentsClient)

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

        # --- Delivery 1: Transient Failure ---
        with self.assertRaises(GeminiTransientTranscriptionError):
            await processor.process_event(cloud_event)

        # Verify no egress published and no DB annotation written
        mock_publisher.publish.assert_not_called()
        mock_audio_segments_client.add_audio_segment_annotation.assert_not_called()

        # --- Delivery 2: Successful Retry ---
        await processor.process_event(cloud_event)

        # Verify egress was published
        mock_publisher.publish.assert_called_once()

        # Verify the annotation was written EXACTLY once with the successful transcript
        mock_audio_segments_client.add_audio_segment_annotation.assert_called_once()
        call_data = mock_audio_segments_client.add_audio_segment_annotation.call_args.kwargs[
            "data"
        ]
        self.assertEqual(call_data["text"], "Engine 41 responding")
        self.assertEqual(call_data["errors"], [])

    async def test_process_event_audio_too_long_permanent_failure(self) -> None:
        """Verifies that when audio duration exceeds the transcriber's limit,
        the transcriber raises ValueError and it is treated as a permanent failure.
        """
        mock_transcriber = MagicMock(spec=Transcriber)
        mock_transcriber.transcribe.side_effect = ValueError(
            "Audio payload too long for synchronous API"
        )
        mock_publisher = MagicMock()
        mock_audio_segments_client = MagicMock(spec=AsyncAudioSegmentsClient)

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
        await processor.process_event(cloud_event)

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

    async def test_process_event_google_api_transient_error_propagates(
        self,
    ) -> None:
        """Verifies that a transient GoogleAPICallError propagates to trigger a retry."""
        mock_transcriber = MagicMock(spec=Transcriber)
        mock_transcriber.transcribe.side_effect = ServiceUnavailable(
            "Transient backend error"
        )

        mock_publisher = MagicMock()
        mock_publisher.topic_path.return_value = (
            "projects/test-proj/topics/egress"
        )
        mock_audio_segments_client = MagicMock(spec=AsyncAudioSegmentsClient)

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
            await processor.process_event(cloud_event)

        mock_audio_segments_client.add_audio_segment_annotation.assert_not_called()

    async def test_process_event_google_api_permanent_error_silent_drop(
        self,
    ) -> None:
        """Verifies that a permanent GoogleAPICallError is caught and acknowledged without retry."""
        mock_transcriber = MagicMock(spec=Transcriber)
        mock_transcriber.transcribe.side_effect = PermissionDenied(
            "GCP Permission Denied"
        )

        mock_publisher = MagicMock()
        mock_publisher.topic_path.return_value = (
            "projects/test-proj/topics/egress"
        )
        mock_audio_segments_client = MagicMock(spec=AsyncAudioSegmentsClient)

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
        await processor.process_event(cloud_event)

        mock_publisher.publish.assert_not_called()
        mock_audio_segments_client.add_audio_segment_annotation.assert_called_once()
        call_data = mock_audio_segments_client.add_audio_segment_annotation.call_args.kwargs[
            "data"
        ]
        self.assertEqual(call_data["text"], "")
        self.assertIn("Permanent Failure", call_data["errors"][0])

    async def test_process_event_retry_error_transient_cause_propagates(
        self,
    ) -> None:
        """Verifies that a RetryError with transient cause propagates to trigger retry."""
        mock_transcriber = MagicMock(spec=Transcriber)
        cause = ServiceUnavailable("Service Unavailable")
        mock_transcriber.transcribe.side_effect = RetryError(
            "Timeout", cause=cause
        )

        mock_publisher = MagicMock()
        mock_audio_segments_client = MagicMock(spec=AsyncAudioSegmentsClient)

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

        # RetryError with ServiceUnavailable cause must propagate
        with self.assertRaises(RetryError):
            await processor.process_event(cloud_event)

        mock_publisher.publish.assert_not_called()
        mock_audio_segments_client.add_audio_segment_annotation.assert_not_called()

    async def test_process_event_retry_error_permanent_cause_silent_drop(
        self,
    ) -> None:
        """Verifies that a RetryError with permanent cause is caught and acknowledged without retry."""
        mock_transcriber = MagicMock(spec=Transcriber)
        cause = PermissionDenied("GCP Permission Denied")
        mock_transcriber.transcribe.side_effect = RetryError(
            "Timeout", cause=cause
        )

        mock_publisher = MagicMock()
        mock_audio_segments_client = MagicMock(spec=AsyncAudioSegmentsClient)

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

        # RetryError with PermissionDenied cause must be caught and swallowed cleanly
        await processor.process_event(cloud_event)

        mock_publisher.publish.assert_not_called()
        mock_audio_segments_client.add_audio_segment_annotation.assert_called_once()
        call_data = mock_audio_segments_client.add_audio_segment_annotation.call_args.kwargs[
            "data"
        ]
        self.assertEqual(call_data["text"], "")
        self.assertIn("Permanent Failure", call_data["errors"][0])

    async def test_process_event_retry_error_no_cause_propagates(
        self,
    ) -> None:
        """Verifies that a RetryError with no cause (fallback) propagates to trigger retry."""
        mock_transcriber = MagicMock(spec=Transcriber)
        mock_transcriber.transcribe.side_effect = RetryError(
            "Timeout", cause=None
        )

        mock_publisher = MagicMock()
        mock_audio_segments_client = MagicMock(spec=AsyncAudioSegmentsClient)

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

        # RetryError with no cause must propagate
        with self.assertRaises(RetryError):
            await processor.process_event(cloud_event)

        mock_publisher.publish.assert_not_called()
        mock_audio_segments_client.add_audio_segment_annotation.assert_not_called()

    async def test_process_event_requests_timeout_transient_error_propagates(
        self,
    ) -> None:
        """Verifies that requests.exceptions.Timeout during transcription propagates to trigger a retry."""
        mock_transcriber = MagicMock(spec=Transcriber)
        mock_transcriber.transcribe.side_effect = requests.exceptions.Timeout(
            "Request timed out"
        )

        mock_publisher = MagicMock()
        mock_audio_segments_client = MagicMock(spec=AsyncAudioSegmentsClient)

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
            await processor.process_event(cloud_event)

        mock_publisher.publish.assert_not_called()
        mock_audio_segments_client.add_audio_segment_annotation.assert_not_called()

    async def test_process_event_requests_connection_error_transient_error_propagates(
        self,
    ) -> None:
        """Verifies that requests.exceptions.ConnectionError during transcription propagates to trigger a retry."""
        mock_transcriber = MagicMock(spec=Transcriber)
        mock_transcriber.transcribe.side_effect = (
            requests.exceptions.ConnectionError("Connection refused")
        )

        mock_publisher = MagicMock()
        mock_audio_segments_client = MagicMock(spec=AsyncAudioSegmentsClient)

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
            await processor.process_event(cloud_event)

        mock_publisher.publish.assert_not_called()
        mock_audio_segments_client.add_audio_segment_annotation.assert_not_called()

    async def test_process_event_requests_http_500_transient_error_propagates(
        self,
    ) -> None:
        """Verifies that requests.exceptions.HTTPError (500) during transcription propagates to trigger a retry."""
        mock_transcriber = MagicMock(spec=Transcriber)

        mock_resp = MagicMock()
        mock_resp.status_code = 500
        http_err = requests.exceptions.HTTPError(
            "500 Server Error", response=mock_resp
        )
        mock_transcriber.transcribe.side_effect = http_err

        mock_publisher = MagicMock()
        mock_audio_segments_client = MagicMock(spec=AsyncAudioSegmentsClient)

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
            await processor.process_event(cloud_event)

        mock_publisher.publish.assert_not_called()
        mock_audio_segments_client.add_audio_segment_annotation.assert_not_called()

    async def test_process_event_requests_http_400_permanent_error_silent_drop(
        self,
    ) -> None:
        """Verifies that requests.exceptions.HTTPError (400) during transcription is caught and silently dropped."""
        mock_transcriber = MagicMock(spec=Transcriber)

        mock_resp = MagicMock()
        mock_resp.status_code = 400
        http_err = requests.exceptions.HTTPError(
            "400 Bad Request", response=mock_resp
        )
        mock_transcriber.transcribe.side_effect = http_err

        mock_publisher = MagicMock()
        mock_audio_segments_client = MagicMock(spec=AsyncAudioSegmentsClient)

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

        await processor.process_event(cloud_event)

        mock_publisher.publish.assert_not_called()
        mock_audio_segments_client.add_audio_segment_annotation.assert_called_once()
        call_data = mock_audio_segments_client.add_audio_segment_annotation.call_args.kwargs[
            "data"
        ]
        self.assertEqual(call_data["text"], "")
        self.assertIn("Permanent Failure", call_data["errors"][0])


class IsTransientExceptionTest(unittest.TestCase):
    """Unit tests for the is_transient_exception helper."""

    def test_google_api_call_error_transient(self) -> None:
        e = GoogleAPICallError("Resource exhausted")
        e.code = 429
        self.assertTrue(is_transient_exception(e))

        e = GoogleAPICallError("Internal error")
        e.code = 500
        self.assertTrue(is_transient_exception(e))

        e = GoogleAPICallError("Conflict")
        e.code = 409
        self.assertTrue(is_transient_exception(e))

        e = GoogleAPICallError("Bad request")
        e.code = 400
        self.assertFalse(is_transient_exception(e))

    def test_google_genai_api_error_transient(self) -> None:
        e = genai_errors.APIError(429, {})
        self.assertTrue(is_transient_exception(e))

        e = genai_errors.APIError(503, {})
        self.assertTrue(is_transient_exception(e))

        e = genai_errors.APIError(400, {})
        self.assertFalse(is_transient_exception(e))

    def test_httpx_errors_transient(self) -> None:
        mock_request = httpx.Request("GET", "https://example.com")
        e = httpx.ReadTimeout("Timeout", request=mock_request)
        self.assertTrue(is_transient_exception(e))

        e = httpx.ConnectError("Connection refused", request=mock_request)
        self.assertTrue(is_transient_exception(e))

        e = httpx.RequestError("Request failed", request=mock_request)
        self.assertTrue(is_transient_exception(e))


class TranscriptionProcessorTracingTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        # Setup in-memory provider
        self.provider = TracerProvider()
        self.exporter = InMemorySpanExporter()
        self.provider.add_span_processor(SimpleSpanProcessor(self.exporter))

        # Inject as custom provider
        self.original_provider = tracing_utils._state.custom_provider
        tracing_utils._state.custom_provider = self.provider

    def tearDown(self) -> None:
        tracing_utils._state.custom_provider = self.original_provider

    async def test_processor_span_nesting_and_attributes(self) -> None:
        """Verifies that processing a transcription event generates the correct nested tracing spans and attributes."""
        # Mocks
        mock_transcriber = MagicMock(spec=Transcriber)
        mock_transcriber.transcribe.return_value = "Test transcript text"

        mock_publisher = MagicMock()
        mock_future = Future()
        mock_future.set_result("msg-12345")
        mock_publisher.publish.return_value = mock_future
        mock_publisher.topic_path.return_value = (
            "projects/test-proj/topics/egress"
        )

        mock_audio_segments_client = MagicMock(spec=AsyncAudioSegmentsClient)

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
                "attributes": {
                    "traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
                },
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

        await processor.process_event(cloud_event)

        # Get all finished spans
        spans = self.exporter.get_finished_spans()

        # We expect 4 finished spans:
        # 1. transcribe_audio
        # 2. publish_transcribed_audio
        # 3. write_transcript_annotation
        # 4. transcribe_claim_check (parent)
        self.assertEqual(len(spans), 4)

        # Verify names
        names = [span.name for span in spans]
        self.assertIn("transcribe_audio", names)
        self.assertIn("publish_transcribed_audio", names)
        self.assertIn("write_transcript_annotation", names)
        self.assertIn("transcribe_claim_check", names)

        # Find root span and verify trace ID propagation
        root_span = next(s for s in spans if s.name == "transcribe_claim_check")
        root_span_ctx = root_span.get_span_context()
        self.assertIsNotNone(root_span_ctx)
        assert root_span_ctx is not None
        self.assertEqual(
            format(root_span_ctx.trace_id, "032x"),
            "4bf92f3577b34da6a3ce929d0e0e4736",
        )

        # Verify attributes on transcribe_audio
        transcribe_span = next(s for s in spans if s.name == "transcribe_audio")
        self.assertIsNotNone(transcribe_span.attributes)
        assert transcribe_span.attributes is not None
        self.assertEqual(
            transcribe_span.attributes.get("segment_id"), "tx-1111"
        )
        self.assertEqual(transcribe_span.attributes.get("feed_id"), "feed-2222")
        self.assertEqual(transcribe_span.attributes.get("duration_ms"), 5000)
        # Verify it is nested under root
        self.assertIsNotNone(transcribe_span.parent)
        assert transcribe_span.parent is not None
        self.assertEqual(
            transcribe_span.parent.span_id, root_span.context.span_id
        )

        # Verify nesting of other child spans
        publish_span = next(
            s for s in spans if s.name == "publish_transcribed_audio"
        )
        self.assertIsNotNone(publish_span.parent)
        assert publish_span.parent is not None
        self.assertEqual(publish_span.parent.span_id, root_span.context.span_id)

        write_span = next(
            s for s in spans if s.name == "write_transcript_annotation"
        )
        self.assertIsNotNone(write_span.parent)
        assert write_span.parent is not None
        self.assertEqual(write_span.parent.span_id, root_span.context.span_id)

    async def test_processor_span_error_recording(self) -> None:
        """Verifies that spans record errors and set status to ERROR on failure."""
        mock_transcriber = MagicMock(spec=Transcriber)
        # Simulate permanent failure by throwing ValueError
        mock_transcriber.transcribe.side_effect = ValueError(
            "Corrupt audio file"
        )

        mock_publisher = MagicMock()
        mock_audio_segments_client = MagicMock(spec=AsyncAudioSegmentsClient)
        # Mock database write to fail as well
        mock_audio_segments_client.add_audio_segment_annotation.side_effect = (
            RuntimeError("DB down")
        )

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

        envelope = {
            "message": {
                "data": base64.b64encode(claim.SerializeToString()).decode(
                    "utf-8"
                ),
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

        await processor.process_event(cloud_event)

        spans = self.exporter.get_finished_spans()

        # Verify transcribe_audio span is set to ERROR
        transcribe_span = next(s for s in spans if s.name == "transcribe_audio")
        self.assertEqual(transcribe_span.status.status_code, StatusCode.ERROR)

        # Verify write_transcript_annotation span is set to ERROR
        write_span = next(
            s for s in spans if s.name == "write_transcript_annotation"
        )
        self.assertEqual(write_span.status.status_code, StatusCode.ERROR)

        # Verify root span is set to ERROR
        root_span = next(s for s in spans if s.name == "transcribe_claim_check")
        self.assertEqual(root_span.status.status_code, StatusCode.ERROR)
