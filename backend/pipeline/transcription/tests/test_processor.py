"""Unit tests for the TranscriptionEventProcessor class."""

import base64
import unittest
from unittest.mock import MagicMock

from cloudevents.http.event import CloudEvent
from google.protobuf.duration_pb2 import Duration  # type: ignore
from google.protobuf.timestamp_pb2 import Timestamp  # type: ignore

from backend.pipeline.schema_types.normalized_audio_pb2 import (
    NormalizedAudio,
)
from backend.pipeline.schema_types.transcribed_audio_pb2 import (
    TranscribedAudio,
)
from backend.pipeline.transcription.processor import (
    CHIRP_UNINTELLIGIBLE_MARKER,
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
            transmission_id="tx-1111",
            feed_id="feed-2222",
            missing_prior_context=False,
            missing_post_context=False,
            source_audio_uris=["gs://bucket/raw1.flac"],
            canonical_audio_uri="gs://bucket/normalized.flac",
            playback_audio_uri="gs://bucket/normalized.m4a",
            feed_name="Test Feed",
            external_id="ext-1234",
        )

        # Set timestamps
        t_start = Timestamp(seconds=1000, nanos=1000000)
        t_end = Timestamp(seconds=1005, nanos=2000000)
        claim.start_timestamp.CopyFrom(t_start)
        claim.end_timestamp.CopyFrom(t_end)

        # Set offsets
        claim.start_audio_offset.CopyFrom(Duration(seconds=0, nanos=0))
        claim.end_audio_offset.CopyFrom(Duration(seconds=5, nanos=0))

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
        self.assertEqual(out_proto.transmission_id, "tx-1111")
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
            transmission_id="tx-1111",
            feed_id="feed-2222",
            missing_prior_context=False,
            missing_post_context=False,
            source_audio_uris=["gs://bucket/raw1.flac"],
            canonical_audio_uri="gs://bucket/normalized.flac",
            playback_audio_uri="gs://bucket/normalized.m4a",
            feed_name="Test Feed",
            external_id="ext-1234",
        )

        # Set timestamps
        t_start = Timestamp(seconds=1000, nanos=1000000)
        t_end = Timestamp(seconds=1005, nanos=2000000)
        claim.start_timestamp.CopyFrom(t_start)
        claim.end_timestamp.CopyFrom(t_end)

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

        # Verify add_audio_segment_annotation was called with error
        mock_audio_segments_client.add_audio_segment_annotation.assert_called_once_with(
            audio_segment_id="tx-1111",
            annotation_type=audio_segments_models.AnnotationType.TRANSCRIPT,
            data={
                "text": (CHIRP_UNINTELLIGIBLE_MARKER),
                "errors": ["Empty transcription from Speech Model"],
            },
        )
