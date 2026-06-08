"""Unit tests for the NormalizationEventProcessor class."""

import base64
import io
import unittest
from unittest.mock import MagicMock, patch

import numpy as np
import soundfile as sf
from cloudevents.http.event import CloudEvent
from google.protobuf.duration_pb2 import Duration  # type: ignore
from google.protobuf.timestamp_pb2 import Timestamp  # type: ignore

from backend.pipeline.normalization.processor import (
    NormalizationEventProcessor,
)
from backend.pipeline.schema_types.normalized_audio_pb2 import NormalizedAudio
from backend.pipeline.schema_types.segmented_audio_pb2 import SegmentedAudio


class NormalizationEventProcessorTest(unittest.TestCase):
    @patch("google.cloud.storage.Client")
    @patch("google.cloud.pubsub_v1.PublisherClient")
    def setUp(
        self, mock_pubsub_client: MagicMock, mock_storage_client: MagicMock
    ) -> None:
        self.mock_gcs = mock_storage_client
        self.mock_publisher = mock_pubsub_client

        self.project_id = "test-project"
        self.canonical_bucket = "canonical-bucket"
        self.output_topic = "projects/test-project/topics/normalized-events"

        self.mock_segments_client = MagicMock()

    @patch("backend.pipeline.normalization.audio_processor.AudioProcessor")
    @patch("backend.pipeline.common.storage.gcs_uploader.GCSAudioUploader")
    def test_process_event_speech_success(
        self,
        mock_uploader_cls: MagicMock,
        mock_processor_cls: MagicMock,
    ) -> None:
        """Verifies successful speech segment downloading, normalization, uploading, and metadata persistence."""
        # Setup mocks
        mock_processor = mock_processor_cls.return_value
        mock_uploader = mock_uploader_cls.return_value

        # Mock processor output
        mock_processor_output = MagicMock()
        mock_processor_output.success = True
        mock_processor_output.flac_bytes = b"fake-flac-data"
        mock_processor_output.processed_audio = np.zeros(16000, dtype=np.int16)
        mock_processor.process_buffer.return_value = mock_processor_output
        mock_processor.export_m4a.return_value = b"fake-m4a-data"

        # Mock GCS download
        mock_blob = MagicMock()
        # Create valid 1-second dummy FLAC file bytes to satisfy soundfile
        dummy_io = io.BytesIO()
        sf.write(
            dummy_io, np.zeros(16000, dtype=np.int16), 16000, format="FLAC"
        )
        mock_blob.download_as_bytes.return_value = dummy_io.getvalue()

        self.mock_gcs.return_value.bucket.return_value.get_blob.return_value = (
            mock_blob
        )

        # Mock GCS upload
        mock_uploader.upload_audio_derivatives.return_value = (
            "gs://canonical-bucket/lossless/tx-1.flac",
            "gs://canonical-bucket/playback/tx-1.m4a",
        )

        # Mock Pub/Sub egress publish
        mock_future = MagicMock()
        mock_future.result.return_value = "msg-12345"
        self.mock_publisher.return_value.publish.return_value = mock_future
        self.mock_publisher.return_value.topic_path.return_value = (
            self.output_topic
        )

        # Build dummy SegmentedAudio claim proto with SPEECH classification
        claim = SegmentedAudio(
            segment_id="tx-1111",
            feed_id="feed-2222",
            missing_prior_context=False,
            missing_post_context=False,
            source_audio_uris=["gs://bucket/raw1.flac"],
            start_timestamp=Timestamp(seconds=1000, nanos=0),
            end_timestamp=Timestamp(seconds=1001, nanos=0),
            start_audio_offset=Duration(seconds=0, nanos=0),
            end_audio_offset=Duration(seconds=1, nanos=0),
            feed_name="Test Feed",
            external_id="ext-1234",
            audio_classification=SegmentedAudio.AUDIO_CLASSIFICATION_SPEECH,
            raw_audio_uri="gs://staging-bucket/raw_segments/tx-1111.flac",
        )

        # Serialize and wrap in CloudEvent envelope
        data_bytes = claim.SerializeToString()
        envelope = {
            "message": {
                "data": base64.b64encode(data_bytes).decode("utf-8"),
                "attributes": {},
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

        processor = NormalizationEventProcessor(
            project_id=self.project_id,
            canonical_audio_bucket=self.canonical_bucket,
            output_topic=self.output_topic,
            audio_segments_client=self.mock_segments_client,
            publisher=self.mock_publisher.return_value,
            gcs_client=self.mock_gcs.return_value,
        )

        # Run process_event
        processor.process_event(cloud_event)

        # Verify GCS download was called
        self.mock_gcs.return_value.bucket.return_value.get_blob.assert_called_once_with(
            "raw_segments/tx-1111.flac"
        )

        # Verify audio processor was called to normalize
        mock_processor.process_buffer.assert_called_once()

        # Verify uploader was called to upload lossless and playback formats
        mock_uploader.upload_audio_derivatives.assert_called_once()

        # Verify database segment persistence payload
        self.mock_segments_client.add_audio_segment.assert_called_once()
        segment_payload = self.mock_segments_client.add_audio_segment.call_args[
            0
        ][0]
        self.assertEqual(segment_payload["id"], "tx-1111")
        self.assertEqual(segment_payload["classification"], "SPEECH_DETECTED")
        self.assertEqual(
            segment_payload["start_timestamp"], "1970-01-01T00:16:40+00:00"
        )  # seconds=1000 maps to this datetime string
        self.assertEqual(
            segment_payload["canonical_audio_uri"],
            "gs://canonical-bucket/lossless/feed-2222/1970/01/01/tx-1111.flac",
        )

        # Verify Pub/Sub downstream message egress
        self.mock_publisher.return_value.publish.assert_called_once()
        publish_data = self.mock_publisher.return_value.publish.call_args[1][
            "data"
        ]
        egress_claim = NormalizedAudio()
        egress_claim.ParseFromString(publish_data)
        self.assertEqual(egress_claim.segment_id, "tx-1111")
        self.assertEqual(
            egress_claim.audio_classification,
            NormalizedAudio.AUDIO_CLASSIFICATION_SPEECH,
        )

    @patch("backend.pipeline.normalization.audio_processor.AudioProcessor")
    @patch("backend.pipeline.common.storage.gcs_uploader.GCSAudioUploader")
    def test_process_event_non_speech_success(
        self,
        mock_uploader_cls: MagicMock,
        mock_processor_cls: MagicMock,
    ) -> None:
        """Verifies successful non-speech segment processing with proper database mapping."""
        mock_processor = mock_processor_cls.return_value
        mock_uploader = mock_uploader_cls.return_value

        mock_processor_output = MagicMock()
        mock_processor_output.success = True
        mock_processor_output.flac_bytes = b"fake-flac-data"
        mock_processor_output.processed_audio = np.zeros(16000, dtype=np.int16)
        mock_processor.process_buffer.return_value = mock_processor_output
        mock_processor.export_m4a.return_value = b"fake-m4a-data"

        mock_blob = MagicMock()
        dummy_io = io.BytesIO()
        sf.write(
            dummy_io, np.zeros(16000, dtype=np.int16), 16000, format="FLAC"
        )
        mock_blob.download_as_bytes.return_value = dummy_io.getvalue()

        self.mock_gcs.return_value.bucket.return_value.get_blob.return_value = (
            mock_blob
        )

        mock_uploader.upload_audio_derivatives.return_value = (
            "gs://canonical-bucket/lossless/tx-2.flac",
            "gs://canonical-bucket/playback/tx-2.m4a",
        )

        mock_future = MagicMock()
        mock_future.result.return_value = "msg-67890"
        self.mock_publisher.return_value.publish.return_value = mock_future
        self.mock_publisher.return_value.topic_path.return_value = (
            self.output_topic
        )

        # Build dummy SegmentedAudio claim proto with NO_SPEECH classification
        claim = SegmentedAudio(
            segment_id="tx-2222",
            feed_id="feed-2222",
            missing_prior_context=False,
            missing_post_context=False,
            source_audio_uris=["gs://bucket/raw2.flac"],
            start_timestamp=Timestamp(seconds=1000, nanos=0),
            end_timestamp=Timestamp(seconds=1001, nanos=0),
            start_audio_offset=Duration(seconds=0, nanos=0),
            end_audio_offset=Duration(seconds=1, nanos=0),
            feed_name="Test Feed",
            external_id="ext-1234",
            audio_classification=SegmentedAudio.AUDIO_CLASSIFICATION_OTHER,
            raw_audio_uri="gs://staging-bucket/raw_segments/tx-2222.flac",
        )

        data_bytes = claim.SerializeToString()
        envelope = {
            "message": {
                "data": base64.b64encode(data_bytes).decode("utf-8"),
                "attributes": {},
                "messageId": "msg-2",
            }
        }

        cloud_event = CloudEvent(
            attributes={
                "type": "google.cloud.pubsub.topic.v1.messagePublished",
                "source": "test-source",
            },
            data=envelope,
        )

        processor = NormalizationEventProcessor(
            project_id=self.project_id,
            canonical_audio_bucket=self.canonical_bucket,
            output_topic=self.output_topic,
            audio_segments_client=self.mock_segments_client,
            publisher=self.mock_publisher.return_value,
            gcs_client=self.mock_gcs.return_value,
        )

        processor.process_event(cloud_event)

        # Verify database persistence classification mapping is UNCLASSIFIED for non-speech
        self.mock_segments_client.add_audio_segment.assert_called_once()
        segment_payload = self.mock_segments_client.add_audio_segment.call_args[
            0
        ][0]
        self.assertEqual(segment_payload["id"], "tx-2222")
        self.assertEqual(segment_payload["classification"], "UNCLASSIFIED")

        # Verify Pub/Sub egress has the correct classification downstream
        self.mock_publisher.return_value.publish.assert_called_once()
        publish_data = self.mock_publisher.return_value.publish.call_args[1][
            "data"
        ]
        egress_claim = NormalizedAudio()
        egress_claim.ParseFromString(publish_data)
        self.assertEqual(egress_claim.segment_id, "tx-2222")
        self.assertEqual(
            egress_claim.audio_classification,
            NormalizedAudio.AUDIO_CLASSIFICATION_OTHER,
        )


if __name__ == "__main__":
    unittest.main()
