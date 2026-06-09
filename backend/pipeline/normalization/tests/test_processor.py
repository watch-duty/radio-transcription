"""Unit tests for the NormalizationEventProcessor class."""

import base64
import unittest
from unittest.mock import MagicMock, patch

from cloudevents.http.event import CloudEvent
from google.protobuf.duration_pb2 import Duration  # type: ignore
from google.protobuf.timestamp_pb2 import Timestamp  # type: ignore

from backend.pipeline.common.constants import GCS_DOWNLOAD_TIMEOUT_SEC
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
    def test_process_event_source_flac_success(
        self,
        mock_uploader_cls: MagicMock,
        mock_processor_cls: MagicMock,
    ) -> None:
        """Verifies that if the source is FLAC, we copy FLAC directly and transcode to M4A."""
        # Setup mocks
        mock_processor = mock_processor_cls.return_value
        mock_uploader = mock_uploader_cls.return_value

        # Mock processor output
        mock_processor.transcode_to_m4a.return_value = b"fake-m4a-data"
        mock_processor.transcode_to_mono_flac.return_value = (
            b"fake-mono-flac-data"
        )

        # Mock GCS download
        mock_blob = MagicMock()
        mock_blob.download_as_bytes.return_value = b"fake-flac-data"

        self.mock_gcs.return_value.bucket.return_value.get_blob.return_value = (
            mock_blob
        )

        # Mock GCS upload
        mock_uploader.upload_bytes.side_effect = (
            lambda data, bucket_name, destination_path, content_type: (
                f"gs://{bucket_name}/{destination_path}"
            )
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
            external_audio_segment_id="ext-id-1234",
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
            "raw_segments/tx-1111.flac",
            timeout=GCS_DOWNLOAD_TIMEOUT_SEC,
        )
        mock_blob.download_as_bytes.assert_called_once_with(
            timeout=GCS_DOWNLOAD_TIMEOUT_SEC
        )

        # Verify we copied FLAC directly (no transcode_to_flac called)
        mock_processor.transcode_to_flac.assert_not_called()

        # Verify audio processor was called to transcode FLAC to M4A
        mock_processor.transcode_to_m4a.assert_called_once_with(
            b"fake-flac-data"
        )

        # Verify audio processor was called to transcode FLAC to mono FLAC
        mock_processor.transcode_to_mono_flac.assert_called_once_with(
            b"fake-flac-data"
        )

        # Verify uploader was called with correct bytes
        mock_uploader.upload_bytes.assert_any_call(
            data=b"fake-flac-data",
            bucket_name=self.canonical_bucket,
            destination_path="lossless/feed-2222/1970/01/01/tx-1111.flac",
            content_type="audio/flac",
        )
        mock_uploader.upload_bytes.assert_any_call(
            data=b"fake-m4a-data",
            bucket_name=self.canonical_bucket,
            destination_path="playback/feed-2222/1970/01/01/tx-1111.m4a",
            content_type="audio/mp4",
        )
        mock_uploader.upload_bytes.assert_any_call(
            data=b"fake-mono-flac-data",
            bucket_name=self.canonical_bucket,
            destination_path="ephemeral/transcription/feed-2222/1970/01/01/tx-1111.flac",
            content_type="audio/flac",
        )

        # Verify database persist was called with correct payload including external_audio_segment_id
        self.mock_segments_client.add_audio_segment.assert_called_once()
        saved_payload = self.mock_segments_client.add_audio_segment.call_args[0][0]
        self.assertEqual(saved_payload["id"], "tx-1111")
        self.assertEqual(saved_payload["feed_id"], "feed-2222")
        self.assertEqual(saved_payload["external_audio_segment_id"], "ext-id-1234")

    @patch("backend.pipeline.normalization.audio_processor.AudioProcessor")
    @patch("backend.pipeline.common.storage.gcs_uploader.GCSAudioUploader")
    def test_process_event_source_m4a_success(
        self,
        mock_uploader_cls: MagicMock,
        mock_processor_cls: MagicMock,
    ) -> None:
        """Verifies that if the source is M4A, we copy M4A directly and transcode to FLAC."""
        # Setup mocks
        mock_processor = mock_processor_cls.return_value
        mock_uploader = mock_uploader_cls.return_value

        # Mock processor output
        mock_processor.transcode_to_flac.return_value = b"fake-flac-data"
        mock_processor.transcode_to_mono_flac.return_value = (
            b"fake-mono-flac-data"
        )

        # Mock GCS download
        mock_blob = MagicMock()
        mock_blob.download_as_bytes.return_value = b"fake-m4a-data"

        self.mock_gcs.return_value.bucket.return_value.get_blob.return_value = (
            mock_blob
        )

        # Mock GCS upload
        mock_uploader.upload_bytes.side_effect = (
            lambda data, bucket_name, destination_path, content_type: (
                f"gs://{bucket_name}/{destination_path}"
            )
        )

        # Mock Pub/Sub egress publish
        mock_future = MagicMock()
        mock_future.result.return_value = "msg-12345"
        self.mock_publisher.return_value.publish.return_value = mock_future
        self.mock_publisher.return_value.topic_path.return_value = (
            self.output_topic
        )

        # Build dummy SegmentedAudio claim proto
        claim = SegmentedAudio(
            segment_id="tx-1111",
            feed_id="feed-2222",
            missing_prior_context=False,
            missing_post_context=False,
            source_audio_uris=["gs://bucket/raw1.m4a"],
            start_timestamp=Timestamp(seconds=1000, nanos=0),
            end_timestamp=Timestamp(seconds=1001, nanos=0),
            start_audio_offset=Duration(seconds=0, nanos=0),
            end_audio_offset=Duration(seconds=1, nanos=0),
            feed_name="Test Feed",
            external_id="ext-1234",
            audio_classification=SegmentedAudio.AUDIO_CLASSIFICATION_SPEECH,
            raw_audio_uri="gs://staging-bucket/raw_segments/tx-1111.m4a",
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

        # Verify we copied M4A directly (no transcode_to_m4a called)
        mock_processor.transcode_to_m4a.assert_not_called()

        # Verify audio processor was called to transcode M4A to FLAC
        mock_processor.transcode_to_flac.assert_called_once_with(
            b"fake-m4a-data"
        )

        # Verify audio processor was called to transcode FLAC to mono FLAC
        mock_processor.transcode_to_mono_flac.assert_called_once_with(
            b"fake-flac-data"
        )

        # Verify uploads occurred correctly
        mock_uploader.upload_bytes.assert_any_call(
            data=b"fake-flac-data",
            bucket_name=self.canonical_bucket,
            destination_path="lossless/feed-2222/1970/01/01/tx-1111.flac",
            content_type="audio/flac",
        )
        mock_uploader.upload_bytes.assert_any_call(
            data=b"fake-m4a-data",
            bucket_name=self.canonical_bucket,
            destination_path="playback/feed-2222/1970/01/01/tx-1111.m4a",
            content_type="audio/mp4",
        )
        mock_uploader.upload_bytes.assert_any_call(
            data=b"fake-mono-flac-data",
            bucket_name=self.canonical_bucket,
            destination_path="ephemeral/transcription/feed-2222/1970/01/01/tx-1111.flac",
            content_type="audio/flac",
        )

        # Verify Pub/Sub egress has the correct classification downstream
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
    def test_process_event_source_wav_success(
        self,
        mock_uploader_cls: MagicMock,
        mock_processor_cls: MagicMock,
    ) -> None:
        """Verifies that if the source is WAV, we transcode to BOTH FLAC and M4A."""
        # Setup mocks
        mock_processor = mock_processor_cls.return_value
        mock_uploader = mock_uploader_cls.return_value

        # Mock processor output
        mock_processor.transcode_to_flac.return_value = b"fake-flac-data"
        mock_processor.transcode_to_m4a.return_value = b"fake-m4a-data"
        mock_processor.transcode_to_mono_flac.return_value = (
            b"fake-mono-flac-data"
        )

        # Mock GCS download
        mock_blob = MagicMock()
        mock_blob.download_as_bytes.return_value = b"fake-wav-data"

        self.mock_gcs.return_value.bucket.return_value.get_blob.return_value = (
            mock_blob
        )

        # Mock GCS upload
        mock_uploader.upload_bytes.side_effect = (
            lambda data, bucket_name, destination_path, content_type: (
                f"gs://{bucket_name}/{destination_path}"
            )
        )

        # Mock Pub/Sub egress publish
        mock_future = MagicMock()
        mock_future.result.return_value = "msg-12345"
        self.mock_publisher.return_value.publish.return_value = mock_future
        self.mock_publisher.return_value.topic_path.return_value = (
            self.output_topic
        )

        # Build dummy SegmentedAudio claim proto
        claim = SegmentedAudio(
            segment_id="tx-1111",
            feed_id="feed-2222",
            missing_prior_context=False,
            missing_post_context=False,
            source_audio_uris=["gs://bucket/raw1.wav"],
            start_timestamp=Timestamp(seconds=1000, nanos=0),
            end_timestamp=Timestamp(seconds=1001, nanos=0),
            start_audio_offset=Duration(seconds=0, nanos=0),
            end_audio_offset=Duration(seconds=1, nanos=0),
            feed_name="Test Feed",
            external_id="ext-1234",
            audio_classification=SegmentedAudio.AUDIO_CLASSIFICATION_SPEECH,
            raw_audio_uri="gs://staging-bucket/raw_segments/tx-1111.wav",
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

        # Verify we transcoded to both FLAC and M4A from raw bytes
        mock_processor.transcode_to_flac.assert_called_once_with(
            b"fake-wav-data"
        )
        mock_processor.transcode_to_m4a.assert_called_once_with(
            b"fake-wav-data"
        )

        # Verify audio processor was called to transcode FLAC to mono FLAC
        mock_processor.transcode_to_mono_flac.assert_called_once_with(
            b"fake-flac-data"
        )

        # Verify uploads occurred correctly
        mock_uploader.upload_bytes.assert_any_call(
            data=b"fake-flac-data",
            bucket_name=self.canonical_bucket,
            destination_path="lossless/feed-2222/1970/01/01/tx-1111.flac",
            content_type="audio/flac",
        )
        mock_uploader.upload_bytes.assert_any_call(
            data=b"fake-m4a-data",
            bucket_name=self.canonical_bucket,
            destination_path="playback/feed-2222/1970/01/01/tx-1111.m4a",
            content_type="audio/mp4",
        )
        mock_uploader.upload_bytes.assert_any_call(
            data=b"fake-mono-flac-data",
            bucket_name=self.canonical_bucket,
            destination_path="ephemeral/transcription/feed-2222/1970/01/01/tx-1111.flac",
            content_type="audio/flac",
        )


if __name__ == "__main__":
    unittest.main()
