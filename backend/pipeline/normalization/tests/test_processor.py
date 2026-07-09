"""Unit tests for the NormalizationEventProcessor class."""

import base64
import unittest
from unittest.mock import MagicMock, patch

from cloudevents.http.event import CloudEvent
from google.protobuf.duration_pb2 import Duration
from google.protobuf.timestamp_pb2 import Timestamp

from backend.pipeline.common.constants import GCS_DOWNLOAD_TIMEOUT_SEC
from backend.pipeline.normalization.audio_processor import TranscodeResult
from backend.pipeline.normalization.processor import (
    NormalizationEventProcessor,
)
from backend.pipeline.schema_types.normalized_audio_pb2 import NormalizedAudio
from backend.pipeline.schema_types.segmented_audio_pb2 import SegmentedAudio
from backend.services.audio_segments.models import (
    AnnotationType,
    WaveformAnnotationData,
)


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

        self.record_pipeline_stage_patch = patch(
            "backend.pipeline.normalization.processor.record_pipeline_stage"
        )
        self.mock_record_pipeline_stage = (
            self.record_pipeline_stage_patch.start()
        )

    def tearDown(self) -> None:
        self.record_pipeline_stage_patch.stop()

    @patch("backend.pipeline.normalization.audio_processor.AudioProcessor")
    @patch("backend.pipeline.common.storage.gcs_uploader.GCSAudioUploader")
    def test_process_event_source_flac_success(
        self, mock_uploader_cls: MagicMock, mock_processor_cls: MagicMock
    ) -> None:
        """Verifies that if the source is FLAC, we copy FLAC directly and transcode to M4A."""
        # Setup mocks
        mock_processor = mock_processor_cls.return_value
        mock_uploader = mock_uploader_cls.return_value

        # Mock processor output
        mock_processor.transcode_to_m4a.return_value = b"fake-m4a-data"
        # Simulate MONO
        mock_processor.is_mono.return_value = True

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

        mock_processor.is_mono.assert_called_once_with(b"fake-flac-data")
        mock_processor.downmix_to_mono.assert_not_called()

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

        # Verify database persist was called with correct payload including external_audio_segment_id
        self.mock_segments_client.add_audio_segment.assert_called_once()
        saved_payload = self.mock_segments_client.add_audio_segment.call_args[
            0
        ][0]
        self.assertEqual(saved_payload["id"], "tx-1111")
        self.assertEqual(saved_payload["feed_id"], "feed-2222")
        self.assertEqual(
            saved_payload["external_audio_segment_id"], "ext-id-1234"
        )
        self.mock_record_pipeline_stage.assert_any_call(
            "normalization", "start"
        )
        self.mock_record_pipeline_stage.assert_any_call(
            "normalization", "success"
        )

    @patch("backend.pipeline.normalization.audio_processor.AudioProcessor")
    @patch("backend.pipeline.common.storage.gcs_uploader.GCSAudioUploader")
    def test_process_event_source_m4a_success(
        self, mock_uploader_cls: MagicMock, mock_processor_cls: MagicMock
    ) -> None:
        """Verifies that if the source is M4A, we copy M4A directly and transcode to FLAC."""
        # Setup mocks
        mock_processor = mock_processor_cls.return_value
        mock_uploader = mock_uploader_cls.return_value

        # Mock processor output
        mock_processor.transcode_to_flac.return_value = b"fake-flac-data"
        mock_processor.is_mono.return_value = False
        mock_processor.downmix_to_mono.return_value = b"fake-mono-flac-data"

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
        mock_processor.is_mono.assert_called_once_with(b"fake-flac-data")
        mock_processor.downmix_to_mono.assert_called_once_with(
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
        self, mock_uploader_cls: MagicMock, mock_processor_cls: MagicMock
    ) -> None:
        """Verifies that if the source is WAV, we transcode to BOTH FLAC and M4A."""
        # Setup mocks
        mock_processor = mock_processor_cls.return_value
        mock_uploader = mock_uploader_cls.return_value

        # Mock processor output
        mock_processor.transcode_derivatives.return_value = TranscodeResult(
            flac_bytes=b"fake-flac-data",
            m4a_bytes=b"fake-m4a-data",
        )
        mock_processor.is_mono.return_value = False
        mock_processor.downmix_to_mono.return_value = b"fake-mono-flac-data"

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

        # Verify we transcoded to both FLAC and M4A from raw bytes using transcode_derivatives
        mock_processor.transcode_derivatives.assert_called_once_with(
            b"fake-wav-data"
        )

        # Verify audio processor was called to transcode FLAC to mono FLAC
        mock_processor.is_mono.assert_called_once_with(b"fake-flac-data")
        mock_processor.downmix_to_mono.assert_called_once_with(
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

    @patch("backend.pipeline.normalization.audio_processor.AudioProcessor")
    @patch("backend.pipeline.common.storage.gcs_uploader.GCSAudioUploader")
    def test_process_event_non_speech_skips_ephemeral_upload(
        self,
        mock_uploader_cls: MagicMock,
        mock_processor_cls: MagicMock,
    ) -> None:
        """Verifies that if audio_classification is OTHER (non-speech), ephemeral uploads are skipped."""
        mock_processor = mock_processor_cls.return_value
        mock_uploader = mock_uploader_cls.return_value
        mock_processor.transcode_to_m4a.return_value = b"fake-m4a-data"

        mock_blob = MagicMock()
        mock_blob.download_as_bytes.return_value = b"fake-flac-data"
        self.mock_gcs.return_value.bucket.return_value.get_blob.return_value = (
            mock_blob
        )

        mock_uploader.upload_bytes.side_effect = (
            lambda data, bucket_name, destination_path, content_type: (
                f"gs://{bucket_name}/{destination_path}"
            )
        )

        mock_future = MagicMock()
        mock_future.result.return_value = "msg-12345"
        self.mock_publisher.return_value.publish.return_value = mock_future
        self.mock_publisher.return_value.topic_path.return_value = (
            self.output_topic
        )

        claim = SegmentedAudio(
            segment_id="tx-silence",
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
            audio_classification=SegmentedAudio.AUDIO_CLASSIFICATION_OTHER,
            raw_audio_uri="gs://staging-bucket/raw_segments/tx-silence.flac",
        )

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

        processor.process_event(cloud_event)

        # Transcoding to mono FLAC should NOT be called
        mock_processor.downmix_to_mono.assert_not_called()

        # Should only upload to lossless and playback, NOT ephemeral
        self.assertEqual(mock_uploader.upload_bytes.call_count, 2)

        # Verify egress message has empty transcription_audio_uri
        published_bytes = self.mock_publisher.return_value.publish.call_args[1][
            "data"
        ]
        out_proto = NormalizedAudio()
        out_proto.ParseFromString(published_bytes)
        self.assertEqual(out_proto.transcription_audio_uri, "")

    @patch("backend.pipeline.normalization.processor.with_tracer_context")
    def test_process_event_restores_trace_context(
        self, mock_with_tracer_context: MagicMock
    ) -> None:
        """Verifies that process_event extracts trace parent from different CloudEvent locations and passes them to with_tracer_context."""
        # Create a mock context manager for with_tracer_context
        mock_ctx = MagicMock()
        mock_with_tracer_context.return_value = mock_ctx

        processor = NormalizationEventProcessor(
            project_id=self.project_id,
            canonical_audio_bucket=self.canonical_bucket,
            output_topic=self.output_topic,
            audio_segments_client=self.mock_segments_client,
            publisher=self.mock_publisher.return_value,
            gcs_client=self.mock_gcs.return_value,
        )
        sentinel_exc = ValueError("Abort processing")

        with patch.object(processor, "_parse_claim", side_effect=sentinel_exc):
            # 1. Test case: trace parent in Pub/Sub attributes
            envelope1 = {
                "message": {
                    "data": "dummy",
                    "attributes": {"traceparent": "test-tp-1"},
                }
            }
            event1 = CloudEvent(
                attributes={
                    "type": "google.cloud.pubsub.topic.v1.messagePublished",
                    "source": "test-source",
                },
                data=envelope1,
            )
            try:
                processor.process_event(event1)
            except ValueError as e:
                if str(e) != "Abort processing":
                    raise

            mock_with_tracer_context.assert_called_with(
                {"traceparent": "test-tp-1"},
                "normalize_segmented_audio",
                "backend.pipeline.normalization.processor",
            )

            # 2. Test case: trace parent in CloudEvent attributes (e.g. Eventarc format)
            envelope2 = {
                "message": {
                    "data": "dummy",
                    "attributes": {},
                }
            }
            event2 = CloudEvent(
                attributes={
                    "type": "google.cloud.pubsub.topic.v1.messagePublished",
                    "source": "test-source",
                    "traceparent": "test-tp-2",
                },
                data=envelope2,
            )
            try:
                processor.process_event(event2)
            except ValueError as e:
                if str(e) != "Abort processing":
                    raise

            # It should merge traceparent into combined_attributes
            self.assertEqual(
                mock_with_tracer_context.call_args[0][0]["traceparent"],
                "test-tp-2",
            )

    def _wire_flac_speech_event(
        self, mock_uploader_cls: MagicMock, mock_processor_cls: MagicMock
    ) -> tuple[NormalizationEventProcessor, CloudEvent]:
        """Wires mocks for a FLAC/SPEECH segment; returns processor + event."""
        mock_processor = mock_processor_cls.return_value
        mock_processor.transcode_to_m4a.return_value = b"fake-m4a-data"
        mock_processor.is_mono.return_value = False
        mock_processor.downmix_to_mono.return_value = b"fake-mono-flac-data"

        mock_blob = MagicMock()
        mock_blob.download_as_bytes.return_value = b"fake-flac-data"
        self.mock_gcs.return_value.bucket.return_value.get_blob.return_value = (
            mock_blob
        )

        mock_uploader = mock_uploader_cls.return_value
        mock_uploader.upload_bytes.side_effect = (
            lambda data, bucket_name, destination_path, content_type: (
                f"gs://{bucket_name}/{destination_path}"
            )
        )

        mock_future = MagicMock()
        mock_future.result.return_value = "msg-12345"
        self.mock_publisher.return_value.publish.return_value = mock_future
        self.mock_publisher.return_value.topic_path.return_value = (
            self.output_topic
        )

        claim = SegmentedAudio(
            segment_id="tx-1111",
            feed_id="feed-2222",
            source_audio_uris=["gs://bucket/raw1.flac"],
            start_timestamp=Timestamp(seconds=1000, nanos=0),
            end_timestamp=Timestamp(seconds=1001, nanos=0),
            start_audio_offset=Duration(seconds=0, nanos=0),
            end_audio_offset=Duration(seconds=1, nanos=0),
            audio_classification=SegmentedAudio.AUDIO_CLASSIFICATION_SPEECH,
            raw_audio_uri="gs://staging-bucket/raw_segments/tx-1111.flac",
        )
        envelope = {
            "message": {
                "data": base64.b64encode(claim.SerializeToString()).decode(
                    "utf-8"
                ),
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
        return processor, cloud_event

    @patch("backend.pipeline.normalization.processor.waveform.compute_waveform")
    @patch("backend.pipeline.normalization.audio_processor.AudioProcessor")
    @patch("backend.pipeline.common.storage.gcs_uploader.GCSAudioUploader")
    def test_process_event_persists_waveform_annotation(
        self,
        mock_uploader_cls: MagicMock,
        mock_processor_cls: MagicMock,
        mock_compute_waveform: MagicMock,
    ) -> None:
        """Verifies the computed peaks are posted as a WAVEFORM annotation."""
        mock_compute_waveform.return_value = WaveformAnnotationData(
            peaks=[[0.1, 0.5, 0.25]], duration_seconds=1.5
        )
        processor, cloud_event = self._wire_flac_speech_event(
            mock_uploader_cls, mock_processor_cls
        )

        processor.process_event(cloud_event)

        mock_compute_waveform.assert_called_once_with(b"fake-flac-data")
        self.mock_segments_client.add_audio_segment_annotation.assert_called_once_with(
            "tx-1111",
            AnnotationType.WAVEFORM,
            {"peaks": [[0.1, 0.5, 0.25]], "duration_seconds": 1.5},
        )

    @patch("backend.pipeline.normalization.processor.waveform.compute_waveform")
    @patch("backend.pipeline.normalization.audio_processor.AudioProcessor")
    @patch("backend.pipeline.common.storage.gcs_uploader.GCSAudioUploader")
    def test_process_event_waveform_failure_does_not_abort(
        self,
        mock_uploader_cls: MagicMock,
        mock_processor_cls: MagicMock,
        mock_compute_waveform: MagicMock,
    ) -> None:
        """A waveform failure is swallowed; the segment still persists."""
        mock_compute_waveform.side_effect = RuntimeError("decode boom")
        processor, cloud_event = self._wire_flac_speech_event(
            mock_uploader_cls, mock_processor_cls
        )

        processor.process_event(cloud_event)

        self.mock_segments_client.add_audio_segment_annotation.assert_not_called()
        self.mock_segments_client.add_audio_segment.assert_called_once()
        self.mock_record_pipeline_stage.assert_any_call(
            "normalization", "success"
        )


if __name__ == "__main__":
    unittest.main()
