"""Serverless Cloud Event event processor for the Normalization Cloud Function."""

import base64
import datetime
import json
import logging
import urllib.parse

from cloudevents.http.event import CloudEvent
from google.cloud import pubsub_v1, storage

from backend.pipeline.common.clients.audio_segments_client import (
    AudioSegmentsClient,
)
from backend.pipeline.common.constants import (
    GCS_DOWNLOAD_TIMEOUT_SEC,
    NANOS_PER_SECOND,
)
from backend.pipeline.common.storage import gcs_uploader
from backend.pipeline.common.tracing_utils import with_tracer_context
from backend.pipeline.normalization import audio_processor
from backend.pipeline.schema_types.normalized_audio_pb2 import NormalizedAudio
from backend.pipeline.schema_types.segmented_audio_pb2 import (
    SegmentedAudio,
)
from backend.services.audio_segments.models import AudioClassification

logger = logging.getLogger(__name__)

DEFAULT_FFMPEG_TIMEOUT_SEC = 30

_CLASSIFICATION_MAP = {
    SegmentedAudio.AUDIO_CLASSIFICATION_UNSPECIFIED: AudioClassification.UNSPECIFIED,
    SegmentedAudio.AUDIO_CLASSIFICATION_SPEECH: AudioClassification.SPEECH,
    SegmentedAudio.AUDIO_CLASSIFICATION_OTHER: AudioClassification.OTHER,
}


class NormalizationEventProcessor:
    """Coordinates GCS downloading, VAD-preserving bandpass normalization, and derivative uploads.

    Persists segment records to AlloyDB and publishes downstream NormalizedAudio events.
    """

    def __init__(
        self,
        project_id: str,
        canonical_audio_bucket: str,
        output_topic: str,
        audio_segments_client: AudioSegmentsClient | None = None,
        publisher: pubsub_v1.PublisherClient | None = None,
        gcs_client: storage.Client | None = None,
    ) -> None:
        self.project_id = project_id
        self.canonical_audio_bucket = canonical_audio_bucket
        self.output_topic = output_topic
        self.audio_segments_client = audio_segments_client
        self.publisher = publisher or pubsub_v1.PublisherClient()
        self.gcs_client = gcs_client or storage.Client(project=self.project_id)

        # Set up specialized shared audio processing components
        self.audio_processor = audio_processor.AudioProcessor()
        self.audio_processor.setup()

        self.audio_uploader = gcs_uploader.GCSAudioUploader(
            gcs_client=self.gcs_client,
        )

    def process_event(self, cloud_event: CloudEvent) -> None:
        """Main entrypoint triggered on Pub/Sub claim message push delivery."""
        # Parse envelope from Pub/Sub CloudEvent structure
        try:
            envelope = cloud_event.data
            if not envelope or "message" not in envelope:
                logger.error("Invalid CloudEvent envelope structure.")
                return
            pubsub_message = envelope.get("message", {}) or {}
            attributes = pubsub_message.get("attributes", {}) or {}
            traceparent = attributes.get("traceparent", "")
            raw_data = pubsub_message.get("data", "")
        except Exception as e:
            logger.exception(
                "Failed to parse CloudEvent payload envelope: %s", e
            )
            return

        with with_tracer_context(
            traceparent,
            "normalize_segmented_audio",
            __name__,
        ):
            # Parse SegmentedAudio claim protobuf payload
            try:
                segmented_audio = self._parse_claim(raw_data)
            except Exception:
                # Return early to avoid infinite retries on corrupted/un-parseable payloads
                return

            segment_id = segmented_audio.segment_id
            feed_id = segmented_audio.feed_id
            raw_audio_uri = segmented_audio.raw_audio_uri

            logger.info(
                "Received SegmentedAudio claim for segment %s (feed %s, classification: %s)",
                segment_id,
                feed_id,
                segmented_audio.audio_classification,
            )

            try:
                # 1. Download raw audio bytes from GCS staging
                raw_audio_bytes = self._download_raw_audio(raw_audio_uri)

                # 2. Determine formats based on raw audio GCS URI extension, copying where possible
                lower_uri = raw_audio_uri.lower()
                if lower_uri.endswith(".flac"):
                    flac_bytes = raw_audio_bytes
                    m4a_bytes = self.audio_processor.transcode_to_m4a(
                        flac_bytes
                    )
                elif lower_uri.endswith(".m4a"):
                    m4a_bytes = raw_audio_bytes
                    flac_bytes = self.audio_processor.transcode_to_flac(
                        m4a_bytes
                    )
                else:
                    flac_bytes = self.audio_processor.transcode_to_flac(
                        raw_audio_bytes
                    )
                    m4a_bytes = self.audio_processor.transcode_to_m4a(
                        raw_audio_bytes
                    )

                # 3. Upload lossless FLAC and playback M4A to GCS canonical bucket
                dt = datetime.datetime.fromtimestamp(
                    segmented_audio.start_timestamp.seconds
                    + segmented_audio.start_timestamp.nanos / NANOS_PER_SECOND,
                    tz=datetime.UTC,
                )

                flac_path = (
                    f"lossless/{feed_id}/{dt:%Y/%m/%d}/{segment_id}.flac"
                )
                m4a_path = f"playback/{feed_id}/{dt:%Y/%m/%d}/{segment_id}.m4a"

                canonical_audio_uri = self.audio_uploader.upload_bytes(
                    data=flac_bytes,
                    bucket_name=self.canonical_audio_bucket,
                    destination_path=flac_path,
                    content_type="audio/flac",
                )
                logger.info(
                    "Uploaded stitched audio to %s", canonical_audio_uri
                )

                playback_audio_uri = self.audio_uploader.upload_bytes(
                    data=m4a_bytes,
                    bucket_name=self.canonical_audio_bucket,
                    destination_path=m4a_path,
                    content_type="audio/mp4",
                )
                logger.info("Uploaded playback audio to %s", playback_audio_uri)

                transcription_audio_uri = ""
                if (
                    segmented_audio.audio_classification
                    == SegmentedAudio.AUDIO_CLASSIFICATION_SPEECH
                ):
                    mono_flac_bytes = (
                        self.audio_processor.transcode_to_mono_flac(flac_bytes)
                    )
                    mono_flac_path = f"ephemeral/transcription/{feed_id}/{dt:%Y/%m/%d}/{segment_id}.flac"
                    transcription_audio_uri = self.audio_uploader.upload_bytes(
                        data=mono_flac_bytes,
                        bucket_name=self.canonical_audio_bucket,
                        destination_path=mono_flac_path,
                        content_type="audio/flac",
                    )
                    logger.info(
                        "Uploaded ephemeral transcription audio to %s",
                        transcription_audio_uri,
                    )

                # 4. Persist audio segment metadata record to AlloyDB database
                self._persist_segment(
                    segmented_audio=segmented_audio,
                    dt=dt,
                    canonical_audio_uri=canonical_audio_uri,
                    playback_audio_uri=playback_audio_uri,
                )

                # 5. Publish final NormalizedAudio claim-check downstream
                self._publish_downstream(
                    segmented_audio=segmented_audio,
                    canonical_audio_uri=canonical_audio_uri,
                    playback_audio_uri=playback_audio_uri,
                    transcription_audio_uri=transcription_audio_uri,
                    traceparent=traceparent,
                )

            except Exception as e:
                logger.exception(
                    "Transient or permanent failure processing segmented audio claim for segment %s (feed %s): %s",
                    segment_id,
                    feed_id,
                    e,
                )
                self._publish_dlq(
                    segmented_audio=segmented_audio,
                    error=e,
                    traceparent=traceparent,
                )

    def _download_raw_audio(self, raw_audio_uri: str) -> bytes:
        """Downloads raw audio bytes from GCS staging."""
        parsed_uri = urllib.parse.urlparse(raw_audio_uri)
        bucket_name = parsed_uri.netloc
        blob_name = parsed_uri.path.lstrip("/")

        blob = self.gcs_client.bucket(bucket_name).get_blob(
            blob_name, timeout=GCS_DOWNLOAD_TIMEOUT_SEC
        )
        if not blob:
            err_msg = f"Raw audio staging object not found: {raw_audio_uri}"
            logger.error(err_msg)
            raise FileNotFoundError(err_msg)

        return blob.download_as_bytes(timeout=GCS_DOWNLOAD_TIMEOUT_SEC)

    def _persist_segment(
        self,
        segmented_audio: SegmentedAudio,
        dt: datetime.datetime,
        canonical_audio_uri: str,
        playback_audio_uri: str,
    ) -> None:
        """Persists the segment metadata to AlloyDB."""
        if not self.audio_segments_client:
            return

        segment_id = segmented_audio.segment_id
        feed_id = segmented_audio.feed_id
        start_iso = dt.isoformat()

        end_iso = datetime.datetime.fromtimestamp(
            segmented_audio.end_timestamp.seconds
            + segmented_audio.end_timestamp.nanos / 1e9,
            tz=datetime.UTC,
        ).isoformat()

        start_offset_ms = (
            segmented_audio.start_audio_offset.seconds * 1000
            + segmented_audio.start_audio_offset.nanos // 1000000
        )
        end_offset_ms = (
            segmented_audio.end_audio_offset.seconds * 1000
            + segmented_audio.end_audio_offset.nanos // 1000000
        )

        classification_val = _CLASSIFICATION_MAP.get(
            segmented_audio.audio_classification,
            AudioClassification.UNSPECIFIED,
        ).value

        segment_payload = {
            "id": segment_id,
            "feed_id": feed_id,
            "classification": classification_val,
            "start_timestamp": start_iso,
            "end_timestamp": end_iso,
            "missing_prior_context": segmented_audio.missing_prior_context,
            "missing_post_context": segmented_audio.missing_post_context,
            "source_audio_uris": list(segmented_audio.source_audio_uris),
            "canonical_audio_uri": canonical_audio_uri,
            "playback_audio_uri": playback_audio_uri,
            "start_audio_offset": start_offset_ms / 1000.0,
            "end_audio_offset": end_offset_ms / 1000.0,
        }

        if segmented_audio.external_audio_segment_id:
            segment_payload["external_audio_segment_id"] = (
                segmented_audio.external_audio_segment_id
            )

        logger.info(
            "Saving audio segment %s record to database...",
            segment_id,
        )
        self.audio_segments_client.add_audio_segment(segment_payload)

    def _publish_downstream(
        self,
        segmented_audio: SegmentedAudio,
        canonical_audio_uri: str,
        playback_audio_uri: str,
        transcription_audio_uri: str,
        traceparent: str,
    ) -> None:
        """Publishes the egress NormalizedAudio message downstream to Pub/Sub."""
        segment_id = segmented_audio.segment_id
        feed_id = segmented_audio.feed_id

        out_proto = NormalizedAudio(
            segment_id=segment_id,
            feed_id=feed_id,
            start_timestamp=segmented_audio.start_timestamp,
            end_timestamp=segmented_audio.end_timestamp,
            missing_prior_context=segmented_audio.missing_prior_context,
            missing_post_context=segmented_audio.missing_post_context,
            source_audio_uris=segmented_audio.source_audio_uris,
            canonical_audio_uri=canonical_audio_uri,
            playback_audio_uri=playback_audio_uri,
            start_audio_offset=segmented_audio.start_audio_offset,
            end_audio_offset=segmented_audio.end_audio_offset,
            feed_name=segmented_audio.feed_name,
            audio_classification=SegmentedAudio.AudioClassification.Name(
                segmented_audio.audio_classification
            ),
            transcription_audio_uri=transcription_audio_uri,
        )

        topic_name = self.output_topic.split("/")[-1]
        topic_path = self.publisher.topic_path(self.project_id, topic_name)

        attrs: dict[str, str] = {}
        if traceparent:
            attrs["traceparent"] = traceparent

        future = self.publisher.publish(
            topic=topic_path,
            data=out_proto.SerializeToString(),
            ordering_key=feed_id,
            **attrs,
        )
        msg_id = future.result()
        logger.info(
            "Successfully normalized and published NormalizedAudio claim %s for segment %s (feed %s)",
            msg_id,
            segment_id,
            feed_id,
        )

    def _publish_dlq(
        self,
        segmented_audio: SegmentedAudio,
        error: Exception,
        traceparent: str,
    ) -> None:
        """Publishes a structured error payload to the Normalization Dead Letter Queue topic."""
        segment_id = segmented_audio.segment_id or "unknown_segment"
        feed_id = segmented_audio.feed_id or "unknown_feed"

        dlq_payload = {
            "segment_id": segment_id,
            "feed_id": feed_id,
            "error": str(error),
            "timestamp_ms": int(
                datetime.datetime.now(datetime.UTC).timestamp() * 1000
            ),
        }
        if segmented_audio.external_audio_segment_id:
            dlq_payload["external_audio_segment_id"] = (
                segmented_audio.external_audio_segment_id
            )

        data_bytes = json.dumps(dlq_payload).encode("utf-8")

        # Route to DLQ topic (e.g., projects/.../topics/normalized-audio-dlq-prod)
        topic_name = "normalized-audio-dlq-prod"
        if self.output_topic and "-dlq" not in self.output_topic:
            topic_name = f"{self.output_topic.split('/')[-1]}-dlq"
        topic_path = self.publisher.topic_path(self.project_id, topic_name)

        attrs: dict[str, str] = {"error_type": "normalization_failure"}
        if traceparent:
            attrs["traceparent"] = traceparent

        try:
            future = self.publisher.publish(
                topic=topic_path,
                data=data_bytes,
                ordering_key=feed_id,
                **attrs,
            )
            msg_id = future.result()
            logger.info(
                "Successfully routed failed segment %s (feed %s) to DLQ topic %s (Msg: %s)",
                segment_id,
                feed_id,
                topic_path,
                msg_id,
            )
        except Exception:
            logger.exception("Failed to publish to DLQ topic %s", topic_path)

    def _parse_claim(self, raw_data: str) -> SegmentedAudio:
        """Parses the base64 encoded SegmentedAudio protobuf payload."""
        try:
            data_bytes = base64.b64decode(raw_data)
            claim = SegmentedAudio()
            claim.ParseFromString(data_bytes)
        except Exception as e:
            logger.exception("Failed to parse SegmentedAudio: %s", e)
            raise
        else:
            return claim
