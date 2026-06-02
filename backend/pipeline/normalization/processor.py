"""Serverless Cloud Event event processor for the Normalization Cloud Function."""

import base64
import datetime
import io
import logging
import subprocess
import tempfile
import urllib.parse
from pathlib import Path

import numpy as np
import soundfile as sf
from cloudevents.http.event import CloudEvent
from google.cloud import pubsub_v1, storage

from backend.pipeline.common.clients.audio_segments_client import (
    AudioSegmentsClient,
)
from backend.pipeline.common.constants import MS_PER_SECOND, NANOS_PER_SECOND
from backend.pipeline.common.storage import gcs_uploader
from backend.pipeline.common.tracing_utils import with_tracer_context
from backend.pipeline.normalization import audio_processor
from backend.pipeline.schema_types.normalized_audio_pb2 import NormalizedAudio
from backend.pipeline.schema_types.segmented_audio_pb2 import (
    SegmentedAudio,
)
from backend.pipeline.schema_types.streaming_state_pb2 import (
    TimeRangeProto as TimeRange,
)

logger = logging.getLogger(__name__)

DEFAULT_FFMPEG_TIMEOUT_SEC = 30


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
        self.audio_processor = audio_processor.AudioProcessor(
            gcs_client_instance=self.gcs_client,
        )
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
                # 1. Download and decode raw audio bytes from GCS staging
                audio_data, sample_rate = self._download_and_decode(
                    raw_audio_uri
                )

                # 2. Run bandpass filters and compress high-amplitude volume spikes
                flac_bytes, processed_audio = self._normalize_buffer(
                    audio_data,
                    sample_rate,
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

                canonical_audio_uri, playback_audio_uri = (
                    self.audio_uploader.upload_audio_derivatives(
                        bucket_name=self.canonical_audio_bucket,
                        flac_path=flac_path,
                        m4a_path=m4a_path,
                        flac_bytes=flac_bytes,
                        processed_audio=processed_audio,
                        export_m4a_fn=lambda buf: (
                            self.audio_processor.export_m4a(buf, sample_rate)
                        ),
                    )
                )
                canonical_audio_uri = (
                    f"gs://{self.canonical_audio_bucket}/{flac_path}"
                )
                playback_audio_uri = (
                    f"gs://{self.canonical_audio_bucket}/{m4a_path}"
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
                    traceparent=traceparent,
                )

            except Exception as e:
                logger.exception(
                    "Transient or permanent failure processing segmented audio claim for segment %s (feed %s): %s",
                    segment_id,
                    feed_id,
                    e,
                )
                # Re-raise exception to let functions framework retry transient Pub/Sub errors
                raise

    def _download_and_decode(
        self, raw_audio_uri: str
    ) -> tuple[np.ndarray, int]:
        """Downloads raw audio bytes from GCS and decodes them to a numpy buffer."""
        parsed_uri = urllib.parse.urlparse(raw_audio_uri)
        bucket_name = parsed_uri.netloc
        blob_name = parsed_uri.path.lstrip("/")

        blob = self.gcs_client.bucket(bucket_name).get_blob(blob_name)
        if not blob:
            err_msg = f"Raw audio staging object not found: {raw_audio_uri}"
            logger.error(err_msg)
            raise FileNotFoundError(err_msg)

        raw_audio_bytes = blob.download_as_bytes()

        in_mem_file = io.BytesIO(raw_audio_bytes)
        with tempfile.NamedTemporaryFile(
            suffix=".flac", delete=False
        ) as temp_file:
            temp_filename = temp_file.name

        try:
            process = subprocess.run(
                [
                    "ffmpeg",
                    "-y",
                    "-i",
                    "pipe:0",
                    "-f",
                    "flac",
                    temp_filename,
                ],
                input=in_mem_file.getvalue(),
                capture_output=True,
                check=False,
                timeout=DEFAULT_FFMPEG_TIMEOUT_SEC,
            )
            if process.returncode != 0:
                logger.error(
                    f"ffmpeg error during audio decode: {process.stderr.decode()}"
                )
                msg = "Failed to decode audio via ffmpeg"
                raise RuntimeError(msg)

            audio_data, sample_rate = sf.read(temp_filename, dtype="int16")
        finally:
            try:
                Path(temp_filename).unlink()
            except OSError:
                pass

        return audio_data, sample_rate

    def _normalize_buffer(
        self,
        audio_data: np.ndarray,
        sample_rate: int,
    ) -> tuple[bytes, np.ndarray]:
        """Normalizes the audio buffer using a dummy segment spanning the entire length."""
        duration_ms = int(len(audio_data) / sample_rate * MS_PER_SECOND)
        dummy_segments = [TimeRange(start_ms=0, end_ms=duration_ms)]

        res = self.audio_processor.process_buffer(
            audio_data,
            sample_rate=sample_rate,
            speech_segments=dummy_segments,
        )

        if (
            not res.success
            or res.flac_bytes is None
            or res.processed_audio is None
        ):
            err_msg = "AudioProcessor normalization failed"
            raise RuntimeError(err_msg)

        return res.flac_bytes, res.processed_audio

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

        classification_val = (
            "SPEECH_DETECTED"
            if segmented_audio.audio_classification
            == SegmentedAudio.AUDIO_CLASSIFICATION_SPEECH
            else "UNCLASSIFIED"
        )

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
            audio_classification=segmented_audio.audio_classification,
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
