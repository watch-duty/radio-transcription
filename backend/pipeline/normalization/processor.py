"""Serverless Cloud Event event processor for the Normalization Cloud Function."""

import base64
import concurrent.futures
import datetime
import json
import logging
import os
import traceback
import urllib.parse
from dataclasses import dataclass

from cloudevents.http.event import CloudEvent
from google.cloud import pubsub_v1, storage
from opentelemetry import context

from backend.pipeline.common.clients.audio_segments_client import (
    AudioSegmentsClient,
)
from backend.pipeline.common.constants import (
    GCS_DOWNLOAD_TIMEOUT_SEC,
    NANOS_PER_SECOND,
)
from backend.pipeline.common.log_helper import record_pipeline_stage
from backend.pipeline.common.storage import gcs_uploader
from backend.pipeline.common.tracing_utils import (
    inject_otel_context,
    parse_pubsub_cloudevent,
    with_tracer_context,
)
from backend.pipeline.normalization import audio_processor, waveform
from backend.pipeline.schema_types.normalized_audio_pb2 import NormalizedAudio
from backend.pipeline.schema_types.segmented_audio_pb2 import (
    SegmentedAudio,
)
from backend.services.audio_segments.models import (
    AnnotationType,
    AudioClassification,
)

logger = logging.getLogger(__name__)

DEFAULT_FFMPEG_TIMEOUT_SEC = 30

_CLASSIFICATION_MAP = {
    SegmentedAudio.AUDIO_CLASSIFICATION_UNSPECIFIED: AudioClassification.UNSPECIFIED,
    SegmentedAudio.AUDIO_CLASSIFICATION_SPEECH: AudioClassification.SPEECH,
    SegmentedAudio.AUDIO_CLASSIFICATION_OTHER: AudioClassification.OTHER,
}

# Cloud Run allows 4 concurrent requests, and each request can spawn up to 3 parallel uploads.
# We set the global pool size to 12 to handle peak load without queuing.
_GLOBAL_UPLOAD_EXECUTOR = concurrent.futures.ThreadPoolExecutor(
    max_workers=int(os.environ.get("MAX_UPLOAD_THREADS", "12")),
    thread_name_prefix="normalization_upload_executor",
)


class DLQPublishError(RuntimeError):
    """Raised when publishing a failed claim to the Dead Letter Queue entirely fails."""

    def __init__(self, segment_id: str, topic_path: str) -> None:
        super().__init__(
            f"Failed to emit failed segment {segment_id} to DLQ topic {topic_path}"
        )


@dataclass(frozen=True)
class AudioDerivativeUris:
    """URIs for the transcoded audio files uploaded to GCS."""

    canonical_audio_uri: str
    playback_audio_uri: str
    transcription_audio_uri: str


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
        dlq_topic: str | None = None,
    ) -> None:
        self.project_id = project_id
        self.canonical_audio_bucket = canonical_audio_bucket
        self.output_topic = output_topic
        self.audio_segments_client = audio_segments_client
        self.publisher = publisher or pubsub_v1.PublisherClient()
        self.gcs_client = gcs_client or storage.Client(project=self.project_id)
        self.dlq_topic = dlq_topic or os.environ.get(
            "DLQ_TOPIC", "normalized-audio-dlq-prod"
        )

        # Set up specialized shared audio processing components
        self.audio_processor = audio_processor.AudioProcessor()
        self.audio_processor.setup()

        self.audio_uploader = gcs_uploader.GCSAudioUploader(
            gcs_client=self.gcs_client,
        )

    def process_event(self, cloud_event: CloudEvent) -> None:
        """Main entrypoint triggered on Pub/Sub claim message push delivery."""
        record_pipeline_stage("normalization", "start")
        try:
            combined_attributes, raw_data = parse_pubsub_cloudevent(cloud_event)
        except Exception as e:
            logger.exception(
                "Failed to parse CloudEvent payload envelope: %s", e
            )
            return

        with with_tracer_context(
            combined_attributes,
            "normalize_segmented_audio",
            __name__,
        ):
            # Parse SegmentedAudio claim protobuf payload
            try:
                segmented_audio = self._parse_claim(raw_data)
            except Exception:
                # Return early to avoid infinite retries on corrupted/un-parseable payloads
                record_pipeline_stage("normalization", "error")
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

                # 2. Transcode into lossless FLAC and playback M4A derivatives
                transcoded = self._transcode_audio(
                    raw_audio_uri, raw_audio_bytes
                )
                flac_bytes = transcoded.flac_bytes
                m4a_bytes = transcoded.m4a_bytes

                # 3. Upload lossless FLAC and playback M4A to GCS canonical bucket
                dt = datetime.datetime.fromtimestamp(
                    segmented_audio.start_timestamp.seconds
                    + segmented_audio.start_timestamp.nanos / NANOS_PER_SECOND,
                    tz=datetime.UTC,
                )

                uris = self._upload_audio_derivatives(
                    flac_bytes=flac_bytes,
                    m4a_bytes=m4a_bytes,
                    feed_id=feed_id,
                    dt=dt,
                    segment_id=segment_id,
                    audio_classification=segmented_audio.audio_classification,
                )

                # 4. Persist audio segment metadata record to AlloyDB database
                self._persist_segment(
                    segmented_audio=segmented_audio,
                    dt=dt,
                    canonical_audio_uri=uris.canonical_audio_uri,
                    playback_audio_uri=uris.playback_audio_uri,
                )

                # 5. Publish final NormalizedAudio claim-check downstream
                self._publish_downstream(
                    segmented_audio=segmented_audio,
                    canonical_audio_uri=uris.canonical_audio_uri,
                    playback_audio_uri=uris.playback_audio_uri,
                    transcription_audio_uri=uris.transcription_audio_uri,
                )

                # 6. Attach the timeline waveform (best-effort; never blocks the above)
                self._persist_waveform_annotation(segment_id, flac_bytes)

                record_pipeline_stage("normalization", "success")

            except Exception as e:
                # 1. Inspect delivery attempt counter from Eventarc / PubSub push envelope
                delivery_attempt = int(
                    combined_attributes.get("googclient_deliveryattempt", 0)
                    or combined_attributes.get("delivery_attempt", 0)
                    or 1
                )
                is_permanent = isinstance(
                    e,
                    (
                        ValueError,
                        TypeError,
                        FileNotFoundError,
                        AttributeError,
                        KeyError,
                        RuntimeError,
                    ),
                )
                if not is_permanent and delivery_attempt < 3:
                    logger.warning(
                        "Transient failure processing segment %s (feed %s) on attempt %d. Re-raising to let Functions Framework retry: %s",
                        segment_id,
                        feed_id,
                        delivery_attempt,
                        e,
                    )
                    record_pipeline_stage("normalization", "error")
                    raise

                logger.exception(
                    "Permanent failure (or max transient retries exhausted) processing segment %s (feed %s): %s",
                    segment_id,
                    feed_id,
                    e,
                )
                record_pipeline_stage("normalization", "error")
                self._publish_dlq(
                    segmented_audio=segmented_audio,
                    error=e,
                    delivery_attempt=delivery_attempt,
                )

    def _transcode_audio(
        self, raw_audio_uri: str, raw_audio_bytes: bytes
    ) -> audio_processor.TranscodeResult:
        """Transcodes raw bytes into lossless FLAC and streaming M4A derivatives."""
        lower_uri = raw_audio_uri.lower()
        if lower_uri.endswith(".flac"):
            return audio_processor.TranscodeResult(
                flac_bytes=raw_audio_bytes,
                m4a_bytes=self.audio_processor.transcode_to_m4a(
                    raw_audio_bytes
                ),
            )
        if lower_uri.endswith(".m4a"):
            return audio_processor.TranscodeResult(
                flac_bytes=self.audio_processor.transcode_to_flac(
                    raw_audio_bytes
                ),
                m4a_bytes=raw_audio_bytes,
            )
        return self.audio_processor.transcode_derivatives(raw_audio_bytes)

    def _upload_audio_derivatives(
        self,
        flac_bytes: bytes,
        m4a_bytes: bytes,
        feed_id: str,
        dt: datetime.datetime,
        segment_id: str,
        audio_classification: int,
    ) -> AudioDerivativeUris:
        """Uploads canonical, playback, and optionally transcription audio concurrently to GCS."""
        flac_path = f"lossless/{feed_id}/{dt:%Y/%m/%d}/{segment_id}.flac"
        m4a_path = f"playback/{feed_id}/{dt:%Y/%m/%d}/{segment_id}.m4a"

        mono_flac_bytes: bytes | None = None
        mono_flac_path: str | None = None
        ephemeral_upload_bypassed = False

        if audio_classification == SegmentedAudio.AUDIO_CLASSIFICATION_SPEECH:
            if self.audio_processor.is_mono(flac_bytes):
                logger.info(
                    "Audio is already mono. Bypassing ephemeral transcription upload."
                )
                ephemeral_upload_bypassed = True
            else:
                mono_flac_bytes = self.audio_processor.downmix_to_mono(
                    flac_bytes
                )
                mono_flac_path = f"ephemeral/transcription/{feed_id}/{dt:%Y/%m/%d}/{segment_id}.flac"

        def _upload_and_log(
            data: bytes,
            path: str,
            content_type: str,
            log_name: str,
            ctx: context.Context,
        ) -> str:
            token = context.attach(ctx)
            try:
                uri = self.audio_uploader.upload_bytes(
                    data=data,
                    bucket_name=self.canonical_audio_bucket,
                    destination_path=path,
                    content_type=content_type,
                )
                logger.info("Uploaded %s to %s", log_name, uri)
                return uri
            finally:
                context.detach(token)

        current_ctx = context.get_current()

        canonical_future = _GLOBAL_UPLOAD_EXECUTOR.submit(
            _upload_and_log,
            flac_bytes,
            flac_path,
            "audio/flac",
            "stitched audio",
            current_ctx,
        )
        playback_future = _GLOBAL_UPLOAD_EXECUTOR.submit(
            _upload_and_log,
            m4a_bytes,
            m4a_path,
            "audio/mp4",
            "playback audio",
            current_ctx,
        )

        ephemeral_upload_future = None
        if mono_flac_path and mono_flac_bytes:
            ephemeral_upload_future = _GLOBAL_UPLOAD_EXECUTOR.submit(
                _upload_and_log,
                mono_flac_bytes,
                mono_flac_path,
                "audio/flac",
                "ephemeral transcription audio",
                current_ctx,
            )

        canonical_audio_uri = canonical_future.result()
        playback_audio_uri = playback_future.result()

        transcription_audio_uri = ""
        if ephemeral_upload_future:
            transcription_audio_uri = ephemeral_upload_future.result()
        elif ephemeral_upload_bypassed:
            transcription_audio_uri = canonical_audio_uri

        return AudioDerivativeUris(
            canonical_audio_uri=canonical_audio_uri,
            playback_audio_uri=playback_audio_uri,
            transcription_audio_uri=transcription_audio_uri,
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

    def _persist_waveform_annotation(
        self, segment_id: str, flac_bytes: bytes
    ) -> None:
        """Attaches downsampled waveform peaks for timeline rendering (best-effort)."""
        if not self.audio_segments_client:
            logger.warning(
                "No audio_segments_client configured; "
                "skipping waveform annotation for segment %s",
                segment_id,
            )
            return
        try:
            data = waveform.compute_waveform(flac_bytes).model_dump()
            self.audio_segments_client.add_audio_segment_annotation(
                segment_id, AnnotationType.WAVEFORM, data
            )
        except Exception:
            # Never fail the pipeline for a missing waveform.
            logger.exception(
                "Failed to attach waveform annotation for segment %s",
                segment_id,
            )

    def _publish_downstream(
        self,
        segmented_audio: SegmentedAudio,
        canonical_audio_uri: str,
        playback_audio_uri: str,
        transcription_audio_uri: str,
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
        inject_otel_context(attrs)

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
        delivery_attempt: int = 1,
    ) -> None:
        """Publishes a structured error payload to the Normalization Dead Letter Queue topic."""
        segment_id = segmented_audio.segment_id or "unknown_segment"
        feed_id = segmented_audio.feed_id or "unknown_feed"

        dlq_payload = {
            "segment_id": segment_id,
            "feed_id": feed_id,
            "error": str(error),
            "traceback": traceback.format_exc(),
            "delivery_attempt": delivery_attempt,
            "timestamp_ms": int(
                datetime.datetime.now(datetime.UTC).timestamp() * 1000
            ),
        }
        if segmented_audio.external_audio_segment_id:
            dlq_payload["external_audio_segment_id"] = (
                segmented_audio.external_audio_segment_id
            )

        data_bytes = json.dumps(dlq_payload).encode("utf-8")

        # Option A from Claude review: make DLQ topic a first-class config parameter
        topic_path = self.publisher.topic_path(self.project_id, self.dlq_topic)

        attrs: dict[str, str] = {"error_type": "normalization_failure"}
        inject_otel_context(attrs)

        try:
            # Item #4 from Claude review: publish to DLQ with ordering_key="" so DLQ publishing never gridlocks
            future = self.publisher.publish(
                topic=topic_path,
                data=data_bytes,
                ordering_key="",
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
        except Exception as pub_err:
            logger.critical(
                "CRITICAL ALERT: Failed to publish to DLQ topic %s! Segment %s dropped entirely: %s",
                topic_path,
                segment_id,
                pub_err,
            )
            # Item #5 from Claude review: trigger secondary alert path when DLQ publish entirely fails
            raise DLQPublishError(segment_id, topic_path) from pub_err

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
