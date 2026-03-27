"""Utilities for uploading audio derivatives to Google Cloud Storage."""

import logging
from collections.abc import Callable
from datetime import UTC, datetime

from google.cloud import storage
from pydub import AudioSegment

from backend.pipeline.transcription.datatypes import FlushRequest

logger = logging.getLogger(__name__)


class AudioUploader:
    """Manages audio uploads to GCS."""

    def __init__(
        self,
        gcs_client: storage.Client | None,
        stitched_audio_bucket: str | None,
        export_m4a_fn: Callable[[AudioSegment], bytes],
    ) -> None:
        self.gcs_client = gcs_client
        self.stitched_audio_bucket = stitched_audio_bucket
        self.export_m4a_fn = export_m4a_fn

    def upload_derivatives(
        self,
        request: FlushRequest,
        processed_audio: AudioSegment,
        flac_bytes: bytes,
    ) -> tuple[str | None, str | None]:
        """Uploads FLAC and M4A to GCS as configured."""
        if not self.stitched_audio_bucket:
            return None, None

        if not self.gcs_client:
            msg = "GCS client not initialized"
            raise RuntimeError(msg)

        dt = datetime.fromtimestamp(
            request.time_range.start_ms / 1000.0, tz=UTC
        )
        timestamp_str = dt.strftime("%Y%m%dT%H%M%SZ")

        flac_object_name = f"stitched/lossless/{request.feed_id}/{dt:%Y/%m/%d}/{timestamp_str}.flac"
        m4a_object_name = f"stitched/playback/{request.feed_id}/{dt:%Y/%m/%d}/{timestamp_str}.m4a"

        canonical_audio_uri = (
            f"gs://{self.stitched_audio_bucket}/{flac_object_name}"
        )
        playback_audio_uri = (
            f"gs://{self.stitched_audio_bucket}/{m4a_object_name}"
        )

        try:
            bucket = self.gcs_client.bucket(self.stitched_audio_bucket)

            # Upload FLAC
            flac_blob = bucket.blob(flac_object_name)
            flac_blob.upload_from_string(flac_bytes, content_type="audio/flac")
            logger.info("Uploaded stitched audio to %s", canonical_audio_uri)

            # Export & Upload M4A
            m4a_bytes = self.export_m4a_fn(processed_audio)
            m4a_blob = bucket.blob(m4a_object_name)
            m4a_blob.upload_from_string(m4a_bytes, content_type="audio/mp4")
            logger.info("Uploaded playback audio to %s", playback_audio_uri)

        except Exception:
            logger.exception(
                "Failed to upload audio derivatives to gs://%s/",
                self.stitched_audio_bucket,
            )
            raise

        return canonical_audio_uri, playback_audio_uri
