"""CLI tool to diagnose dropped speech segments on continuous audio feeds.

Fetches audio chunks for a feed and target timeframe from GCS, replays them
through the Silero + UL-UNAS neural pipeline (with tape backup), and reconciles
why any expected speech interval was classified as non-speech.
"""

import argparse
import datetime
import io
import logging
import os
import re
import sys
import urllib.parse
from dataclasses import dataclass

import google.auth
import numpy as np
import requests
from google.auth import impersonated_credentials
from google.auth.transport import requests as google_requests
from google.cloud import storage

from backend.pipeline.segmentation import constants as trans_constants
from backend.pipeline.segmentation.audio import processor as audio_processor
from backend.pipeline.segmentation.audio import vad

logger = logging.getLogger("diagnose_feed_drop")

# Nominal duration of a single continuous-feed audio chunk. Used only as a
# last-resort fallback when a blob carries no duration metadata and cannot be
# decoded; a wrong value here skews every downstream reconciliation window, so
# callers are warned whenever it is applied.
NOMINAL_CHUNK_DURATION_SEC = 15.0

# Identity impersonated when calling the Audio Segments API. Terraform grants
# transcription-sa-<env> roles/run.invoker on that environment's audio-segments
# Cloud Run service (terraform/modules/services/pipeline/transcription/main.tf),
# so the impersonated identity must match the environment being queried --
# impersonating the prod account against dev yields a 403. Override the derived
# value with --service-account or DIAGNOSTIC_SERVICE_ACCOUNT.
SERVICE_ACCOUNT_TEMPLATE = (
    "transcription-sa-{env}@{project}.iam.gserviceaccount.com"
)

# Per-environment GCP configuration. Keyed by the canonical environment name;
# `_canonical_env` maps user-supplied aliases (development, production) onto
# these keys. Every value is overridable via CLI flag or environment variable
# so the tool can be pointed at a new project without a source edit.
ENVIRONMENT_CONFIG = {
    "dev": {
        "bucket": "ingestion-staging-bucket-dev",
        "project": "probable-symbol-492218-i7",
        "segments_api_url": (
            "https://audio-segments-api-dev-lu3a6psyna-uc.a.run.app"
        ),
    },
    "prod": {
        "bucket": "ingestion-staging-bucket",
        "project": "automatic-hawk-481415-m9",
        "segments_api_url": (
            "https://audio-segments-api-prod-lu3a6psyna-uc.a.run.app"
        ),
    },
}


def _canonical_env(env_name: str | None) -> str:
    """Normalizes environment aliases onto ENVIRONMENT_CONFIG keys."""
    key = (env_name or "dev").lower()
    if key in ("prod", "production"):
        return "prod"
    return "dev"


@dataclass(frozen=True)
class DiagnosticConfig:
    """Resolved GCP targets for a diagnostic run.

    Attributes:
        bucket: GCS staging bucket holding raw continuous audio chunks.
        project: GCP project owning the bucket.
        segments_api_url: Base URL of the Audio Segments API.
        service_account: Service account impersonated for API calls.
    """

    bucket: str
    project: str
    segments_api_url: str
    service_account: str


@dataclass(frozen=True)
class DiagnosticUtterance:
    """Record of candidate speech utterance evaluated during replay."""

    start_time: datetime.datetime
    end_time: datetime.datetime
    status: str  # "Accepted" or "Rejected"
    rejection_reason: str | None
    duration_sec: float
    chunk_name: str


@dataclass(frozen=True)
class MissingIntervalReport:
    """Surgical reconciliation audit for a timeframe missing expected speech."""

    missing_start: datetime.datetime
    missing_end: datetime.datetime
    duration_sec: float
    primary_cause: str
    diagnostic_details: str
    actionable_recommendation: str


def _parse_timestamp(ts_str: str) -> datetime.datetime:
    """Parses ISO strings, URLs, or UNIX timestamps into UTC datetime."""
    ts_str = ts_str.strip()

    # If full URL pasted as timestamp
    if ts_str.startswith(("http://", "https://")):
        parsed_url = urllib.parse.urlparse(ts_str)
        qs = urllib.parse.parse_qs(parsed_url.query)
        if "timestamp" in qs:
            ts_str = qs["timestamp"][0]

    # Numeric UNIX timestamp (seconds or milliseconds)
    if ts_str.replace(".", "", 1).isdigit():
        ts_val = float(ts_str)
        if ts_val > 1e11:
            ts_val /= 1000.0
        return datetime.datetime.fromtimestamp(ts_val, tz=datetime.UTC)

    # Standard ISO format string
    try:
        dt = datetime.datetime.fromisoformat(ts_str)
        if dt.tzinfo is None:
            return dt.replace(tzinfo=datetime.UTC)
        return dt.astimezone(datetime.UTC)
    except ValueError:
        pass

    # Common human date/time string formats
    formats = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y/%m/%d %H:%M:%S",
        "%m/%d/%Y %H:%M:%S",
        "%H:%M:%S",
        "%H:%M",
    ]
    for fmt in formats:
        try:
            dt = datetime.datetime.strptime(ts_str, fmt)
            if fmt in ("%H:%M:%S", "%H:%M"):
                today = datetime.datetime.now(datetime.UTC).date()
                dt = datetime.datetime.combine(today, dt.time())
            if dt.tzinfo is None:
                return dt.replace(tzinfo=datetime.UTC)
            return dt.astimezone(datetime.UTC)
        except ValueError:
            continue

    msg = f"Failed to parse timestamp string {ts_str!r} into a valid datetime."
    raise ValueError(msg)


def _extract_blob_time(blob_name: str) -> datetime.datetime | None:
    """Extracts start timestamp from GCS chunk filenames."""
    match = re.search(r"(\d{8}T\d{6}Z)", blob_name)
    if match:
        try:
            return datetime.datetime.strptime(
                match.group(1), "%Y%m%dT%H%M%SZ"
            ).replace(tzinfo=datetime.UTC)
        except ValueError:
            return None
    return None


def _find_segment_blob(
    bucket: storage.Bucket, segment_id: str, feed_id: str | None
) -> storage.Blob | None:
    """Locates GCS blob corresponding to segment_id scoped by feed_id."""
    if not feed_id:
        return None

    for prefix in (f"lossless/{feed_id}/", f"playback/{feed_id}/"):
        for blob in bucket.list_blobs(prefix=prefix):
            if segment_id in blob.name:
                return blob
    return None


def _resolve_segment_via_api(
    api_url: str,
    service_account: str,
    segment_id: str,
    feed_id: str | None = None,
) -> tuple[str, datetime.datetime, datetime.datetime] | None:
    """Attempts to resolve exact timestamps from Audio Segments API."""
    try:
        source_creds, _project = google.auth.default()
        target_creds = impersonated_credentials.Credentials(
            source_creds,
            target_principal=service_account,
            target_scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
        id_creds = impersonated_credentials.IDTokenCredentials(
            target_creds,
            target_audience=api_url,
            include_email=True,
        )
        id_creds.refresh(google_requests.Request())
        token = id_creds.token

        params = {"limit": "5000"}
        if feed_id:
            params["feed_ids"] = feed_id
        if segment_id:
            params["segment_ids"] = segment_id

        resp = requests.get(
            f"{api_url}/v1/audio_segments",
            params=params,
            headers={"Authorization": f"Bearer {token}"},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        items = (
            data.get("segments", data)
            if isinstance(data, dict)
            else data
            if isinstance(data, list)
            else []
        )
        for item in items:
            if item.get("id") == segment_id:
                res_feed = item.get("feed_id", feed_id)
                st = _parse_timestamp(item.get("start_timestamp"))
                et = _parse_timestamp(item.get("end_timestamp"))
                logger.info(
                    "Resolved %s via API: feed=%s, start=%s, end=%s",
                    segment_id,
                    res_feed,
                    st.isoformat(),
                    et.isoformat(),
                )
                return res_feed, st, et
    except Exception as e:
        # Isolation point: the API lookup is a best-effort fast path and the
        # caller falls back to scanning canonical storage. Logged at warning
        # so auth/impersonation failures stay visible rather than surfacing
        # later as a misleading "segment not found".
        logger.warning(
            "Could not resolve segment %s via Audio Segments API (%s); "
            "falling back to canonical storage scan.",
            segment_id,
            e,
        )
    return None


def _resolve_blob_end_time(
    matched_blob: storage.Blob, start_time: datetime.datetime, segment_id: str
) -> datetime.datetime:
    """Extracts or decodes exact end timestamp for a segment blob."""
    if matched_blob.metadata:
        for k in ("end_timestamp", "endTimestamp"):
            if k in matched_blob.metadata:
                return _parse_timestamp(matched_blob.metadata[k])
        for k in ("duration", "duration_sec", "duration_seconds", "length_sec"):
            if k in matched_blob.metadata:
                try:
                    dur_sec = float(matched_blob.metadata[k])
                    return start_time + datetime.timedelta(seconds=dur_sec)
                except ValueError:
                    pass

    try:
        in_mem = io.BytesIO()
        matched_blob.download_to_file(in_mem)
        in_mem.seek(0)
        processor = audio_processor.SegmentationAudioProcessor()
        samples, sr = processor.decode_audio_in_memory(in_mem)
        if sr > 0 and len(samples) > 0:
            dur_sec = len(samples) / float(sr)
            return start_time + datetime.timedelta(seconds=dur_sec)
    except Exception as err:
        # Isolation point: duration is recoverable from a nominal fallback,
        # but that fallback fabricates the reconciliation window, so surface
        # the decode failure rather than hiding it at debug level.
        logger.warning(
            "Could not decode blob duration for segment %s: %s",
            segment_id,
            err,
        )

    logger.warning(
        "Segment %s has no duration metadata and could not be decoded; "
        "assuming a nominal %.1fs chunk. The reconciliation window is an "
        "estimate and any reported gap near its end may be an artifact.",
        segment_id,
        NOMINAL_CHUNK_DURATION_SEC,
    )
    return start_time + datetime.timedelta(seconds=NOMINAL_CHUNK_DURATION_SEC)


def _resolve_segment_id(
    config: DiagnosticConfig,
    segment_id: str,
    feed_id: str | None = None,
) -> tuple[str, datetime.datetime, datetime.datetime]:
    """Resolves feed_id and exact timestamps for a Segment ID."""
    api_res = _resolve_segment_via_api(
        config.segments_api_url, config.service_account, segment_id, feed_id
    )
    if api_res:
        return api_res

    client = storage.Client(project=config.project)
    canonical_bucket = config.bucket.replace("staging", "canonical")
    b = client.bucket(canonical_bucket)

    matched_blob = _find_segment_blob(b, segment_id, feed_id)
    if not matched_blob:
        msg = (
            f"Segment ID {segment_id!r} not found in canonical storage "
            f"bucket {canonical_bucket!r}."
        )
        raise ValueError(msg)

    matched_name = matched_blob.name or ""
    parts = [p for p in matched_name.split("/") if p]
    resolved_feed_id = feed_id or (parts[1] if len(parts) >= 2 else "")

    start_time = matched_blob.time_created
    if matched_blob.metadata:
        for k in ("start_timestamp", "startTimestamp"):
            if k in matched_blob.metadata:
                start_time = _parse_timestamp(matched_blob.metadata[k])
                break

    end_time = _resolve_blob_end_time(matched_blob, start_time, segment_id)
    return resolved_feed_id, start_time, end_time


class FeedDiagnosticRunner:
    """Executes GCS chunk discovery, warm VAD replay, and reconciliation."""

    def __init__(
        self, bucket_name: str, feed_id: str, project_id: str | None
    ) -> None:
        self.bucket_name = bucket_name
        self.feed_id = feed_id
        self.gcs_client = storage.Client(project=project_id)
        self.vad_engine = vad.VoiceActivityDetector()
        self.vad_engine.setup()

    def discover_audio_blobs(
        self,
        start_bound: datetime.datetime,
        end_bound: datetime.datetime,
    ) -> list[storage.Blob]:
        """Discovers and sorts GCS audio objects for active feed in bounds."""
        bucket = self.gcs_client.bucket(self.bucket_name)

        logger.info(
            "Discovering chunks in bucket '%s' for feed '%s' "
            "between %s and %s...",
            self.bucket_name,
            self.feed_id,
            start_bound.isoformat(),
            end_bound.isoformat(),
        )

        candidates = []
        # Standard raw continuous input prefixes (excluding derived output
        # prefixes like raw_segments/)
        prefixes = [
            f"bcfy_calls/{self.feed_id}/",
            f"bcfy_feeds/{self.feed_id}/",
            f"echo/{self.feed_id}/",
            f"openmhz/{self.feed_id}/",
            f"icecast/{self.feed_id}/",
            f"{self.feed_id}/",
        ]

        for prefix in prefixes:
            blobs = list(bucket.list_blobs(prefix=prefix))
            if blobs:
                candidates.extend(blobs)
                break

        if not candidates:
            logger.warning(
                "No audio blobs found in bucket '%s' for feed '%s' "
                "matching standard prefixes.",
                self.bucket_name,
                self.feed_id,
            )

        matched = []
        for blob in candidates:
            blob_time = _extract_blob_time(blob.name) or blob.time_created
            if blob_time and start_bound <= blob_time <= end_bound:
                matched.append((blob_time, blob))

        matched.sort(key=lambda x: x[0])
        return [b[1] for b in matched]

    def _decode_audio(self, blob: storage.Blob) -> tuple[np.ndarray, int]:
        """Decodes audio samples from GCS using PyAV."""
        in_mem = io.BytesIO()
        blob.download_to_file(in_mem)
        in_mem.seek(0)
        processor = audio_processor.SegmentationAudioProcessor()
        samples, sr = processor.decode_audio_in_memory(in_mem)
        mono = processor.downmix_to_mono(samples)
        return mono, sr

    def evaluate_chunk_absolute(
        self,
        blob: storage.Blob,
        chunk_start_time: datetime.datetime,
        prior_samples: np.ndarray | None,
        *,
        is_warmup: bool,
    ) -> tuple[
        list[DiagnosticUtterance],
        np.ndarray | None,
        np.ndarray,
        list[tuple[float, float]],
    ]:
        """Replays audio object to UTC datetimes."""
        if not is_warmup:
            logger.info(
                "--- Replaying Chunk: %s (Time: %s) ---",
                blob.name,
                chunk_start_time.strftime("%H:%M:%S"),
            )

        samples, sr = self._decode_audio(blob)
        if len(samples) == 0:
            if not is_warmup:
                logger.warning("Chunk contains zero audio samples.")
            return [], None, np.array([], dtype=np.float32), []

        detection = self.vad_engine.detect_speech_segments_with_diagnostics(
            samples,
            sample_rate=sr,
            prior_audio=prior_samples,
        )
        accepted_segments = detection.accepted_segments
        rejected_details = detection.rejected_segments
        preprocessed = detection.preprocessed_audio

        prim_native = int(trans_constants.VAD_DEFAULT_PRIMING_SEC * sr)
        next_prior_tail = samples[-prim_native:] if len(samples) > 0 else None

        utterances = []
        for start_sec, end_sec in accepted_segments:
            abs_start = chunk_start_time + datetime.timedelta(seconds=start_sec)
            abs_end = chunk_start_time + datetime.timedelta(seconds=end_sec)
            utterances.append(
                DiagnosticUtterance(
                    start_time=abs_start,
                    end_time=abs_end,
                    status="Accepted",
                    rejection_reason=None,
                    duration_sec=end_sec - start_sec,
                    chunk_name=blob.name or "unknown",
                )
            )

        for start_sec, end_sec, reason in rejected_details:
            abs_start = chunk_start_time + datetime.timedelta(seconds=start_sec)
            abs_end = chunk_start_time + datetime.timedelta(seconds=end_sec)
            utterances.append(
                DiagnosticUtterance(
                    start_time=abs_start,
                    end_time=abs_end,
                    status="Rejected",
                    rejection_reason=reason,
                    duration_sec=end_sec - start_sec,
                    chunk_name=blob.name or "unknown",
                )
            )

        # Plot probabilities against the same signal the detector judged:
        # 16 kHz, denoised, peak-normalized. Falls back to raw scaling only
        # when VAD was skipped outright (silent/empty chunk), in which case
        # there are no segments to reconcile against anyway.
        if preprocessed is None:
            preprocessed = (
                samples.astype(np.float32) / 32768.0
                if np.issubdtype(samples.dtype, np.integer)
                else samples
            )
        _, probs_list = self.vad_engine.extract_vad_frame_probs(
            preprocessed,
            chunk_size=vad.DEFAULT_SILERO_WINDOW_SIZE,
            context_size=64,
        )
        return utterances, next_prior_tail, preprocessed, probs_list

    def execute_warm_replay(
        self,
        blobs: list[storage.Blob],
        warmup_end_time: datetime.datetime,
        target_end: datetime.datetime | None = None,
    ) -> tuple[
        list[DiagnosticUtterance],
        list[tuple[float, float, float]],
    ]:
        """Replays blobs continuously, separating warmup from reporting."""
        prior_tail = None
        live_utterances = []
        live_probs = []

        warmup_count = 0
        live_count = 0

        target_dur = (
            (target_end - warmup_end_time).total_seconds()
            if target_end
            else float("inf")
        )

        for blob in blobs:
            blob_time = (
                _extract_blob_time(blob.name or "")
                or blob.time_created
                or datetime.datetime.now(tz=datetime.UTC)
            )
            chunk_end_time = blob_time + datetime.timedelta(
                seconds=NOMINAL_CHUNK_DURATION_SEC
            )
            is_warmup = chunk_end_time <= warmup_end_time

            if is_warmup:
                warmup_count += 1
            else:
                live_count += 1

            (
                utts,
                prior_tail,
                preprocessed,
                probs_list,
            ) = self.evaluate_chunk_absolute(
                blob,
                blob_time,
                prior_tail,
                is_warmup=is_warmup,
            )

            for off_sec, prob in probs_list:
                frame_time = blob_time + datetime.timedelta(seconds=off_sec)
                rel_sec = (frame_time - warmup_end_time).total_seconds()
                if 0.0 <= rel_sec <= target_dur:
                    frame_idx = int(off_sec * vad.TARGET_SAMPLE_RATE)
                    window_samples = preprocessed[
                        frame_idx : frame_idx + vad.DEFAULT_SILERO_WINDOW_SIZE
                    ]
                    amp = (
                        float(np.max(np.abs(window_samples)))
                        if len(window_samples) > 0
                        else 0.0
                    )
                    live_probs.append((rel_sec, amp, prob))

            for u in utts:
                if u.end_time >= warmup_end_time and (
                    target_end is None or u.start_time <= target_end
                ):
                    live_utterances.append(u)
                    if u.status == "Accepted":
                        logger.info(
                            "  [ACCEPTED] Speech Segment: (%s to %s) | "
                            "Duration: %.2fs",
                            u.start_time.strftime("%H:%M:%S.%f")[:-4],
                            u.end_time.strftime("%H:%M:%S.%f")[:-4],
                            u.duration_sec,
                        )
                    else:
                        logger.warning(
                            "  [REJECTED] Candidate Slice: (%s to %s) | "
                            "Reason: %s",
                            u.start_time.strftime("%H:%M:%S.%f")[:-4],
                            u.end_time.strftime("%H:%M:%S.%f")[:-4],
                            u.rejection_reason,
                        )

        logger.info(
            "Tape Backup Warmup complete: streamed %d pre-roll chunks "
            "before %d live reporting chunks.",
            warmup_count,
            live_count,
        )
        return live_utterances, live_probs

    def _merge_accepted_intervals(
        self,
        target_start: datetime.datetime,
        target_end: datetime.datetime,
        live_utterances: list[DiagnosticUtterance],
    ) -> list[tuple[datetime.datetime, datetime.datetime]]:
        """Isolates and merges accepted speech intervals in target window."""
        accepted_intervals = []
        for u in live_utterances:
            if u.status == "Accepted":
                i_start = max(target_start, u.start_time)
                i_end = min(target_end, u.end_time)
                if i_start < i_end:
                    accepted_intervals.append((i_start, i_end))

        accepted_intervals.sort(key=lambda x: x[0])
        merged_accepted = []
        for start, end in accepted_intervals:
            if merged_accepted and merged_accepted[-1][1] >= start:
                merged_accepted[-1] = (
                    merged_accepted[-1][0],
                    max(merged_accepted[-1][1], end),
                )
            else:
                merged_accepted.append((start, end))
        return merged_accepted

    def _compute_missing_intervals(
        self,
        target_start: datetime.datetime,
        target_end: datetime.datetime,
        merged_accepted: list[tuple[datetime.datetime, datetime.datetime]],
    ) -> list[tuple[datetime.datetime, datetime.datetime]]:
        """Computes sub-intervals within timeframe not categorized as speech."""
        missing = []
        curr = target_start
        for a_start, a_end in merged_accepted:
            if curr < a_start:
                missing.append((curr, a_start))
            curr = max(curr, a_end)
        if curr < target_end:
            missing.append((curr, target_end))
        return missing

    def _audit_missing_interval(
        self,
        m_start: datetime.datetime,
        m_end: datetime.datetime,
        dur: float,
        live_utterances: list[DiagnosticUtterance],
        live_probs: list[tuple[float, float, float]] | None = None,
        target_start: datetime.datetime | None = None,
    ) -> MissingIntervalReport:
        """Audits missing interval cause."""
        rejections_in_window = [
            u
            for u in live_utterances
            if u.status == "Rejected"
            and max(m_start, u.start_time) < min(m_end, u.end_time)
        ]

        if rejections_in_window:
            # Sort by overlap duration descending for primary rejection
            rejections_in_window.sort(
                key=lambda u: (
                    min(m_end, u.end_time) - max(m_start, u.start_time)
                ).total_seconds(),
                reverse=True,
            )
            rej = rejections_in_window[0]
            cause = (
                f"Post-VAD Rejection Heuristic Failure ({rej.rejection_reason})"
            )
            name = rej.chunk_name
            details = (
                "Silero VAD successfully detected speech in this timeframe, "
                f"but it was discarded by post-VAD checks in chunk {name}."
            )
            rec = (
                "To retain this audio, tune the specific post-VAD rejection "
                "limit (e.g., lower min_rms_threshold or increase "
                "spikiness_ratio_threshold)."
            )
        else:
            # Check energy floor within missing interval for silence
            max_amp = 0.0
            max_p = 0.0
            if live_probs and target_start:
                m_rel_s = (m_start - target_start).total_seconds()
                m_rel_e = (m_end - target_start).total_seconds()
                window_probs = [
                    (amp, p)
                    for rel_s, amp, p in live_probs
                    if m_rel_s <= rel_s <= m_rel_e
                ]
                if window_probs:
                    max_amp = max(amp for amp, _ in window_probs)
                    max_p = max(p for _, p in window_probs)

            min_rms = self.vad_engine.min_rms_threshold
            onset_thresh = self.vad_engine.threshold_onset

            if max_amp < min_rms and max_p < onset_thresh:
                cause = "Genuine Silence / Inactive Audio Channel"
                details = (
                    "The audio signal during this timeframe contains only "
                    f"silence or static below floor (max_amp={max_amp:.6f} < "
                    f"threshold={min_rms:.6f})."
                )
                rec = (
                    "No action required if silence is expected. If speech "
                    "was expected, verify feed line gain or squelch."
                )
            else:
                cause = (
                    "Silero VAD Neural Network Probability Drop "
                    "(Whisper or Quelled Onset)"
                )
                details = (
                    "Silero VAD's neural classification probability "
                    f"failed to cross threshold_onset ({onset_thresh:.2f}) "
                    f"during this timeframe (max p={max_p:.2f})."
                )
                suggested_thresh = max(0.15, onset_thresh - 0.07)
                rec = (
                    "To capture quiet vocal onsets, reduce "
                    f"threshold_onset (currently {onset_thresh:.2f}, e.g. "
                    f"to {suggested_thresh:.2f}) or raise peak normalization."
                )

        return MissingIntervalReport(
            missing_start=m_start,
            missing_end=m_end,
            duration_sec=dur,
            primary_cause=cause,
            diagnostic_details=details,
            actionable_recommendation=rec,
        )

    def reconcile_timeframe(
        self,
        target_start: datetime.datetime,
        target_end: datetime.datetime,
        live_utterances: list[DiagnosticUtterance],
        live_probs: list[tuple[float, float, float]] | None = None,
    ) -> list[MissingIntervalReport]:
        """Reconciles detected speech intervals against requested timeframe."""
        logger.info(
            "========================================================================"
        )
        logger.info("TIMEFRAME RECONCILIATION REPORT")
        logger.info(
            "Expected Speech Window: %s to %s",
            target_start.strftime("%H:%M:%S.%f")[:-4],
            target_end.strftime("%H:%M:%S.%f")[:-4],
        )
        logger.info(
            "========================================================================"
        )

        merged_accepted = self._merge_accepted_intervals(
            target_start, target_end, live_utterances
        )
        missing_intervals = self._compute_missing_intervals(
            target_start, target_end, merged_accepted
        )

        if not missing_intervals:
            logger.info(
                "  [MATCH] 100%% of expected speech window was detected and "
                "emitted as valid speech."
            )
            return []

        logger.warning(
            "  [DISCREPANCY] Identified %d non-speech interval(s) within "
            "requested timeframe:",
            len(missing_intervals),
        )

        reports = []
        for m_start, m_end in missing_intervals:
            dur = (m_end - m_start).total_seconds()
            report = self._audit_missing_interval(
                m_start,
                m_end,
                dur,
                live_utterances,
                live_probs=live_probs,
                target_start=target_start,
            )
            reports.append(report)

            t_str = (
                f"({report.missing_start.strftime('%H:%M:%S.%f')[:-4]} to "
                f"{report.missing_end.strftime('%H:%M:%S.%f')[:-4]}) | "
                f"Duration: {report.duration_sec:.2f}s"
            )
            logger.warning("  --- Missing Interval: %s ---", t_str)
            logger.info("      Primary Cause: %s", report.primary_cause)
            logger.info("      Details:       %s", report.diagnostic_details)
            logger.info(
                "      Action:        %s", report.actionable_recommendation
            )

        return reports


def _compute_overlap_fraction(
    target_start: datetime.datetime,
    bin_start_sec: float,
    bin_end_sec: float,
    accepted_segments: list[tuple[datetime.datetime, datetime.datetime]],
    user_speech_ranges: list[tuple[float, float]],
) -> float:
    """Computes the fraction of a bin where accepted and expected agree.

    Args:
        target_start: Absolute time that relative offsets are measured from.
        bin_start_sec: Bin start, in seconds relative to target_start.
        bin_end_sec: Bin end, in seconds relative to target_start.
        accepted_segments: Absolute intervals the detector emitted as speech.
        user_speech_ranges: Expected speech ranges, relative to target_start.

    Returns:
        Fraction of the bin in [0.0, 1.0] covered by both an accepted segment
        and an expected range.
    """
    bin_dur = max(bin_end_sec - bin_start_sec, 0.001)
    intersection_sec = 0.0
    for a_s, a_e in accepted_segments:
        a_rel_s = (a_s - target_start).total_seconds()
        a_rel_e = (a_e - target_start).total_seconds()
        for u_s, u_e in user_speech_ranges:
            inter_s = max(bin_start_sec, a_rel_s, u_s)
            inter_e = min(bin_end_sec, a_rel_e, u_e)
            if inter_s < inter_e:
                intersection_sec += inter_e - inter_s
    return min(intersection_sec / bin_dur, 1.0)


# Timeline bin labels keyed by (vad_accepted, user_expected) when the caller
# supplied expected-speech ranges to score against.
_CONFUSION_MATRIX_LABELS = {
    (True, True): ("[TP]", "BOTH (MATCH)"),
    (False, True): ("[FN]", "USER ONLY"),
    (True, False): ("[FP]", "VAD ONLY"),
    (False, False): ("[TN]", "SILENCE"),
}


def _classify_timeline_bin(
    prob: float,
    onset_threshold: float,
    *,
    is_accepted: bool,
    is_user: bool,
    has_user_ranges: bool,
) -> tuple[str, str]:
    """Returns the (tag, status) label for a single timeline bin.

    When the caller supplied expected-speech ranges the bin is scored as a
    confusion-matrix cell against them; otherwise it reports whether the VAD
    emitted the bin, crossed onset without emitting, or stayed silent.

    Args:
        prob: Mean Silero speech probability across the bin.
        onset_threshold: Configured VAD onset threshold to compare against.
        is_accepted: Whether an emitted speech segment overlaps the bin.
        is_user: Whether an expected speech range overlaps the bin.
        has_user_ranges: Whether expected ranges were supplied at all.

    Returns:
        A (tag, status) pair for display in the timeline chart.
    """
    if has_user_ranges:
        return _CONFUSION_MATRIX_LABELS[(is_accepted, is_user)]

    if is_accepted:
        return "[V]", "VAD ACCEPTED"
    if prob >= onset_threshold:
        return "[?]", "VAD UNEMITTED"
    return "[.]", "SILENCE"


def _render_ascii_vad_timeline(
    target_start: datetime.datetime,
    target_end: datetime.datetime,
    frame_probs: list[tuple[float, float, float]],
    accepted_segments: list[tuple[datetime.datetime, datetime.datetime]],
    onset_threshold: float,
    user_speech_ranges: list[tuple[float, float]] | None = None,
) -> None:
    """Renders a visual ASCII VAD probability timeline chart in console logs."""
    logger.info("")
    logger.info(
        "========================================================================"
    )
    logger.info("  VISUAL VAD PROBABILITY & SPEECH TIMELINE CHART")
    logger.info(
        "========================================================================"
    )
    logger.info(
        "  Rel Time  |     Silero p(speech)        | Overlap |   Match Status  "
    )
    logger.info(
        "------------+-----------------------------+---------+-----------------"
    )

    total_dur = max((target_end - target_start).total_seconds(), 1.0)
    step_size = max(total_dur / 25.0, 0.4)
    t = 0.0

    while t <= total_dur:
        bin_start_sec = t
        bin_end_sec = min(t + step_size, total_dur)

        curr_start = target_start + datetime.timedelta(seconds=bin_start_sec)
        curr_end = target_start + datetime.timedelta(seconds=bin_end_sec)

        matching_probs = [
            p
            for rel_s, _, p in frame_probs
            if bin_start_sec <= rel_s <= bin_end_sec
        ]
        prob = (
            sum(matching_probs) / len(matching_probs) if matching_probs else 0.0
        )

        bar_len = min(round(prob * 20.0), 20)
        bar_str = "[" + "█" * bar_len + "░" * (20 - bar_len) + "]"

        is_accepted = any(
            (max(a_s, curr_start) < min(a_e, curr_end))
            for a_s, a_e in accepted_segments
        )
        is_user = bool(user_speech_ranges) and any(
            (max(u_s, bin_start_sec) < min(u_e, bin_end_sec))
            for u_s, u_e in (user_speech_ranges or [])
        )

        overlap_frac = 0.0
        if user_speech_ranges and accepted_segments:
            overlap_frac = _compute_overlap_fraction(
                target_start,
                bin_start_sec,
                bin_end_sec,
                accepted_segments,
                user_speech_ranges,
            )

        tag, status = _classify_timeline_bin(
            prob,
            onset_threshold,
            is_accepted=is_accepted,
            is_user=is_user,
            has_user_ranges=bool(user_speech_ranges),
        )

        logger.info(
            "  [+%05.2fs] | %s (%0.2f) |  %0.2f   | %-4s %-12s",
            t,
            bar_str,
            prob,
            overlap_frac,
            tag,
            status,
        )
        t += step_size

    logger.info(
        "========================================================================"
    )


def _build_arg_parser() -> argparse.ArgumentParser:
    """Builds argument parser for feed drop diagnostic tool."""
    parser = argparse.ArgumentParser(
        description=(
            "Diagnose dropped speech and timeframe discrepancies on "
            "production feeds with tape backup."
        )
    )
    parser.add_argument(
        "--user-speech",
        type=str,
        required=False,
        help=(
            "User expected speech relative offset ranges specific to the "
            "target segment (e.g. '0.873-1.565,2.900-3.500' or "
            "'00:00:873-00:01:565', where 0.0s is the start of the target "
            "segment; do NOT include tape pre-roll)."
        ),
    )
    parser.add_argument(
        "--url",
        type=str,
        required=False,
        help=(
            "Full Watch Duty web app transcript URL "
            "(e.g. https://radio.watchduty.org/transcripts?feedId=...&segmentId=...)."
        ),
    )
    parser.add_argument(
        "--segment-id",
        "--segment",
        type=str,
        required=False,
        help="Target Segment ID UUID (copied from UI Segment Info popover).",
    )
    parser.add_argument(
        "--start-segment-id",
        type=str,
        required=False,
        help="Start Segment ID UUID defining beginning of target timeframe.",
    )
    parser.add_argument(
        "--end-segment-id",
        type=str,
        required=False,
        help="End Segment ID UUID defining end of target timeframe.",
    )
    parser.add_argument(
        "--feed-id",
        type=str,
        required=False,
        help="Unique feed identifier (UUID or broadcastify ID).",
    )
    parser.add_argument(
        "--env",
        "--environment",
        type=str,
        required=False,
        default=os.environ.get("ENVIRONMENT") or os.environ.get("ENV") or "dev",
        choices=["dev", "development", "staging", "prod", "production"],
        help="Deployment environment (dev, staging, prod). Defaults to dev.",
    )
    parser.add_argument(
        "--bucket",
        "--staging-bucket",
        "--continuous-bucket",
        type=str,
        required=False,
        default=os.environ.get("AUDIO_STAGING_BUCKET"),
        dest="bucket",
        help=(
            "GCS staging bucket where raw audio chunks are written "
            "(defaults to AUDIO_STAGING_BUCKET env var or --env preset)."
        ),
    )
    parser.add_argument(
        "--service-account",
        type=str,
        required=False,
        default=os.environ.get("DIAGNOSTIC_SERVICE_ACCOUNT"),
        help=(
            "Service account to impersonate for Audio Segments API calls "
            "(defaults to DIAGNOSTIC_SERVICE_ACCOUNT env var, then the "
            "built-in transcription service account)."
        ),
    )
    parser.add_argument(
        "--segments-api-url",
        type=str,
        required=False,
        default=os.environ.get("AUDIO_SEGMENTS_API_URL"),
        help=(
            "Base URL of the Audio Segments API (defaults to "
            "AUDIO_SEGMENTS_API_URL env var or the --env preset)."
        ),
    )
    parser.add_argument(
        "--expected-start",
        type=str,
        required=False,
        help=(
            "Ground-truth expected speech start timestamp "
            "(ISO format or UNIX timestamp)."
        ),
    )
    parser.add_argument(
        "--expected-end",
        type=str,
        required=False,
        help="Ground-truth expected speech end timestamp.",
    )
    parser.add_argument(
        "--timestamp",
        type=str,
        required=False,
        help=(
            "Shorthand point-in-time target timestamp "
            "(Alternative to --expected-start / --expected-end)."
        ),
    )
    parser.add_argument(
        "--backup-sec",
        type=float,
        default=45.0,
        help=(
            "Seconds of preceding audio 'tape' to sequentially replay to "
            "warm up denoiser and VAD recurrent states (default: 45.0s)."
        ),
    )
    parser.add_argument(
        "--window-sec",
        type=float,
        default=120.0,
        help=(
            "Search window in seconds to retrieve around target timeframe "
            "(default: 120s)."
        ),
    )
    parser.add_argument(
        "--project",
        type=str,
        required=False,
        default=None,
        help="GCP Project ID associated with the Google Cloud Storage bucket.",
    )
    return parser


def _resolve_config(args: argparse.Namespace) -> DiagnosticConfig:
    """Resolves GCP targets from --env presets, flags, and env vars."""
    env_key = _canonical_env(args.env)
    env_defaults = ENVIRONMENT_CONFIG[env_key]
    project = args.project or env_defaults["project"]
    # Derive the impersonation target from the same environment as the API URL
    # so the two can never drift apart.
    default_service_account = SERVICE_ACCOUNT_TEMPLATE.format(
        env=env_key, project=env_defaults["project"]
    )
    return DiagnosticConfig(
        bucket=args.bucket or env_defaults["bucket"],
        project=project,
        segments_api_url=(
            args.segments_api_url or env_defaults["segments_api_url"]
        ),
        service_account=args.service_account or default_service_account,
    )


def _resolve_url_query_params(
    url_target: str | None,
    feed_id: str | None,
    timestamp_arg: str | None,
    segment_id: str | None = None,
) -> tuple[str | None, str | None, str | None]:
    """Extracts feedId, timestamp, and segmentId from transcript URL if
    present.
    """
    if not url_target:
        return feed_id, timestamp_arg, segment_id

    parsed_url = urllib.parse.urlparse(url_target)
    qs = urllib.parse.parse_qs(parsed_url.query)
    is_url = feed_id and feed_id.startswith(("http://", "https://"))
    res_feed = feed_id
    if "feedId" in qs and (not res_feed or is_url):
        res_feed = qs["feedId"][0]
    elif "feed_id" in qs and (not res_feed or is_url):
        res_feed = qs["feed_id"][0]

    res_ts = timestamp_arg
    if "timestamp" in qs and not res_ts:
        res_ts = qs["timestamp"][0]

    res_seg = segment_id
    if "segmentId" in qs and not res_seg:
        res_seg = qs["segmentId"][0]
    elif "segment_id" in qs and not res_seg:
        res_seg = qs["segment_id"][0]

    return res_feed, res_ts, res_seg


def _resolve_target_timeframe(
    args: argparse.Namespace, config: DiagnosticConfig
) -> tuple[str, datetime.datetime, datetime.datetime] | None:
    """Resolves target feed_id, start_time, and end_time from arguments."""
    feed_id = args.feed_id
    timestamp_arg = args.timestamp
    expected_start = args.expected_start
    expected_end = args.expected_end
    segment_id = args.segment_id

    url_target = args.url or (
        feed_id
        if feed_id and feed_id.startswith(("http://", "https://"))
        else None
    )
    feed_id, timestamp_arg, segment_id = _resolve_url_query_params(
        url_target, feed_id, timestamp_arg, segment_id
    )

    has_segment_args = bool(
        segment_id or args.start_segment_id or args.end_segment_id
    )
    if has_segment_args and not feed_id:
        logger.error(
            "When specifying --segment-id, you must also provide --feed-id "
            "or a transcript --url so GCS storage lookups are scoped to "
            "the specific feed."
        )
        return None

    seg_target_start = None
    seg_target_end = None
    if segment_id:
        res_feed, res_start, res_end = _resolve_segment_id(
            config, segment_id, feed_id=feed_id
        )
        feed_id = feed_id or res_feed
        seg_target_start = res_start
        seg_target_end = res_end
        timestamp_arg = timestamp_arg or res_start.isoformat()

    if args.start_segment_id:
        res_feed, res_start, _ = _resolve_segment_id(
            config, args.start_segment_id, feed_id=feed_id
        )
        feed_id = feed_id or res_feed
        expected_start = expected_start or res_start.isoformat()

    if args.end_segment_id:
        _, _, res_end = _resolve_segment_id(
            config, args.end_segment_id, feed_id=feed_id
        )
        expected_end = expected_end or res_end.isoformat()

    if not feed_id:
        logger.error(
            "You must specify --feed-id or provide a transcript --url."
        )
        return None

    if seg_target_start and seg_target_end:
        t_start = seg_target_start
        t_end = seg_target_end
    elif timestamp_arg:
        t_start = _parse_timestamp(timestamp_arg)
        t_end = t_start + datetime.timedelta(seconds=15.0)
    elif expected_start and expected_end:
        t_start = _parse_timestamp(expected_start)
        t_end = _parse_timestamp(expected_end)
    else:
        logger.error(
            "You must provide --timestamp, --segment-id, --url, OR "
            "(--expected-start AND --expected-end)."
        )
        return None

    if t_start >= t_end:
        logger.error(
            "Expected speech start timestamp must be strictly before "
            "expected end timestamp."
        )
        return None

    return feed_id, t_start, t_end


def _parse_offset_seconds(time_str: str) -> float:
    """Parses offset seconds from float (0.807) or timestamp (00:00:807)."""
    t_str = time_str.strip()
    try:
        return float(t_str)
    except ValueError:
        pass

    parts = t_str.split(":")
    if len(parts) == 3:
        try:
            m, s, ms = float(parts[0]), float(parts[1]), float(parts[2])
            if ms >= 60.0:
                return m * 60.0 + s + (ms / 1000.0)
            return m * 3600.0 + s * 60.0 + ms
        except ValueError:
            pass

    if len(parts) == 2:
        try:
            m, s = float(parts[0]), float(parts[1])
            return m * 60.0 + s
        except ValueError:
            pass

    msg = f"Could not parse speech offset timestamp {time_str!r}."
    raise ValueError(msg)


def _run_diagnostic_session(
    args: argparse.Namespace,
    config: DiagnosticConfig,
    feed_id: str,
    target_start: datetime.datetime,
    target_end: datetime.datetime,
) -> None:
    """Executes chunk discovery, replay, and reconciliation."""
    runner = FeedDiagnosticRunner(config.bucket, feed_id, config.project)

    warmup_start = target_start - datetime.timedelta(seconds=args.backup_sec)
    search_end = max(
        target_end + datetime.timedelta(seconds=NOMINAL_CHUNK_DURATION_SEC),
        target_start + datetime.timedelta(seconds=args.window_sec),
    )

    blobs = runner.discover_audio_blobs(warmup_start, search_end)
    if not blobs:
        logger.warning(
            "Zero matching GCS audio objects discovered for feed %s "
            "between %s and %s.",
            feed_id,
            warmup_start.isoformat(),
            search_end.isoformat(),
        )
        return

    if any(
        b.name and b.name.startswith(("bcfy_calls/", "openmhz/")) for b in blobs
    ):
        logger.error(
            "Unsupported feed type for feed '%s': VAD drop diagnostics "
            "only support continuous unsegmented stream feeds (bcfy_feeds "
            "/ icecast). Discrete call feeds (bcfy_calls / openmhz) do not "
            "run continuous VAD segmentation.",
            feed_id,
        )
        return

    logger.info(
        "Discovered %d chronologically ordered chunks. "
        "Beginning warm-state replay...",
        len(blobs),
    )

    live_utterances, live_probs = runner.execute_warm_replay(
        blobs, target_start, target_end=target_end
    )
    runner.reconcile_timeframe(
        target_start, target_end, live_utterances, live_probs=live_probs
    )

    accepted_segs = [
        (u.start_time, u.end_time)
        for u in live_utterances
        if u.status == "Accepted"
    ]

    user_ranges = None
    if args.user_speech:
        user_ranges = []
        for raw_p in args.user_speech.split(","):
            part_str = raw_p.strip()
            if "-" in part_str:
                s_s, s_e = part_str.split("-", 1)
                user_ranges.append(
                    (_parse_offset_seconds(s_s), _parse_offset_seconds(s_e))
                )
            else:
                v = _parse_offset_seconds(part_str)
                user_ranges.append((v, v + 1.0))

    _render_ascii_vad_timeline(
        target_start,
        target_end,
        live_probs,
        accepted_segs,
        runner.vad_engine.threshold_onset,
        user_ranges,
    )


def main() -> None:
    parser = _build_arg_parser()
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    try:
        config = _resolve_config(args)
        resolved = _resolve_target_timeframe(args, config)
        if not resolved:
            return

        feed_id, target_start, target_end = resolved
        _run_diagnostic_session(args, config, feed_id, target_start, target_end)

    except ValueError as e:
        sys.stderr.write(f"Error: {e}\n")
    except Exception:
        logger.exception("Diagnostic execution terminated with an error.")


if __name__ == "__main__":
    main()
