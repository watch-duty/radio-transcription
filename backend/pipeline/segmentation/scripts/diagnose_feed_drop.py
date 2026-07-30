"""CLI tool to diagnose dropped speech segments on continuous audio feeds.

Fetches audio chunks for a feed and target timeframe from GCS, replays them
through the Silero + UL-UNAS neural pipeline (with tape backup), and reconciles
why any expected speech interval was classified as non-speech.
"""

import argparse
import datetime
import io
import json
import logging
import os
import re
import sys
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path

import numpy as np
from google.cloud import storage

from backend.pipeline.segmentation import constants as trans_constants
from backend.pipeline.segmentation.audio import processor as audio_processor
from backend.pipeline.segmentation.audio import vad

logger = logging.getLogger("diagnose_feed_drop")


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
    bucket_name: str,
    segment_id: str,
    feed_id: str | None = None,
) -> tuple[str, datetime.datetime, datetime.datetime] | None:
    """Attempts to resolve exact timestamps from Audio Segments API."""
    try:
        import google.auth  # noqa: PLC0415
        from google.auth.impersonated_credentials import (  # noqa: PLC0415
            Credentials,
            IDTokenCredentials,
        )
        from google.auth.transport.requests import Request  # noqa: PLC0415

        aud = "https://audio-segments-api-prod-lu3a6psyna-uc.a.run.app"
        if "dev" in bucket_name or "staging-bucket-dev" in bucket_name:
            aud = "https://audio-segments-api-dev-lu3a6psyna-uc.a.run.app"

        sa = (
            "transcription-sa-prod@"
            "automatic-hawk-481415-m9.iam.gserviceaccount.com"
        )
        source_creds, _project = google.auth.default()
        target_creds = Credentials(
            source_creds,
            target_principal=sa,
            target_scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
        id_creds = IDTokenCredentials(
            target_creds,
            target_audience=aud,
            include_email=True,
        )
        request = Request()
        id_creds.refresh(request)
        token = id_creds.token

        query_feed = feed_id or ""
        url = f"{aud}/v1/audio_segments?limit=5000"
        if query_feed:
            url += f"&feed_ids={query_feed}"
        if segment_id:
            url += f"&segment_ids={segment_id}"

        req = urllib.request.Request(  # noqa: S310
            url, headers={"Authorization": f"Bearer {token}"}
        )
        with urllib.request.urlopen(req) as resp:  # noqa: S310
            data = json.loads(resp.read().decode("utf-8"))
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
        logger.debug(
            "Could not resolve segment %s via API: %s",
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
        logger.debug(
            "Could not decode blob duration for segment %s: %s",
            segment_id,
            err,
        )

    return start_time + datetime.timedelta(seconds=15.0)


def _resolve_segment_id(
    bucket_name: str,
    segment_id: str,
    project_id: str | None,
    feed_id: str | None = None,
) -> tuple[str, datetime.datetime, datetime.datetime]:
    """Resolves feed_id and exact timestamps for a Segment ID."""
    api_res = _resolve_segment_via_api(bucket_name, segment_id, feed_id)
    if api_res:
        return api_res

    client = storage.Client(project=project_id)
    canonical_bucket = bucket_name.replace("staging", "canonical")
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
        int,
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
            return [], None, np.array([], dtype=np.float32), sr, []

        (
            accepted_segments,
            rejected_details,
        ) = self.vad_engine.detect_speech_segments_with_diagnostics(
            samples,
            sample_rate=sr,
            prior_audio=prior_samples,
        )

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

        last_prep = getattr(self.vad_engine, "last_preprocessed_audio", None)
        preprocessed = (
            last_prep
            if last_prep is not None
            else (
                samples.astype(np.float32) / 32768.0
                if np.issubdtype(samples.dtype, np.integer)
                else samples
            )
        )
        _, probs_list = self.vad_engine.extract_vad_frame_probs(
            preprocessed,
            chunk_size=vad.DEFAULT_SILERO_WINDOW_SIZE,
            context_size=64,
        )
        return utterances, next_prior_tail, preprocessed, sr, probs_list

    def execute_warm_replay(
        self,
        blobs: list[storage.Blob],
        warmup_end_time: datetime.datetime,
        target_end: datetime.datetime | None = None,
    ) -> tuple[
        list[DiagnosticUtterance],
        dict[storage.Blob, tuple[np.ndarray, int]],
        list[tuple[float, float, float]],
    ]:
        """Replays blobs continuously, separating warmup from reporting."""
        prior_tail = None
        live_utterances = []
        signal_cache = {}
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
            chunk_end_time = blob_time + datetime.timedelta(seconds=15.0)
            is_warmup = chunk_end_time <= warmup_end_time

            if is_warmup:
                warmup_count += 1
            else:
                live_count += 1

            (
                utts,
                prior_tail,
                preprocessed,
                sr,
                probs_list,
            ) = self.evaluate_chunk_absolute(
                blob,
                blob_time,
                prior_tail,
                is_warmup=is_warmup,
            )

            signal_cache[blob] = (preprocessed, sr)

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
        return live_utterances, signal_cache, live_probs

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
            onset_thresh = getattr(
                self.vad_engine,
                "threshold_onset",
                vad.VAD_DEFAULT_THRESHOLD_ONSET,
            )

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


def _build_svg_rects(
    target_start: datetime.datetime,
    target_end: datetime.datetime,
    total_dur: float,
    pad_l: int,
    pad_t: int,
    plot_w: int,
    plot_h: int,
    accepted_segments: list[tuple[datetime.datetime, datetime.datetime]],
    missing_intervals: list[tuple[datetime.datetime, datetime.datetime]],
    user_speech_ranges: list[tuple[float, float]] | None = None,
) -> tuple[list[str], list[str], list[str]]:
    """Builds SVG rect elements for accepted, missing, and user speech."""
    accepted_rects = []
    for a_start, a_end in accepted_segments:
        if a_end < target_start or a_start > target_end:
            continue
        s_sec = max((a_start - target_start).total_seconds(), 0.0)
        e_sec = min((a_end - target_start).total_seconds(), total_dur)
        x = pad_l + (s_sec / total_dur) * plot_w
        w = max(((e_sec - s_sec) / total_dur) * plot_w, 2.0)
        accepted_rects.append(
            f'<rect x="{x:.1f}" y="{pad_t}" width="{w:.1f}" '
            f'height="{plot_h * 2 + 20}" fill="rgba(46, 204, 113, 0.25)" '
            f'stroke="#2ecc71" stroke-width="1.5"/>'
        )

    missing_rects = []
    for m_start, m_end in missing_intervals:
        if m_end < target_start or m_start > target_end:
            continue
        s_sec = max((m_start - target_start).total_seconds(), 0.0)
        e_sec = min((m_end - target_start).total_seconds(), total_dur)
        x = pad_l + (s_sec / total_dur) * plot_w
        w = max(((e_sec - s_sec) / total_dur) * plot_w, 2.0)
        missing_rects.append(
            f'<rect x="{x:.1f}" y="{pad_t}" width="{w:.1f}" '
            f'height="{plot_h * 2 + 20}" fill="rgba(231, 76, 60, 0.25)" '
            f'stroke="#e74c3c" stroke-width="1.5" stroke-dasharray="4,2"/>'
        )

    user_rects = []
    if user_speech_ranges:
        for u_start, u_end in user_speech_ranges:
            if u_end < 0.0 or u_start > total_dur:
                continue
            s_sec = max(u_start, 0.0)
            e_sec = min(u_end, total_dur)
            x = pad_l + (s_sec / total_dur) * plot_w
            w = max(((e_sec - s_sec) / total_dur) * plot_w, 2.0)
            user_rects.append(
                f'<rect x="{x:.1f}" y="{pad_t}" width="{w:.1f}" '
                f'height="{plot_h * 2 + 20}" fill="rgba(52, 152, 219, 0.2)" '
                f'stroke="#3498db" stroke-width="2" stroke-dasharray="6,3"/>'
            )
    return accepted_rects, missing_rects, user_rects


def _generate_html_visual_report(
    output_path: str,
    feed_id: str,
    target_start: datetime.datetime,
    target_end: datetime.datetime,
    frame_probs: list[tuple[float, float, float]],
    accepted_segments: list[tuple[datetime.datetime, datetime.datetime]],
    missing_intervals: list[tuple[datetime.datetime, datetime.datetime]],
    user_speech_ranges: list[tuple[float, float]] | None = None,
) -> None:
    """Generates an interactive HTML/SVG visual waveform chart."""
    total_dur = max((target_end - target_start).total_seconds(), 1.0)
    svg_w, svg_h = 1000, 320
    pad_l, pad_r, pad_t, pad_b = 60, 40, 40, 40
    plot_w = svg_w - pad_l - pad_r
    plot_h = (svg_h - pad_t - pad_b - 20) // 2

    valid_probs = sorted(
        [(r, a, p) for r, a, p in frame_probs if 0.0 <= r <= total_dur],
        key=lambda item: item[0],
    )

    wav_pts = []
    prob_pts = []
    for rel_sec, amp, prob in valid_probs:
        x = pad_l + (rel_sec / total_dur) * plot_w
        y_wav = pad_t + plot_h / 2 - (amp * plot_h / 2)
        y_prob = pad_t + plot_h + 20 + (1.0 - prob) * plot_h
        wav_pts.append(f"{x:.1f},{y_wav:.1f}")
        prob_pts.append(f"{x:.1f},{y_prob:.1f}")

    wav_d = "M " + " L ".join(wav_pts) if wav_pts else ""
    prob_d = "M " + " L ".join(prob_pts) if prob_pts else ""
    onset_y = pad_t + plot_h + 20 + (1.0 - 0.35) * plot_h

    accepted_rects, missing_rects, user_rects = _build_svg_rects(
        target_start,
        target_end,
        total_dur,
        pad_l,
        pad_t,
        plot_w,
        plot_h,
        accepted_segments,
        missing_intervals,
        user_speech_ranges,
    )

    t_s_str = target_start.strftime("%H:%M:%S.%f")[:-3]
    t_e_str = target_end.strftime("%H:%M:%S.%f")[:-3]

    parts = [
        "<!DOCTYPE html><html><head><meta charset='utf-8'>",
        f"<title>VAD Diagnostic Report - Feed {feed_id}</title>",
        "<style>",
        "body { font-family: sans-serif; background: #0f172a; "
        "color: #f8fafc; padding: 24px; }",
        ".card { background: #1e293b; border-radius: 12px; padding: 20px; }",
        "h1 { font-size: 20px; color: #38bdf8; }",
        ".meta { font-size: 13px; color: #94a3b8; margin-bottom: 16px; }",
        ".legend { display: flex; gap: 16px; font-size: 12px; }",
        ".badge { padding: 4px 8px; border-radius: 4px; font-weight: 600; }",
        ".b-speech { background: rgba(46,204,113,0.2); color: #2ecc71; "
        "border: 1px solid #2ecc71; }",
        ".b-drop { background: rgba(231,76,60,0.2); color: #e74c3c; "
        "border: 1px solid #e74c3c; }",
        ".b-user { background: rgba(52,152,219,0.2); color: #38bdf8; "
        "border: 1px solid #38bdf8; }",
        "</style></head><body>",
        "<div class='card'><h1>VAD Diagnostic Chart</h1>",
        f"<div class='meta'>Feed: <code>{feed_id}</code> | Timeframe: "
        f"<code>{t_s_str}</code> to <code>{t_e_str} UTC</code></div>",
        f"<svg width='{svg_w}' height='{svg_h}' viewBox='0 0 {svg_w} {svg_h}'>",
        "".join(accepted_rects),
        "".join(missing_rects),
        "".join(user_rects),
        f"<line x1='{pad_l}' y1='{pad_t + plot_h / 2}' x2='{pad_l + plot_w}' "
        f"y2='{pad_t + plot_h / 2}' stroke='#334155'/>",
        f"<path d='{wav_d}' fill='none' stroke='#64748b' stroke-width='1.2'/>",
        f"<line x1='{pad_l}' y1='{pad_t + plot_h + 20 + plot_h}' "
        f"x2='{pad_l + plot_w}' y2='{pad_t + plot_h + 20 + plot_h}' "
        "stroke='#334155'/>",
        f"<line x1='{pad_l}' y1='{onset_y}' x2='{pad_l + plot_w}' "
        f"y2='{onset_y}' stroke='#ef4444' stroke-width='1.5' "
        "stroke-dasharray='4,4'/>",
        f"<text x='{pad_l - 10}' y='{onset_y + 4}' fill='#ef4444' "
        "font-size='10' text-anchor='end'>0.35 Onset</text>",
        f"<path d='{prob_d}' fill='none' stroke='#38bdf8' stroke-width='2'/>",
        "</svg><div class='legend'>",
        "<span class='badge b-speech'>■ Accepted Speech</span>",
        "<span class='badge b-drop'>■ Dropped Interval</span>",
        "<span class='badge b-user'>░ Expected Speech</span>",
        "</div></div></body></html>",
    ]
    html_content = "".join(parts)
    out_p = Path(output_path)
    out_p.parent.mkdir(parents=True, exist_ok=True)
    out_p.write_text(html_content, encoding="utf-8")
    logger.info("Saved Visual Diagnostic Chart to: %s", output_path)


def _render_ascii_vad_timeline(  # noqa: PLR0912, PLR0915
    target_start: datetime.datetime,
    target_end: datetime.datetime,
    frame_probs: list[tuple[float, float, float]],
    accepted_segments: list[tuple[datetime.datetime, datetime.datetime]],
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
        bin_dur = max(bin_end_sec - bin_start_sec, 0.001)

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
        is_user = False
        if user_speech_ranges:
            is_user = any(
                (max(u_s, bin_start_sec) < min(u_e, bin_end_sec))
                for u_s, u_e in user_speech_ranges
            )

        overlap_frac = 0.0
        if user_speech_ranges and accepted_segments:
            intersection_sec = 0.0
            for a_s, a_e in accepted_segments:
                a_rel_s = (a_s - target_start).total_seconds()
                a_rel_e = (a_e - target_start).total_seconds()
                for u_s, u_e in user_speech_ranges:
                    inter_s = max(bin_start_sec, a_rel_s, u_s)
                    inter_e = min(bin_end_sec, a_rel_e, u_e)
                    if inter_s < inter_e:
                        intersection_sec += inter_e - inter_s
            overlap_frac = min(intersection_sec / bin_dur, 1.0)

        if user_speech_ranges:
            if is_accepted and is_user:
                tag = "[TP]"
                status = "BOTH (MATCH)"
            elif is_user and not is_accepted:
                tag = "[FN]"
                status = "USER ONLY"
            elif is_accepted and not is_user:
                tag = "[FP]"
                status = "VAD ONLY"
            else:
                tag = "[TN]"
                status = "SILENCE"
        elif is_accepted:
            tag = "[V]"
            status = "VAD ACCEPTED"
        elif prob >= 0.35:
            tag = "[?]"
            status = "VAD UNEMITTED"
        else:
            tag = "[.]"
            status = "SILENCE"

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
        "--html-report",
        nargs="?",
        const="default",
        type=str,
        default=None,
        help=(
            "Optional path to export an interactive HTML/SVG visual waveform "
            "chart (e.g. '--html-report' or '--html-report report.html')."
        ),
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


def _resolve_env_and_bucket(
    args: argparse.Namespace,
) -> tuple[str, str]:
    """Resolves default GCS bucket and GCP project from environment flags."""
    env_key = (args.env or "dev").lower()
    if env_key in ("prod", "production"):
        default_bucket = "ingestion-staging-bucket"
        default_project = "automatic-hawk-481415-m9"
    else:
        default_bucket = "ingestion-staging-bucket-dev"
        default_project = "probable-symbol-492218-i7"

    bucket = args.bucket or default_bucket
    project = args.project or default_project
    return bucket, project


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
    args: argparse.Namespace, bucket: str, project: str
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
            bucket, segment_id, project, feed_id=feed_id
        )
        feed_id = feed_id or res_feed
        seg_target_start = res_start
        seg_target_end = res_end
        timestamp_arg = timestamp_arg or res_start.isoformat()

    if args.start_segment_id:
        res_feed, res_start, _ = _resolve_segment_id(
            bucket, args.start_segment_id, project, feed_id=feed_id
        )
        feed_id = feed_id or res_feed
        expected_start = expected_start or res_start.isoformat()

    if args.end_segment_id:
        _, _, res_end = _resolve_segment_id(
            bucket, args.end_segment_id, project, feed_id=feed_id
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
    bucket: str,
    project: str,
    feed_id: str,
    target_start: datetime.datetime,
    target_end: datetime.datetime,
) -> None:
    """Executes chunk discovery, replay, and reconciliation."""
    runner = FeedDiagnosticRunner(bucket, feed_id, project)

    warmup_start = target_start - datetime.timedelta(seconds=args.backup_sec)
    search_end = max(
        target_end + datetime.timedelta(seconds=15.0),
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

    live_utterances, _, live_probs = runner.execute_warm_replay(
        blobs, target_start, target_end=target_end
    )
    reports = runner.reconcile_timeframe(
        target_start, target_end, live_utterances, live_probs=live_probs
    )

    accepted_segs = [
        (u.start_time, u.end_time)
        for u in live_utterances
        if u.status == "Accepted"
    ]
    missing_segs = [(r.missing_start, r.missing_end) for r in reports]

    user_ranges = None
    if getattr(args, "user_speech", None):
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
        target_start, target_end, live_probs, accepted_segs, user_ranges
    )

    if args.html_report:
        artifact_dir = os.environ.get("ANTIGRAVITY_ARTIFACT_DIR") or "."
        if args.html_report == "default":
            html_path = os.path.join(
                artifact_dir, f"vad_diagnostic_{feed_id[:8]}.html"
            )
        else:
            html_path = args.html_report

        _generate_html_visual_report(
            html_path,
            feed_id,
            target_start,
            target_end,
            live_probs,
            accepted_segs,
            missing_segs,
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
        bucket, project = _resolve_env_and_bucket(args)
        resolved = _resolve_target_timeframe(args, bucket, project)
        if not resolved:
            return

        feed_id, target_start, target_end = resolved
        _run_diagnostic_session(
            args, bucket, project, feed_id, target_start, target_end
        )

    except ValueError as e:
        sys.stderr.write(f"Error: {e}\n")
    except Exception:
        logger.exception("Diagnostic execution terminated with an error.")


if __name__ == "__main__":
    main()
