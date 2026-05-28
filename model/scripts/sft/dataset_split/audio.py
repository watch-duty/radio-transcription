from __future__ import annotations

from dataclasses import dataclass
import json
import os
from pathlib import Path
import subprocess
import tempfile
from urllib.parse import urlparse

import requests

from common.gcs_utils import download_to_scratch
from dataset_split.artifacts import (
    DatasetArtifactLayout,
    audio_object_uri,
)
from dataset_split.model_writers import ModelWriterError, infer_audio_mime_type
from dataset_split.types import LabeledSegment


class AudioDerivationError(ValueError):
    """Raised when source audio cannot become model-ready audio."""


AUDIO_ACTIONS = ("reused", "copied", "derived", "transcoded")
GENERATED_AUDIO_SUFFIX = ".flac"
SOURCE_DURATION_TOLERANCE_SECONDS = 0.5
SOURCE_DURATION_TOLERANCE_RATIO = 0.02
EXTERNAL_DOWNLOAD_TIMEOUT = (10, 120)
EXTERNAL_DOWNLOAD_CHUNK_SIZE = 1024 * 1024
EXTERNAL_DOWNLOAD_MAX_BYTES = 512 * 1024 * 1024
_SUPPORTED_SOURCE_SCHEMES = frozenset({"gs", "http", "https"})
_NEAR_ZERO_OFFSET_SECONDS = 1e-6


@dataclass(frozen=True)
class AudioProbe:
    duration: float
    codec_name: str
    channels: int
    sample_rate: int
    format_name: str | None = None


@dataclass(frozen=True)
class AudioActionPlan:
    segment: LabeledSegment
    action: str
    source_uri: str
    local_source_path: Path
    destination_uri: str | None
    probe: AudioProbe
    output_suffix: str


@dataclass(frozen=True)
class AudioPreparationResult:
    segments: tuple[LabeledSegment, ...]
    plans: tuple[AudioActionPlan, ...]
    uploaded_audio_uris: tuple[str, ...]
    action_counts: dict[str, int]


def probe_audio(
    local_path: str | Path, *, runner=subprocess.run
) -> AudioProbe:
    command = [
        "ffprobe",
        "-v",
        "error",
        "-select_streams",
        "a:0",
        "-show_entries",
        "format=duration:stream=codec_name,channels,sample_rate",
        "-of",
        "json",
        str(local_path),
    ]
    try:
        result = runner(
            command,
            capture_output=True,
            check=True,
            text=True,
        )
        payload = json.loads(result.stdout)
        stream = payload["streams"][0]
        format_info = payload["format"]
        probe = AudioProbe(
            duration=float(format_info["duration"]),
            codec_name=str(stream["codec_name"]),
            channels=int(stream["channels"]),
            sample_rate=int(stream["sample_rate"]),
            format_name=format_info.get("format_name"),
        )
    except Exception as exc:
        raise AudioDerivationError(
            f"failed to probe audio local_path={local_path}"
        ) from exc
    if probe.duration <= 0:
        raise AudioDerivationError(
            f"source duration must be > 0 local_path={local_path}"
        )
    return probe


def stage_source_audio(
    storage_client: object,
    source_uri: str,
    scratch_dir: str | Path,
) -> Path:
    scratch_path = Path(scratch_dir)
    scratch_path.mkdir(parents=True, exist_ok=True)
    parsed = urlparse(source_uri)
    if parsed.scheme == "gs":
        try:
            return Path(
                download_to_scratch(
                    storage_client, source_uri, str(scratch_path)
                )
            )
        except Exception as exc:
            raise AudioDerivationError(
                f"failed to download GCS source audio source_uri={source_uri}"
            ) from exc
    if parsed.scheme in {"http", "https"}:
        return _download_external_source(source_uri, scratch_path)
    raise AudioDerivationError(
        "source audio URI scheme must be one of "
        f"{sorted(_SUPPORTED_SOURCE_SCHEMES)}: {source_uri}"
    )


def plan_audio_actions(
    storage_client: object,
    *,
    layout: DatasetArtifactLayout,
    segments: tuple[LabeledSegment, ...],
    scratch_dir: str | Path,
    runner=subprocess.run,
) -> tuple[AudioActionPlan, ...]:
    plans: list[AudioActionPlan] = []
    for segment in segments:
        try:
            plans.append(
                _plan_audio_action(
                    storage_client,
                    layout=layout,
                    segment=segment,
                    scratch_dir=scratch_dir,
                    runner=runner,
                )
            )
        except AudioDerivationError:
            raise
        except Exception as exc:
            raise AudioDerivationError(
                f"failed to plan audio action {_row_context(segment)}"
            ) from exc
    return tuple(plans)


def _plan_audio_action(
    storage_client: object,
    *,
    layout: DatasetArtifactLayout,
    segment: LabeledSegment,
    scratch_dir: str | Path,
    runner,
) -> AudioActionPlan:
    _validate_segment_span(segment)
    try:
        local_source_path = stage_source_audio(
            storage_client, segment.audio_uri, scratch_dir
        )
        probe = probe_audio(local_source_path, runner=runner)
    except AudioDerivationError as exc:
        raise AudioDerivationError(
            f"{exc} {_row_context(segment)}"
        ) from exc
    _validate_source_bounds(segment, probe)

    source_supported = _is_writer_supported(segment.audio_uri)
    standalone = _is_standalone_span(segment, probe)
    source_suffix = _source_suffix(segment.audio_uri)
    if source_supported and standalone and segment.audio_uri.startswith("gs://"):
        return AudioActionPlan(
            segment=segment,
            action="reused",
            source_uri=segment.audio_uri,
            local_source_path=local_source_path,
            destination_uri=None,
            probe=probe,
            output_suffix=source_suffix,
        )
    if source_supported and standalone:
        return AudioActionPlan(
            segment=segment,
            action="copied",
            source_uri=segment.audio_uri,
            local_source_path=local_source_path,
            destination_uri=audio_object_uri(
                layout,
                action="copied",
                segment=segment,
                suffix=source_suffix,
            ),
            probe=probe,
            output_suffix=source_suffix,
        )
    if not standalone:
        return AudioActionPlan(
            segment=segment,
            action="derived",
            source_uri=segment.audio_uri,
            local_source_path=local_source_path,
            destination_uri=audio_object_uri(
                layout,
                action="derived",
                segment=segment,
                suffix=GENERATED_AUDIO_SUFFIX,
            ),
            probe=probe,
            output_suffix=GENERATED_AUDIO_SUFFIX,
        )
    return AudioActionPlan(
        segment=segment,
        action="transcoded",
        source_uri=segment.audio_uri,
        local_source_path=local_source_path,
        destination_uri=audio_object_uri(
            layout,
            action="transcoded",
            segment=segment,
            suffix=GENERATED_AUDIO_SUFFIX,
        ),
        probe=probe,
        output_suffix=GENERATED_AUDIO_SUFFIX,
    )


def _download_external_source(source_uri: str, scratch_dir: Path) -> Path:
    fd, local_path = tempfile.mkstemp(
        dir=scratch_dir, suffix=_source_suffix(source_uri)
    )
    bytes_written = 0
    try:
        with os.fdopen(fd, "wb") as output:
            with requests.get(
                source_uri,
                stream=True,
                timeout=EXTERNAL_DOWNLOAD_TIMEOUT,
            ) as response:
                response.raise_for_status()
                for chunk in response.iter_content(
                    chunk_size=EXTERNAL_DOWNLOAD_CHUNK_SIZE
                ):
                    if not chunk:
                        continue
                    bytes_written += len(chunk)
                    if bytes_written > EXTERNAL_DOWNLOAD_MAX_BYTES:
                        raise AudioDerivationError(
                            "external source audio exceeded maximum "
                            f"download size source_uri={source_uri}"
                        )
                    output.write(chunk)
    except AudioDerivationError:
        raise
    except Exception as exc:
        raise AudioDerivationError(
            f"failed to download external source audio source_uri={source_uri}"
        ) from exc
    return Path(local_path)


def _validate_segment_span(segment: LabeledSegment) -> None:
    if segment.duration <= 0:
        raise AudioDerivationError(
            f"row duration must be > 0 {_row_context(segment)}"
        )
    if segment.offset < 0:
        raise AudioDerivationError(
            f"row offset must be >= 0 {_row_context(segment)}"
        )


def _validate_source_bounds(
    segment: LabeledSegment, probe: AudioProbe
) -> None:
    tolerance = _duration_tolerance(segment.duration)
    if segment.offset + segment.duration > probe.duration + tolerance:
        raise AudioDerivationError(
            "row span exceeds source duration "
            f"source_duration={probe.duration} tolerance={tolerance} "
            f"{_row_context(segment)}"
        )


def _is_standalone_span(
    segment: LabeledSegment, probe: AudioProbe
) -> bool:
    tolerance = _duration_tolerance(segment.duration)
    return (
        abs(segment.offset) <= _NEAR_ZERO_OFFSET_SECONDS
        and abs(probe.duration - segment.duration) <= tolerance
    )


def _duration_tolerance(duration: float) -> float:
    return max(
        SOURCE_DURATION_TOLERANCE_SECONDS,
        duration * SOURCE_DURATION_TOLERANCE_RATIO,
    )


def _is_writer_supported(audio_uri: str) -> bool:
    try:
        infer_audio_mime_type(audio_uri)
    except ModelWriterError:
        return False
    return True


def _source_suffix(source_uri: str) -> str:
    path = urlparse(source_uri).path
    suffix = Path(path).suffix.lower()
    if not suffix or any(char in suffix for char in "/\\"):
        return ".audio"
    return suffix


def _row_context(segment: LabeledSegment) -> str:
    return (
        f"dataset_name={segment.dataset_name} row_index={segment.row_index} "
        f"audio_uri={segment.audio_uri} offset={segment.offset} "
        f"duration={segment.duration} split={segment.split} "
        f"source_group={segment.source_group}"
    )
