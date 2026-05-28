from __future__ import annotations

from dataclasses import dataclass, replace
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
from urllib.parse import urlparse

import requests

from common.gcs_utils import download_to_scratch
from dataset_split.artifacts import (
    DatasetArtifactLayout,
    audio_object_uri,
    upload_file_create_only,
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
_ENRICHED_FIELD_NAMES = (
    "model_ready_audio_uri",
    "derived_audio_uri",
    "transformation_metadata",
)
_FFMPEG_FLAC_MONO_ARGS = ("-ac", "1", "-c:a", "flac")


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


def derive_audio_clip(
    input_path: str | Path,
    output_path: str | Path,
    *,
    offset: float,
    duration: float,
    runner=subprocess.run,
) -> Path:
    command = [
        "ffmpeg",
        "-hide_banner",
        "-y",
        "-ss",
        f"{offset:.6f}",
        "-t",
        f"{duration:.6f}",
        "-i",
        str(input_path),
        "-map",
        "0:a:0",
        "-vn",
        *_FFMPEG_FLAC_MONO_ARGS,
        str(output_path),
    ]
    _run_ffmpeg(command, runner=runner)
    return Path(output_path)


def transcode_audio_file(
    input_path: str | Path,
    output_path: str | Path,
    *,
    runner=subprocess.run,
) -> Path:
    command = [
        "ffmpeg",
        "-hide_banner",
        "-y",
        "-i",
        str(input_path),
        "-map",
        "0:a:0",
        "-vn",
        *_FFMPEG_FLAC_MONO_ARGS,
        str(output_path),
    ]
    _run_ffmpeg(command, runner=runner)
    return Path(output_path)


def copy_audio_file(
    input_path: str | Path, output_path: str | Path
) -> Path:
    try:
        shutil.copyfile(input_path, output_path)
    except Exception as exc:
        raise AudioDerivationError(
            f"failed to copy audio input_path={input_path} "
            f"output_path={output_path}"
        ) from exc
    return Path(output_path)


def prepare_audio_for_publication(
    storage_client: object,
    *,
    layout: DatasetArtifactLayout,
    segments: tuple[LabeledSegment, ...],
    scratch_dir: str | Path | None = None,
    runner=subprocess.run,
) -> AudioPreparationResult:
    if scratch_dir is None:
        with tempfile.TemporaryDirectory() as temporary_scratch:
            return _prepare_audio_for_publication(
                storage_client,
                layout=layout,
                segments=segments,
                scratch_dir=temporary_scratch,
                runner=runner,
            )
    return _prepare_audio_for_publication(
        storage_client,
        layout=layout,
        segments=segments,
        scratch_dir=scratch_dir,
        runner=runner,
    )


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


def _prepare_audio_for_publication(
    storage_client: object,
    *,
    layout: DatasetArtifactLayout,
    segments: tuple[LabeledSegment, ...],
    scratch_dir: str | Path,
    runner,
) -> AudioPreparationResult:
    plans = plan_audio_actions(
        storage_client,
        layout=layout,
        segments=tuple(segments),
        scratch_dir=scratch_dir,
        runner=runner,
    )
    version_summary = _command_version_summary(runner)
    enriched_segments: list[LabeledSegment] = []
    uploaded_audio_uris: list[str] = []
    action_counts = {action: 0 for action in AUDIO_ACTIONS}
    for plan in plans:
        try:
            enriched, uploaded_uri = _materialize_plan(
                storage_client,
                plan=plan,
                scratch_dir=Path(scratch_dir),
                version_summary=version_summary,
                runner=runner,
            )
        except AudioDerivationError as exc:
            raise AudioDerivationError(
                f"{exc} {_row_context(plan.segment)}"
            ) from exc
        action_counts[plan.action] += 1
        enriched_segments.append(enriched)
        if uploaded_uri is not None:
            uploaded_audio_uris.append(uploaded_uri)
    return AudioPreparationResult(
        segments=tuple(enriched_segments),
        plans=plans,
        uploaded_audio_uris=tuple(uploaded_audio_uris),
        action_counts=action_counts,
    )


def _materialize_plan(
    storage_client: object,
    *,
    plan: AudioActionPlan,
    scratch_dir: Path,
    version_summary: dict[str, str],
    runner,
) -> tuple[LabeledSegment, str | None]:
    if plan.action == "reused":
        model_ready_uri = plan.source_uri
        output_probe = plan.probe
        uploaded_uri = None
        ffmpeg_summary = None
    else:
        if plan.destination_uri is None:
            raise AudioDerivationError(
                f"missing destination_uri for action={plan.action}"
            )
        local_output_path = _local_output_path(
            scratch_dir, suffix=plan.output_suffix
        )
        ffmpeg_summary = None
        if plan.action == "copied":
            copy_audio_file(plan.local_source_path, local_output_path)
            output_probe = plan.probe
        elif plan.action == "derived":
            derive_audio_clip(
                plan.local_source_path,
                local_output_path,
                offset=plan.segment.offset,
                duration=plan.segment.duration,
                runner=runner,
            )
            ffmpeg_summary = _derive_command_summary()
            output_probe = probe_audio(local_output_path, runner=runner)
        elif plan.action == "transcoded":
            transcode_audio_file(
                plan.local_source_path,
                local_output_path,
                runner=runner,
            )
            ffmpeg_summary = _transcode_command_summary()
            output_probe = probe_audio(local_output_path, runner=runner)
        else:
            raise AudioDerivationError(f"unsupported action={plan.action}")
        model_ready_uri = plan.destination_uri
        _upload_audio_file(
            storage_client,
            uri=model_ready_uri,
            local_path=local_output_path,
        )
        uploaded_uri = model_ready_uri
    if not model_ready_uri.startswith("gs://"):
        raise AudioDerivationError(
            f"model_ready_audio_uri must be gs://: {model_ready_uri}"
        )
    derived_audio_uri = model_ready_uri if plan.action == "derived" else None
    metadata = _transformation_metadata(
        plan,
        output_probe=output_probe,
        ffmpeg_summary=ffmpeg_summary,
        version_summary=version_summary,
    )
    segment = plan.segment
    return (
        replace(segment,
            model_ready_audio_uri=model_ready_uri,
            derived_audio_uri=derived_audio_uri,
            transformation_metadata=metadata,
        ),
        uploaded_uri,
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


def _run_ffmpeg(command: list[str], *, runner) -> None:
    try:
        runner(
            command,
            capture_output=True,
            check=True,
            text=True,
        )
    except Exception as exc:
        raise AudioDerivationError(
            f"failed to run ffmpeg command={command[0]}"
        ) from exc


def _upload_audio_file(
    storage_client: object, *, uri: str, local_path: Path
) -> None:
    try:
        upload_file_create_only(
            storage_client,
            uri,
            local_path,
            content_type=infer_audio_mime_type(uri),
        )
    except Exception as exc:
        raise AudioDerivationError(
            f"failed to upload audio uri={uri}"
        ) from exc


def _local_output_path(scratch_dir: Path, *, suffix: str) -> Path:
    fd, local_path = tempfile.mkstemp(dir=scratch_dir, suffix=suffix)
    os.close(fd)
    return Path(local_path)


def _transformation_metadata(
    plan: AudioActionPlan,
    *,
    output_probe: AudioProbe,
    ffmpeg_summary: str | None,
    version_summary: dict[str, str],
) -> dict[str, object]:
    segment = plan.segment
    return {
        "action": plan.action,
        "original_audio_uri": segment.original_audio_uri,
        "source_audio_uri": plan.source_uri,
        "offset": segment.offset,
        "duration": segment.duration,
        "source_duration": plan.probe.duration,
        "output_duration": output_probe.duration,
        "source_format": _probe_format(plan.probe),
        "output_format": _probe_format(output_probe),
        "source_channels": plan.probe.channels,
        "output_channels": output_probe.channels,
        "mixed_to_mono": (
            plan.action in {"derived", "transcoded"}
            and plan.probe.channels != output_probe.channels
            and output_probe.channels == 1
        ),
        "resampled": plan.probe.sample_rate != output_probe.sample_rate,
        "padded": False,
        "split": segment.split,
        "source_group": segment.source_group,
        "ffprobe": _ffprobe_command_summary(),
        "ffmpeg": ffmpeg_summary,
        "ffprobe_version": version_summary["ffprobe_version"],
        "ffmpeg_version": version_summary["ffmpeg_version"],
    }


def _probe_format(probe: AudioProbe) -> str:
    return probe.format_name or probe.codec_name


def _ffprobe_command_summary() -> str:
    return (
        "ffprobe -v error -select_streams a:0 -show_entries "
        "format=duration:stream=codec_name,channels,sample_rate -of json INPUT"
    )


def _derive_command_summary() -> str:
    return (
        "ffmpeg -hide_banner -y -ss OFFSET -t DURATION -i INPUT "
        "-map 0:a:0 -vn -ac 1 -c:a flac OUTPUT.flac"
    )


def _transcode_command_summary() -> str:
    return (
        "ffmpeg -hide_banner -y -i INPUT -map 0:a:0 -vn "
        "-ac 1 -c:a flac OUTPUT.flac"
    )


def _command_version_summary(runner) -> dict[str, str]:
    return {
        "ffprobe_version": _program_version("ffprobe", runner=runner),
        "ffmpeg_version": _program_version("ffmpeg", runner=runner),
    }


def _program_version(program: str, *, runner) -> str:
    try:
        result = runner(
            [program, "-version"],
            capture_output=True,
            check=True,
            text=True,
        )
    except Exception:
        return "unavailable"
    first_line = str(result.stdout).splitlines()[0].strip()
    return first_line or "unavailable"


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
