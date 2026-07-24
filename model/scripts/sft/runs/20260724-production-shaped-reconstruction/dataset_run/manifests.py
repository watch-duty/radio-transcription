"""Deterministic manifests for the production-shaped reconstruction.

This module is intentionally provider-free: it serializes already-constructed
requests and already-verified materialization receipts.  It neither uploads
audio nor submits tuning or inference work.

The canonical manifests retain exact source-frame lineage for auditing and
scoring.  The Gemini manifests deliberately contain only the current
production system instruction, one audio-only user turn, and one model target.
Local materialization paths are emitted only in the separate mapping receipt.
"""

# One-time fail-loud serializer: direct diagnostic exceptions are intentional.
# ruff: noqa: D202, EM101, EM102, PLR0912, TRY003

from __future__ import annotations

import ast
import copy
import dataclasses
import decimal
import hashlib
import pathlib
import re
from collections.abc import Mapping, Sequence

from . import artifacts, domain, eval_views, media

_BACKEND_PROMPT_PATH = pathlib.PurePosixPath(
    "backend/pipeline/transcription/transcribers/prompts.py"
)
_MODEL_PROMPT_PATH = pathlib.PurePosixPath("model/src/common/gemini/prompts.py")
_LATEST_AUDIO_ONLY_CONTRACT_PATH = pathlib.PurePosixPath(
    "model/scripts/sft/runs/"
    "20260712-gemini31-flash-lite-a16-lr05-e14/"
    "sft_run/contract.py"
)
_BACKEND_PROMPT_SYMBOL = "GEMINI_PROMPT"
_MODEL_PROMPT_SYMBOL = "GEMINI_TRANSCRIBE_SYSTEM_PROMPT"
_FAMILIES = frozenset(
    {"bcfy_calls", "bcfy_feeds", "echo", "fire_notifications"}
)
_REQUEST_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*\Z")
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_AUDIO_MIME = "audio/flac"
_AUDIO_ONLY_SCHEMA = {
    "contents": [
        {
            "parts": [
                {
                    "fileData": {
                        "fileUri": "<canonical-gcs-flac>",
                        "mimeType": _AUDIO_MIME,
                    }
                }
            ],
            "role": "user",
        },
        {"parts": [{"text": "<nonblank-target>"}], "role": "model"},
    ],
    "systemInstruction": {
        "parts": [{"text": "<current-production-system-prompt>"}],
        "role": "system",
    },
}


class ManifestError(ValueError):
    """Raised when deterministic manifest serialization cannot be proven."""


@dataclasses.dataclass(frozen=True, slots=True)
class MaterializationBinding:
    """Associate a materialized continuous slice with its request."""

    request_id: str
    receipt: media.SliceReceipt


@dataclasses.dataclass(frozen=True, slots=True)
class PromptContractReceipt:
    """Hashes proving the exact current prompt and audio-only SFT shape."""

    system_prompt: str
    system_prompt_sha256: str
    backend_prompt_source: str
    backend_prompt_source_sha256: str
    model_prompt_source: str
    model_prompt_source_sha256: str
    latest_audio_only_contract_source: str
    latest_audio_only_contract_source_sha256: str
    audio_only_schema_sha256: str


@dataclasses.dataclass(frozen=True, slots=True)
class ManifestArtifacts:
    """All deterministic bytes produced by manifest serialization."""

    canonical_train_rows: tuple[dict[str, object], ...]
    canonical_eval_rows: tuple[dict[str, object], ...]
    provider_validation_rows: tuple[dict[str, object], ...]
    provider_train_examples: tuple[dict[str, object], ...]
    provider_validation_examples: tuple[dict[str, object], ...]
    canonical_train_jsonl: bytes
    canonical_eval_jsonl: bytes
    canonical_validation_jsonl: bytes
    primary_eval_jsonl: bytes
    gemini_train_jsonl: bytes
    gemini_validation_jsonl: bytes
    materialization_mapping_rows: tuple[dict[str, object], ...]
    materialization_mapping_jsonl: bytes
    prompt_contract_receipt: PromptContractReceipt
    provider_validation_receipt: dict[str, object]
    eval_lane_receipt: dict[str, object]
    manifest_receipt: dict[str, object]

    def files(self) -> dict[str, bytes]:
        """Return the complete create-only artifact payload map.

        No atomic unmasked evaluation artifact is present by construction.
        """

        return {
            "eval/reconstructed_primary.jsonl": self.primary_eval_jsonl,
            "manifests/canonical/eval.jsonl": self.canonical_eval_jsonl,
            "manifests/canonical/train.jsonl": self.canonical_train_jsonl,
            "manifests/canonical/validation.jsonl": (
                self.canonical_validation_jsonl
            ),
            "model_inputs/gemini/train.jsonl": self.gemini_train_jsonl,
            "model_inputs/gemini/validation.jsonl": (
                self.gemini_validation_jsonl
            ),
            "receipts/eval_lanes.json": artifacts.canonical_json_bytes(
                self.eval_lane_receipt
            ),
            "receipts/manifests.json": artifacts.canonical_json_bytes(
                self.manifest_receipt
            ),
            "receipts/materialization_mapping.jsonl": (
                self.materialization_mapping_jsonl
            ),
            "receipts/prompt_contract.json": artifacts.canonical_json_bytes(
                dataclasses.asdict(self.prompt_contract_receipt)
            ),
            "receipts/provider_validation.json": (
                artifacts.canonical_json_bytes(self.provider_validation_receipt)
            ),
        }


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _jsonl(rows: Sequence[Mapping[str, object]]) -> bytes:
    return b"".join(artifacts.canonical_json_bytes(dict(row)) for row in rows)


def _seconds(frames: int, sample_rate: int) -> str:
    with decimal.localcontext() as context:
        context.prec = max(50, len(str(frames)) + len(str(sample_rate)) + 10)
        rendered = format(
            decimal.Decimal(frames) / decimal.Decimal(sample_rate),
            "f",
        )
    if "." in rendered:
        rendered = rendered.rstrip("0").rstrip(".")
    return rendered


def _require_sha256(value: object, *, label: str) -> str:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise ManifestError(f"{label} must be a lowercase SHA-256")
    return value


def _read_source(path: pathlib.Path, *, label: str) -> bytes:
    try:
        return path.read_bytes()
    except OSError as error:
        raise ManifestError(f"cannot read {label}: {path}") from error


def _literal_string_assignment(
    source: bytes,
    *,
    path: pathlib.Path,
    symbol: str,
) -> str:
    try:
        tree = ast.parse(source, filename=str(path))
    except (SyntaxError, ValueError) as error:
        raise ManifestError(f"cannot parse prompt source: {path}") from error
    values: list[str] = []
    for node in tree.body:
        if isinstance(node, ast.Assign):
            names = [
                target.id
                for target in node.targets
                if isinstance(target, ast.Name)
            ]
            value_node = node.value
        elif isinstance(node, ast.AnnAssign) and isinstance(
            node.target, ast.Name
        ):
            names = [node.target.id]
            value_node = node.value
        else:
            continue
        if symbol not in names or value_node is None:
            continue
        try:
            value = ast.literal_eval(value_node)
        except (SyntaxError, TypeError, ValueError) as error:
            raise ManifestError(
                f"{path}: {symbol} must remain a literal string"
            ) from error
        if not isinstance(value, str):
            raise ManifestError(f"{path}: {symbol} must be a string")
        values.append(value)
    if len(values) != 1:
        raise ManifestError(
            f"{path}: expected exactly one assignment to {symbol}"
        )
    if not values[0]:
        raise ManifestError(f"{path}: {symbol} must be nonblank")
    return values[0]


def load_current_prompt_contract(
    repo_root: str | pathlib.Path,
) -> PromptContractReceipt:
    """Load and cross-check the current backend and model system prompts.

    Both independently maintained prompt sources must be byte-identical as
    Python strings.  Their source hashes and the historical audio-only
    contract source hash are retained so the one-time build is reproducible.
    """

    root = pathlib.Path(repo_root).resolve()
    backend_path = root / _BACKEND_PROMPT_PATH
    model_path = root / _MODEL_PROMPT_PATH
    latest_contract_path = root / _LATEST_AUDIO_ONLY_CONTRACT_PATH
    backend_source = _read_source(backend_path, label="backend prompt source")
    model_source = _read_source(model_path, label="model prompt source")
    contract_source = _read_source(
        latest_contract_path,
        label="latest audio-only tuning contract",
    )
    backend_prompt = _literal_string_assignment(
        backend_source,
        path=backend_path,
        symbol=_BACKEND_PROMPT_SYMBOL,
    )
    model_prompt = _literal_string_assignment(
        model_source,
        path=model_path,
        symbol=_MODEL_PROMPT_SYMBOL,
    )
    if backend_prompt != model_prompt:
        raise ManifestError(
            "production/model system prompt drift: "
            f"{_BACKEND_PROMPT_PATH} != {_MODEL_PROMPT_PATH}"
        )
    prompt_bytes = backend_prompt.encode("utf-8")
    return PromptContractReceipt(
        system_prompt=backend_prompt,
        system_prompt_sha256=_sha256(prompt_bytes),
        backend_prompt_source=_BACKEND_PROMPT_PATH.as_posix(),
        backend_prompt_source_sha256=_sha256(backend_source),
        model_prompt_source=_MODEL_PROMPT_PATH.as_posix(),
        model_prompt_source_sha256=_sha256(model_source),
        latest_audio_only_contract_source=(
            _LATEST_AUDIO_ONLY_CONTRACT_PATH.as_posix()
        ),
        latest_audio_only_contract_source_sha256=_sha256(contract_source),
        audio_only_schema_sha256=artifacts.canonical_json_sha256(
            _AUDIO_ONLY_SCHEMA
        ),
    )


def canonical_audio_uri(
    proposed_gcs_prefix: str,
    request: domain.Request,
) -> str:
    """Return the proposed immutable FLAC URI for one request."""

    if (
        not isinstance(proposed_gcs_prefix, str)
        or not proposed_gcs_prefix.startswith("gs://")
        or not proposed_gcs_prefix.endswith("/")
    ):
        raise ManifestError(
            "proposed_gcs_prefix must be a gs:// prefix ending in '/'"
        )
    bucket_and_prefix = proposed_gcs_prefix.removeprefix("gs://").split("/", 1)
    if (
        len(bucket_and_prefix) != 2
        or not bucket_and_prefix[0]
        or not bucket_and_prefix[1]
    ):
        raise ManifestError(
            "proposed_gcs_prefix must include bucket and object prefix"
        )
    if _REQUEST_ID.fullmatch(request.request_id) is None:
        raise ManifestError(
            f"request_id is not safe for a canonical URI: {request.request_id!r}"
        )
    if request.family not in _FAMILIES:
        raise ManifestError(f"unsupported request family: {request.family!r}")
    return (
        f"{proposed_gcs_prefix}audio/{request.split.value}/"
        f"{request.family}/{request.request_id}.flac"
    )


def _topology_index(
    requests: Sequence[domain.Request],
    topology_ids_by_request: Mapping[str, Sequence[str]],
) -> dict[str, tuple[str, ...]]:
    request_ids = {request.request_id for request in requests}
    if set(topology_ids_by_request) != request_ids:
        missing = sorted(request_ids - set(topology_ids_by_request))
        extra = sorted(set(topology_ids_by_request) - request_ids)
        raise ManifestError(
            "topology/request mismatch; "
            f"missing={missing[:5]}, extra={extra[:5]}"
        )
    result: dict[str, tuple[str, ...]] = {}
    for request_id in sorted(request_ids):
        raw_ids = topology_ids_by_request[request_id]
        if isinstance(raw_ids, (str, bytes)) or not isinstance(
            raw_ids, Sequence
        ):
            raise ManifestError(
                f"{request_id}: topology IDs must be a sequence"
            )
        ids = tuple(raw_ids)
        if (
            not ids
            or any(
                not isinstance(value, str)
                or not value.strip()
                or value != value.strip()
                for value in ids
            )
            or len(ids) != len(set(ids))
        ):
            raise ManifestError(
                f"{request_id}: topology IDs must be unique nonblank strings"
            )
        result[request_id] = tuple(sorted(ids))
    return result


def _receipt_index(
    requests: Sequence[domain.Request],
    bindings: Sequence[MaterializationBinding],
) -> dict[str, media.SliceReceipt]:
    request_ids = {request.request_id for request in requests}
    result: dict[str, media.SliceReceipt] = {}
    for binding in bindings:
        if not isinstance(binding, MaterializationBinding):
            raise ManifestError(
                "materialization bindings must be MaterializationBinding"
            )
        if binding.request_id in result:
            raise ManifestError(
                f"duplicate materialization receipt: {binding.request_id}"
            )
        result[binding.request_id] = binding.receipt
    if set(result) != request_ids:
        missing = sorted(request_ids - set(result))
        extra = sorted(set(result) - request_ids)
        raise ManifestError(
            "materialization receipt/request mismatch; "
            f"missing={missing[:5]}, extra={extra[:5]}"
        )
    return result


def _validate_request_universe(requests: Sequence[domain.Request]) -> None:
    if not requests:
        raise ManifestError("reconstruction requests must not be empty")
    request_ids: set[str] = set()
    owner_ids: set[str] = set()
    for request in requests:
        if not isinstance(request, domain.Request):
            raise ManifestError("requests must be domain.Request objects")
        if request.request_id in request_ids:
            raise ManifestError(f"duplicate request ID: {request.request_id}")
        request_ids.add(request.request_id)
        normalized = domain.normalize_text(request.text)
        if normalized is None or normalized != request.text:
            raise ManifestError(
                f"{request.request_id}: target must be nonblank and normalized"
            )
        repeated = owner_ids.intersection(request.owner_ids)
        if repeated:
            raise ManifestError(
                f"owner appears in multiple requests: {sorted(repeated)[:5]}"
            )
        owner_ids.update(request.owner_ids)


def _validate_receipt(
    request: domain.Request,
    receipt: media.SliceReceipt,
) -> None:
    if receipt.output_format != "FLAC" or receipt.output_codec != "flac":
        raise ManifestError(
            f"{request.request_id}: materialized MIME is not audio/flac"
        )
    if pathlib.Path(receipt.output_path).suffix.lower() != ".flac":
        raise ManifestError(
            f"{request.request_id}: local materialization is not a FLAC path"
        )
    expected_frames = request.end_frame - request.start_frame
    expected_duration = _seconds(expected_frames, request.sample_rate)
    mismatches = {
        "source_uri": (receipt.source_uri, request.source_uri),
        "start_frame": (receipt.start_frame, request.start_frame),
        "end_frame": (receipt.end_frame, request.end_frame),
        "frame_count": (receipt.frame_count, expected_frames),
        "sample_rate": (receipt.sample_rate, request.sample_rate),
        "duration_seconds": (
            receipt.duration_seconds,
            expected_duration,
        ),
    }
    changed = [
        name
        for name, (observed, expected) in mismatches.items()
        if observed != expected
    ]
    if changed:
        raise ManifestError(
            f"{request.request_id}: materialization receipt mismatch: "
            f"{', '.join(changed)}"
        )
    if receipt.source_pcm_sha256 != receipt.output_decoded_pcm_sha256:
        raise ManifestError(
            f"{request.request_id}: materialized decoded PCM differs "
            "from source slice"
        )
    for label, value in (
        ("source_encoded_sha256", receipt.source_encoded_sha256),
        ("source_pcm_sha256", receipt.source_pcm_sha256),
        ("output_encoded_sha256", receipt.output_encoded_sha256),
        ("output_decoded_pcm_sha256", receipt.output_decoded_pcm_sha256),
        ("encoder_binary_sha256", receipt.encoder_binary_sha256),
        ("encoder_command_sha256", receipt.encoder_command_sha256),
        ("encoder_contract_sha256", receipt.encoder_contract_sha256),
    ):
        _require_sha256(value, label=f"{request.request_id}.{label}")
    if receipt.encoder_contract_sha256 != media.ENCODER_CONTRACT_SHA256:
        raise ManifestError(
            f"{request.request_id}: encoder contract SHA-256 is not frozen"
        )
    expected_profile = (
        media.EncoderProfile.BCFY_FEEDS_SOUNDFILE
        if request.family == "bcfy_feeds"
        else media.EncoderProfile.PRESEGMENTED_FFMPEG
    )
    if receipt.encoder_profile != expected_profile.value:
        raise ManifestError(
            f"{request.request_id}: encoder profile does not match family"
        )
    expected_pcm_format = {
        "PCM_16": "s16le",
        "PCM_24": "s32le",
    }.get(receipt.subtype)
    if (
        expected_pcm_format is None
        or receipt.encoder_input_pcm_format != expected_pcm_format
        or not isinstance(receipt.encoder_version, str)
        or not receipt.encoder_version
    ):
        raise ManifestError(f"{request.request_id}: invalid encoder metadata")
    if expected_profile is media.EncoderProfile.BCFY_FEEDS_SOUNDFILE:
        if (
            receipt.encoder_version
            != ("python-soundfile 0.13.1; libsndfile 1.2.2; libFLAC 1.4.3")
            or receipt.encoder_python_artifact_sha256 is None
            or receipt.encoder_cffi_artifact_sha256 is None
            or receipt.encoder_embedded_libflac_version != "1.4.3"
            or receipt.encoder_binary_sha256
            != media.PINNED_LIBSNDFILE_BINARY_SHA256
            or receipt.encoder_python_artifact_sha256
            != media.PINNED_SOUNDFILE_PYTHON_ARTIFACT_SHA256
            or receipt.encoder_cffi_artifact_sha256
            != media.PINNED_SOUNDFILE_CFFI_ARTIFACT_SHA256
            or receipt.encoder_production_image_uri is not None
            or receipt.encoder_production_revision is not None
        ):
            raise ManifestError(
                f"{request.request_id}: feed encoder provenance is invalid"
            )
        _require_sha256(
            receipt.encoder_python_artifact_sha256,
            label=(f"{request.request_id}.encoder_python_artifact_sha256"),
        )
        _require_sha256(
            receipt.encoder_cffi_artifact_sha256,
            label=f"{request.request_id}.encoder_cffi_artifact_sha256",
        )
    else:
        if (
            receipt.encoder_version != "6.1.1"
            or receipt.encoder_binary_sha256 != media.PINNED_FFMPEG_SHA256
            or receipt.encoder_python_artifact_sha256 is not None
            or receipt.encoder_cffi_artifact_sha256 is not None
            or receipt.encoder_embedded_libflac_version is not None
            or not isinstance(receipt.encoder_production_image_uri, str)
            or "@sha256:" not in receipt.encoder_production_image_uri
            or not isinstance(receipt.encoder_production_revision, str)
            or not receipt.encoder_production_revision
        ):
            raise ManifestError(
                f"{request.request_id}: FFmpeg encoder provenance is invalid"
            )
        _require_sha256(
            receipt.encoder_binary_sha256,
            label=f"{request.request_id}.encoder_binary_sha256",
        )
    if (
        isinstance(receipt.source_generation, bool)
        or not isinstance(receipt.source_generation, int)
        or receipt.source_generation <= 0
        or isinstance(receipt.output_encoded_size_bytes, bool)
        or not isinstance(receipt.output_encoded_size_bytes, int)
        or receipt.output_encoded_size_bytes <= 0
        or isinstance(receipt.channels, bool)
        or not isinstance(receipt.channels, int)
        or receipt.channels <= 0
        or not isinstance(receipt.subtype, str)
        or not receipt.subtype
    ):
        raise ManifestError(
            f"{request.request_id}: invalid materialization media metadata"
        )


def _ordered_requests(
    requests: Sequence[domain.Request],
) -> tuple[domain.Request, ...]:
    return tuple(
        sorted(
            requests,
            key=lambda request: (
                request.split.value,
                request.family,
                request.source_uri,
                request.start_frame,
                request.end_frame,
                request.request_id,
            ),
        )
    )


def _canonical_row(
    *,
    request: domain.Request,
    receipt: media.SliceReceipt,
    audio_uri: str,
    topology_ids: tuple[str, ...],
    request_order: int,
) -> dict[str, object]:
    frame_count = request.end_frame - request.start_frame
    offset_seconds = _seconds(request.start_frame, request.sample_rate)
    duration_seconds = _seconds(frame_count, request.sample_rate)
    source_audio = {
        "audio_filepath": request.source_uri,
        "duration": float(duration_seconds),
        "duration_seconds_exact": duration_seconds,
        "end_frame": request.end_frame,
        "offset": float(offset_seconds),
        "offset_seconds_exact": offset_seconds,
        "sample_rate": request.sample_rate,
        "start_frame": request.start_frame,
    }
    context_episode_id = (
        request.request_id if request.family == "echo" else request.source_uri
    )
    row: dict[str, object] = {
        "audio_filepath": audio_uri,
        "context_episode_id": context_episode_id,
        "dataset": {
            "family": request.family,
            "name": request.family,
        },
        "duration": float(duration_seconds),
        "example_id": request.request_id,
        "lang": "en_us",
        "offset": 0.0,
        "owner_ids": list(request.owner_ids),
        "request_id": request.request_id,
        "request_order": request_order,
        "segment_id": request.request_id,
        "source_audio": source_audio,
        "source_lineage": {
            "context_episode_id": context_episode_id,
            "owner_ids": list(request.owner_ids),
            "source_duration_seconds_exact": duration_seconds,
            "source_encoded_sha256": receipt.source_encoded_sha256,
            "source_end_frame": request.end_frame,
            "source_generation": receipt.source_generation,
            "source_offset_seconds_exact": offset_seconds,
            "source_sample_rate": request.sample_rate,
            "source_start_frame": request.start_frame,
            "source_uri": request.source_uri,
            "topology_ids": list(topology_ids),
        },
        "text": request.text,
        "topology_ids": list(topology_ids),
    }
    # Provider validation is an exact subset of eval rows.  Eval rows omit the
    # optional role-specific split field so any selected row can be packaged
    # as validation without changing its bytes.  Training keeps its explicit
    # split.
    if request.split is domain.Split.TRAIN:
        row["split"] = request.split.value
    _assert_no_local_path(row, label=f"canonical row {request.request_id}")
    return row


def _assert_no_local_path(value: object, *, label: str) -> None:
    if isinstance(value, pathlib.Path):
        raise ManifestError(f"{label} contains a local Path")
    if isinstance(value, Mapping):
        for key, child in value.items():
            normalized = str(key).lower()
            if normalized in {
                "cache_path",
                "local_output_path",
                "output_path",
            }:
                raise ManifestError(f"{label} contains local path field {key}")
            _assert_no_local_path(child, label=label)
    elif isinstance(value, (list, tuple)):
        for child in value:
            _assert_no_local_path(child, label=label)


def _provider_example(
    row: Mapping[str, object],
    *,
    prompt: str,
) -> dict[str, object]:
    audio_uri = row.get("audio_filepath")
    target = row.get("text")
    if (
        not isinstance(audio_uri, str)
        or not audio_uri.startswith("gs://")
        or not audio_uri.endswith(".flac")
    ):
        raise ManifestError("provider audio URI must be a gs:// FLAC object")
    if not isinstance(target, str) or domain.normalize_text(target) is None:
        raise ManifestError("provider target must be nonblank")
    example: dict[str, object] = {
        "contents": [
            {
                "parts": [
                    {
                        "fileData": {
                            "fileUri": audio_uri,
                            "mimeType": _AUDIO_MIME,
                        }
                    }
                ],
                "role": "user",
            },
            {"parts": [{"text": target}], "role": "model"},
        ],
        "systemInstruction": {
            "parts": [{"text": prompt}],
            "role": "system",
        },
    }
    _validate_provider_example(
        example,
        expected_audio_uri=audio_uri,
        expected_prompt=prompt,
        expected_target=target,
    )
    _assert_no_local_path(example, label="provider example")
    return example


def _validate_provider_example(
    example: Mapping[str, object],
    *,
    expected_audio_uri: str,
    expected_prompt: str,
    expected_target: str,
) -> None:
    expected = {
        "contents": [
            {
                "parts": [
                    {
                        "fileData": {
                            "fileUri": expected_audio_uri,
                            "mimeType": _AUDIO_MIME,
                        }
                    }
                ],
                "role": "user",
            },
            {"parts": [{"text": expected_target}], "role": "model"},
        ],
        "systemInstruction": {
            "parts": [{"text": expected_prompt}],
            "role": "system",
        },
    }
    if dict(example) != expected:
        raise ManifestError(
            "Gemini example drifted from the latest audio-only user/model "
            "contract"
        )


def _mapping_row(
    *,
    request: domain.Request,
    receipt: media.SliceReceipt,
    canonical_uri: str,
) -> dict[str, object]:
    return {
        "canonical_audio_uri": canonical_uri,
        "encoder": {
            "binary_sha256": receipt.encoder_binary_sha256,
            "cffi_artifact_sha256": (receipt.encoder_cffi_artifact_sha256),
            "command_sha256": receipt.encoder_command_sha256,
            "contract_sha256": receipt.encoder_contract_sha256,
            "embedded_libflac_version": (
                receipt.encoder_embedded_libflac_version
            ),
            "input_pcm_format": receipt.encoder_input_pcm_format,
            "production_image_uri": receipt.encoder_production_image_uri,
            "production_revision": receipt.encoder_production_revision,
            "profile": receipt.encoder_profile,
            "python_artifact_sha256": (receipt.encoder_python_artifact_sha256),
            "version": receipt.encoder_version,
        },
        "local_output_path": str(pathlib.Path(receipt.output_path)),
        "output": {
            "channels": receipt.channels,
            "codec": receipt.output_codec,
            "decoded_pcm_sha256": receipt.output_decoded_pcm_sha256,
            "duration_seconds_exact": receipt.duration_seconds,
            "encoded_size_bytes": receipt.output_encoded_size_bytes,
            "encoded_sha256": receipt.output_encoded_sha256,
            "format": receipt.output_format,
            "frame_count": receipt.frame_count,
            "sample_rate": receipt.sample_rate,
            "subtype": receipt.subtype,
        },
        "request_id": request.request_id,
        "source": {
            "encoded_sha256": receipt.source_encoded_sha256,
            "end_frame": receipt.end_frame,
            "generation": receipt.source_generation,
            "pcm_sha256": receipt.source_pcm_sha256,
            "start_frame": receipt.start_frame,
            "uri": receipt.source_uri,
        },
    }


def build_manifest_artifacts(
    *,
    requests: Sequence[domain.Request],
    materialization_bindings: Sequence[MaterializationBinding],
    proposed_gcs_prefix: str,
    topology_ids_by_request: Mapping[str, Sequence[str]],
    repo_root: str | pathlib.Path,
) -> ManifestArtifacts:
    """Serialize train, full primary eval, and provider-safe validation.

    Provider validation is a deterministic, outcome-blind subset of the
    reconstructed eval.  The full primary eval remains unchanged for scoring.
    The prior-context lane is metadata-only here because its context must later
    be populated from same-checkpoint predictions.  No atomic unmasked eval
    lane is produced.
    """

    request_list = tuple(requests)
    _validate_request_universe(request_list)
    prompt_receipt = load_current_prompt_contract(repo_root)
    topology_index = _topology_index(
        request_list,
        topology_ids_by_request,
    )
    receipt_index = _receipt_index(
        request_list,
        tuple(materialization_bindings),
    )
    ordered = _ordered_requests(request_list)
    seen_audio_uris: set[str] = set()
    source_ordinals: dict[tuple[domain.Split, str], int] = {}
    train_rows_list: list[dict[str, object]] = []
    eval_rows_list: list[dict[str, object]] = []
    mapping_rows: list[dict[str, object]] = []
    for request in ordered:
        receipt = receipt_index[request.request_id]
        _validate_receipt(request, receipt)
        audio_uri = canonical_audio_uri(proposed_gcs_prefix, request)
        if audio_uri in seen_audio_uris:
            raise ManifestError(f"duplicate canonical audio URI: {audio_uri}")
        seen_audio_uris.add(audio_uri)
        source_key = (request.split, request.source_uri)
        request_order = source_ordinals.get(source_key, 0)
        source_ordinals[source_key] = request_order + 1
        canonical_row = _canonical_row(
            request=request,
            receipt=receipt,
            audio_uri=audio_uri,
            topology_ids=topology_index[request.request_id],
            request_order=request_order,
        )
        if request.split is domain.Split.TRAIN:
            train_rows_list.append(canonical_row)
        else:
            eval_rows_list.append(canonical_row)
        mapping_rows.append(
            _mapping_row(
                request=request,
                receipt=receipt,
                canonical_uri=audio_uri,
            )
        )

    train_rows = tuple(train_rows_list)
    eval_rows = tuple(eval_rows_list)
    if not train_rows or not eval_rows:
        raise ManifestError("both train and eval requests must be nonempty")
    canonical_train = _jsonl(train_rows)
    canonical_eval = _jsonl(eval_rows)
    provider_validation_view = eval_views.prepare_provider_validation(
        eval_rows,
        train_count=len(train_rows),
        input_lock_digest=_sha256(canonical_eval),
    )
    provider_validation_rows = provider_validation_view.rows
    canonical_validation = provider_validation_view.jsonl
    provider_train = tuple(
        _provider_example(
            row,
            prompt=prompt_receipt.system_prompt,
        )
        for row in train_rows
    )
    provider_validation = tuple(
        _provider_example(
            row,
            prompt=prompt_receipt.system_prompt,
        )
        for row in provider_validation_rows
    )
    gemini_train = _jsonl(provider_train)
    gemini_validation = _jsonl(provider_validation)
    mapping_rows_tuple = tuple(mapping_rows)
    mapping_jsonl = _jsonl(mapping_rows_tuple)
    eval_lane_receipt: dict[str, object] = {
        "atomic_unmasked_eval_included": False,
        "lanes": {
            "masked_v2": {
                "construction": "unchanged_rows_refiltered_against_final_train",
                "kind": "masked_eval",
            },
            "predicted_prior_context_10": {
                "base_lane": "reconstructed_primary",
                "context_source": (
                    "same_checkpoint_prior_predicted_transcriptions_only"
                ),
                "kind": "inference_view",
                "max_prior_segments": 10,
                "reference_transcription_used_as_context": False,
            },
            "reconstructed_primary": {
                "kind": "reference_manifest",
                "reference_retained_for_scoring": True,
                "row_count": len(eval_rows),
                "sha256": _sha256(canonical_eval),
            },
        },
        "schema_version": 1,
        "validation": {
            "algorithm": provider_validation_view.receipt["algorithm"],
            "canonical_dataset_is_deterministic_primary_subset": True,
            "full_primary_sha256": _sha256(canonical_eval),
            "row_audio_text_bytes_are_from_primary_eval": True,
            "row_count": len(provider_validation_rows),
            "sha256": _sha256(canonical_validation),
        },
    }
    manifest_receipt: dict[str, object] = {
        "canonical": {
            "eval": {
                "row_count": len(eval_rows),
                "sha256": _sha256(canonical_eval),
            },
            "train": {
                "row_count": len(train_rows),
                "sha256": _sha256(canonical_train),
            },
            "validation": {
                "derived_from_eval_as_deterministic_subset": True,
                "full_eval_sha256": _sha256(canonical_eval),
                "row_count": len(provider_validation_rows),
                "sha256": _sha256(canonical_validation),
            },
        },
        "gemini": {
            "train": {
                "row_count": len(provider_train),
                "sha256": _sha256(gemini_train),
            },
            "validation": {
                "derived_from_canonical_provider_validation_in_identical_order": (
                    True
                ),
                "row_count": len(provider_validation),
                "sha256": _sha256(gemini_validation),
            },
        },
        "local_paths_present_only_in_materialization_mapping": True,
        "materialization_mapping": {
            "row_count": len(mapping_rows_tuple),
            "sha256": _sha256(mapping_jsonl),
        },
        "prompt_contract_sha256": artifacts.canonical_json_sha256(
            dataclasses.asdict(prompt_receipt)
        ),
        "schema_version": 1,
    }
    result = ManifestArtifacts(
        canonical_train_rows=train_rows,
        canonical_eval_rows=eval_rows,
        provider_validation_rows=provider_validation_rows,
        provider_train_examples=provider_train,
        provider_validation_examples=provider_validation,
        canonical_train_jsonl=canonical_train,
        canonical_eval_jsonl=canonical_eval,
        canonical_validation_jsonl=canonical_validation,
        primary_eval_jsonl=canonical_eval,
        gemini_train_jsonl=gemini_train,
        gemini_validation_jsonl=gemini_validation,
        materialization_mapping_rows=mapping_rows_tuple,
        materialization_mapping_jsonl=mapping_jsonl,
        prompt_contract_receipt=prompt_receipt,
        provider_validation_receipt=copy.deepcopy(
            provider_validation_view.receipt
        ),
        eval_lane_receipt=eval_lane_receipt,
        manifest_receipt=manifest_receipt,
    )
    if result.canonical_eval_jsonl != result.primary_eval_jsonl:
        raise AssertionError(
            "canonical and primary eval serialization diverged"
        )
    if result.canonical_validation_jsonl != _jsonl(
        result.provider_validation_rows
    ):
        raise AssertionError("provider-validation serialization diverged")
    return result


# Descriptive alias used by one-time orchestration code.
serialize_manifests = build_manifest_artifacts
