"""Runnable predicted-prior-context evaluation for reconstructed eval rows.

The reconstruction stage intentionally emits a provider-free schedule.  This
module is the thin online layer that turns that schedule into production-shaped
Google Gen AI SDK calls.  It never reads reference transcripts:

* every checkpoint has an independent rolling-context builder;
* context episodes run concurrently, while requests in an episode are serial;
* only the effective transcript persisted by production can enter history;
* every logical application attempt and every terminal request stays in the
  operational scoring ledger, including provider input-decode rejection.

The Google SDK may make more than one physical HTTP attempt inside one
``generate_content`` call.  Accordingly, this module deliberately calls its
rows ``logical_application_attempt`` and never claims a physical-call count.
"""

# One-time fail-loud evaluator. Direct diagnostics are intentional.
# ruff: noqa: EM101, EM102, PLC0415, PLR0911, PLR0912, PLR0915, TRY003, TRY004, UP047

from __future__ import annotations

import asyncio
import collections
import collections.abc
import copy
import dataclasses
import enum
import hashlib
import json
import pathlib
import re
import time
from typing import Any, Protocol, TypeVar, runtime_checkable

from common.gemini import context as gemini_context

from . import artifacts, eval_views, manifests

PRODUCTION_TEMPERATURE = 0.0
PRODUCTION_MAX_OUTPUT_TOKENS = 512
PRODUCTION_THINKING_BUDGET = 0
PRODUCTION_CLIENT_TIMEOUT_MS = 60_000
PRODUCTION_SDK_RETRY_ATTEMPTS = 3
PRODUCTION_SDK_RETRY_INITIAL_DELAY = 1.0
PRODUCTION_SDK_RETRY_MAX_DELAY = 60.0
PRODUCTION_SDK_RETRY_MULTIPLIER = 2.0
PRODUCTION_MAX_CONNECTIONS = 500
PRODUCTION_MAX_KEEPALIVE_CONNECTIONS = 100
TUNED_APPLICATION_ATTEMPTS = 3

PRODUCTION_SAFETY_SETTINGS = (
    ("HARM_CATEGORY_HATE_SPEECH", "BLOCK_NONE"),
    ("HARM_CATEGORY_SEXUALLY_EXPLICIT", "BLOCK_NONE"),
    ("HARM_CATEGORY_DANGEROUS_CONTENT", "BLOCK_NONE"),
    ("HARM_CATEGORY_HARASSMENT", "BLOCK_NONE"),
    ("HARM_CATEGORY_CIVIC_INTEGRITY", "BLOCK_NONE"),
)

DECODE_REJECTION_MESSAGE = (
    "Failed to decode audio or visual data. Please make sure the audio or "
    "visual data is valid."
)

HISTORY_MODE = "guarded_transcript_block"
HISTORY_SOURCE = "same_checkpoint_prior_effective_predictions_only"
CONTEXT_USER_PROMPT = (
    "IMPORTANT: The current audio is the only clip to transcribe. Use prior "
    "transcripts only as context. Return one line: the verbatim transcript "
    "of the attached radio audio clip.\n\nCurrent audio clip:\n"
)
STAGED_PRIOR_CONTEXT_SCHEDULE = pathlib.Path(
    "eval/predicted_prior_context_10/schedule.jsonl"
)
STAGED_PRIOR_CONTEXT_RECEIPT = pathlib.Path(
    "receipts/eval/prior_context_10.json"
)
STAGED_MATERIALIZATION_MAPPING = pathlib.Path(
    "receipts/materialization_mapping.jsonl"
)
FROZEN_ENCODER_CONTRACT_SHA256 = (
    "f0d22af2c589c36fdff9dd1da1b4f82cc93207582b72d102030ef059e3feab29"
)
FEED_ENCODER_PROFILE = "bcfy_feeds_soundfile_libflac_pcm_preserve_v1"
PRESEGMENTED_ENCODER_PROFILE = (
    "presegmented_ffmpeg_6_1_1_flac5_pcm_preserve_v1"
)
FEED_EMBEDDED_LIBFLAC_VERSION = "1.4.3"

_EFFECTIVE_EMPTY_MARKERS = frozenset(
    {
        "[inaudible]",
        "[unintelligible]",
        "<inaudible>",
        "<unintelligible>",
        "inaudible",
        "unintelligible",
    }
)
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_T = TypeVar("_T")
_GRPC_CODE_BY_NAME = {
    "OK": 0,
    "CANCELLED": 1,
    "UNKNOWN": 2,
    "INVALID_ARGUMENT": 3,
    "DEADLINE_EXCEEDED": 4,
    "NOT_FOUND": 5,
    "ALREADY_EXISTS": 6,
    "PERMISSION_DENIED": 7,
    "RESOURCE_EXHAUSTED": 8,
    "FAILED_PRECONDITION": 9,
    "ABORTED": 10,
    "OUT_OF_RANGE": 11,
    "UNIMPLEMENTED": 12,
    "INTERNAL": 13,
    "UNAVAILABLE": 14,
    "DATA_LOSS": 15,
    "UNAUTHENTICATED": 16,
}


class OutcomeCategory(enum.StrEnum):
    """Mutually exclusive provider/application outcome taxonomy."""

    ACCEPTED_STOP = "accepted_stop"
    BLANK_STOP = "blank_stop"
    EFFECTIVE_EMPTY_STOP = "effective_empty_stop"
    FINISH_REASON_NONE = "finish_reason_none"
    HTTP_499 = "http_499"
    HTTP_504 = "http_504"
    MAX_TOKENS = "max_tokens"
    NO_CANDIDATE = "no_candidate"
    OTHER_ERROR = "other_error"
    OTHER_FINISH_REASON = "other_finish_reason"
    PROVIDER_INPUT_DECODE_REJECTED = "provider_input_decode_rejected"
    SAFETY_BLOCKED = "safety_blocked"
    TRANSPORT_CONNECT_ERROR = "transport_connect_error"
    TRANSPORT_TIMEOUT = "transport_timeout"


class ProviderTargetFatalError(RuntimeError):
    """The target/request setup is invalid, so per-row scoring must stop."""

    def __init__(
        self,
        reason: str,
        *,
        http_status: int | None,
        grpc_code: int | None,
    ) -> None:
        self.reason = reason
        self.http_status = http_status
        self.grpc_code = grpc_code
        super().__init__(
            "provider rejected the checkpoint/request contract; "
            f"reason={reason}; "
            f"http_status={http_status}; grpc_code={grpc_code}"
        )


@dataclasses.dataclass(frozen=True, slots=True)
class ProductionRequestContract:
    """Exact production SDK semantics plus the one context-only extension."""

    system_prompt: str
    system_prompt_sha256: str
    temperature: float = PRODUCTION_TEMPERATURE
    max_output_tokens: int = PRODUCTION_MAX_OUTPUT_TOKENS
    thinking_budget: int = PRODUCTION_THINKING_BUDGET
    safety_settings: tuple[tuple[str, str], ...] = (
        PRODUCTION_SAFETY_SETTINGS
    )
    client_timeout_ms: int = PRODUCTION_CLIENT_TIMEOUT_MS
    sdk_retry_attempts: int = PRODUCTION_SDK_RETRY_ATTEMPTS
    sdk_retry_initial_delay: float = PRODUCTION_SDK_RETRY_INITIAL_DELAY
    sdk_retry_max_delay: float = PRODUCTION_SDK_RETRY_MAX_DELAY
    sdk_retry_multiplier: float = PRODUCTION_SDK_RETRY_MULTIPLIER
    max_connections: int = PRODUCTION_MAX_CONNECTIONS
    max_keepalive_connections: int = (
        PRODUCTION_MAX_KEEPALIVE_CONNECTIONS
    )
    tuned_application_attempts: int = TUNED_APPLICATION_ATTEMPTS

    def __post_init__(self) -> None:
        if not isinstance(self.system_prompt, str) or not self.system_prompt:
            raise ValueError("production system prompt must be nonblank")
        if _SHA256.fullmatch(self.system_prompt_sha256) is None:
            raise ValueError("production system prompt SHA-256 is invalid")
        if (
            hashlib.sha256(self.system_prompt.encode()).hexdigest()
            != self.system_prompt_sha256
        ):
            raise ValueError("production system prompt SHA-256 mismatches text")
        if (
            self.temperature != PRODUCTION_TEMPERATURE
            or self.max_output_tokens != PRODUCTION_MAX_OUTPUT_TOKENS
            or self.thinking_budget != PRODUCTION_THINKING_BUDGET
            or self.safety_settings != PRODUCTION_SAFETY_SETTINGS
            or self.client_timeout_ms != PRODUCTION_CLIENT_TIMEOUT_MS
            or self.sdk_retry_attempts != PRODUCTION_SDK_RETRY_ATTEMPTS
            or self.sdk_retry_initial_delay
            != PRODUCTION_SDK_RETRY_INITIAL_DELAY
            or self.sdk_retry_max_delay != PRODUCTION_SDK_RETRY_MAX_DELAY
            or self.sdk_retry_multiplier
            != PRODUCTION_SDK_RETRY_MULTIPLIER
            or self.max_connections != PRODUCTION_MAX_CONNECTIONS
            or self.max_keepalive_connections
            != PRODUCTION_MAX_KEEPALIVE_CONNECTIONS
            or self.tuned_application_attempts
            != TUNED_APPLICATION_ATTEMPTS
        ):
            raise ValueError("Gemini request contract differs from production")

    def receipt(self) -> dict[str, object]:
        """Return the provider/runtime contract recorded with every run."""
        return {
            "application_retry_policy": {
                "attempts": self.tuned_application_attempts,
                "retry_finish_reasons": ["None", "OTHER"],
                "retry_no_candidate": True,
                "retry_tuned_blank_or_whitespace_stop": True,
                "foundation_fallback_enabled": False,
                "reason_no_fallback": (
                    "checkpoint evaluation measures each tuned checkpoint only"
                ),
            },
            "context_extension": {
                "history_mode": HISTORY_MODE,
                "history_source": HISTORY_SOURCE,
                "max_prior_predictions": eval_views.MAX_PRIOR_CONTEXT,
                "production_delta": (
                    "one guarded text part precedes the unchanged current "
                    "audio/flac part because production has no context API yet"
                ),
                "user_prompt": CONTEXT_USER_PROMPT,
            },
            "generation": {
                "max_output_tokens": self.max_output_tokens,
                "temperature": self.temperature,
                "thinking_budget": self.thinking_budget,
            },
            "safety_settings": [
                {"category": category, "threshold": threshold}
                for category, threshold in self.safety_settings
            ],
            "sdk_http": {
                "logical_attempt_unit": "logical_application_attempt",
                "max_connections": self.max_connections,
                "max_keepalive_connections": (
                    self.max_keepalive_connections
                ),
                "physical_http_attempt_count_claimed": False,
                "retry_attempts": self.sdk_retry_attempts,
                "retry_initial_delay": self.sdk_retry_initial_delay,
                "retry_max_delay": self.sdk_retry_max_delay,
                "retry_multiplier": self.sdk_retry_multiplier,
                "timeout_ms": self.client_timeout_ms,
                "transport": "explicit_httpx_async_client",
            },
            "system_prompt_sha256": self.system_prompt_sha256,
        }


def load_production_request_contract(
    repo_root: str | pathlib.Path,
) -> ProductionRequestContract:
    """Load the exact current production prompt and fixed request settings."""
    prompt = manifests.load_current_prompt_contract(repo_root)
    return ProductionRequestContract(
        system_prompt=prompt.system_prompt,
        system_prompt_sha256=prompt.system_prompt_sha256,
    )


@dataclasses.dataclass(frozen=True, slots=True)
class CheckpointTarget:
    """One independently evaluated tuned checkpoint endpoint."""

    checkpoint_id: str
    model_resource: str
    location: str

    def __post_init__(self) -> None:
        _required_string(self.checkpoint_id, "checkpoint_id")
        _required_string(self.model_resource, "model_resource")
        _required_string(self.location, "location")
        if (
            not self.model_resource.startswith("projects/")
            or "/locations/" not in self.model_resource
            or "/endpoints/" not in self.model_resource
        ):
            raise ValueError(
                "checkpoint model_resource must be a tuned Vertex endpoint"
            )
        match = re.search(r"/locations/([^/]+)/", self.model_resource)
        if match is None or match.group(1) != self.location:
            raise ValueError(
                "checkpoint location differs from endpoint resource"
            )


@dataclasses.dataclass(frozen=True, slots=True)
class RequestMediaEvidence:
    """Frozen encoded/decoded identity attached to every operational outcome."""

    request_id: str
    output_uri: str
    source_uri: str
    source_generation: int
    start_frame: int
    end_frame: int
    duration_seconds: str
    sample_rate: int
    channels: int
    container_format: str
    codec: str
    subtype: str
    encoded_size_bytes: int
    output_encoded_sha256: str
    source_encoded_sha256: str
    source_pcm_sha256: str
    output_pcm_sha256: str
    encoder_profile: str
    encoder_version: str
    encoder_binary_sha256: str
    encoder_python_artifact_sha256: str | None
    encoder_cffi_artifact_sha256: str | None
    encoder_embedded_libflac_version: str | None
    encoder_command_sha256: str
    encoder_contract_sha256: str
    encoder_input_pcm_format: str
    encoder_production_image_uri: str | None
    encoder_production_revision: str | None

    def __post_init__(self) -> None:
        _required_string(self.request_id, "media request_id")
        _required_string(self.output_uri, "media output_uri")
        _required_string(self.source_uri, "media source_uri")
        _positive_int(self.source_generation, "media source_generation")
        _nonnegative_int(self.start_frame, "media start_frame")
        _positive_int(self.end_frame, "media end_frame")
        if self.end_frame <= self.start_frame:
            raise ValueError("media end_frame must be after start_frame")
        _required_string(self.duration_seconds, "media duration_seconds")
        _positive_int(self.sample_rate, "media sample_rate")
        _positive_int(self.channels, "media channels")
        _required_string(self.container_format, "media container_format")
        _required_string(self.codec, "media codec")
        _required_string(self.subtype, "media subtype")
        _positive_int(self.encoded_size_bytes, "media encoded_size_bytes")
        for label, value in (
            ("output_encoded_sha256", self.output_encoded_sha256),
            ("source_encoded_sha256", self.source_encoded_sha256),
            ("source_pcm_sha256", self.source_pcm_sha256),
            ("output_pcm_sha256", self.output_pcm_sha256),
        ):
            if _SHA256.fullmatch(value) is None:
                raise ValueError(f"media {label} is invalid")
        _required_string(self.encoder_profile, "media encoder_profile")
        _required_string(self.encoder_version, "media encoder_version")
        if _SHA256.fullmatch(self.encoder_binary_sha256) is None:
            raise ValueError("media encoder_binary_sha256 is invalid")
        if (
            self.encoder_python_artifact_sha256 is not None
            and _SHA256.fullmatch(
                self.encoder_python_artifact_sha256
            )
            is None
        ):
            raise ValueError(
                "media encoder_python_artifact_sha256 is invalid"
            )
        if (
            self.encoder_cffi_artifact_sha256 is not None
            and _SHA256.fullmatch(self.encoder_cffi_artifact_sha256) is None
        ):
            raise ValueError(
                "media encoder_cffi_artifact_sha256 is invalid"
            )
        if self.encoder_embedded_libflac_version is not None:
            _required_string(
                self.encoder_embedded_libflac_version,
                "media encoder_embedded_libflac_version",
            )
        feed_only_encoder_fields = (
            self.encoder_python_artifact_sha256,
            self.encoder_cffi_artifact_sha256,
            self.encoder_embedded_libflac_version,
        )
        if self.encoder_profile == FEED_ENCODER_PROFILE:
            if any(value is None for value in feed_only_encoder_fields):
                raise ValueError(
                    "feed media evidence lacks Python/CFFI/libFLAC identity"
                )
            if (
                self.encoder_embedded_libflac_version
                != FEED_EMBEDDED_LIBFLAC_VERSION
            ):
                raise ValueError(
                    "feed media evidence has the wrong embedded libFLAC"
                )
        elif self.encoder_profile == PRESEGMENTED_ENCODER_PROFILE:
            if any(value is not None for value in feed_only_encoder_fields):
                raise ValueError(
                    "FFmpeg media evidence contains feed-only identity"
                )
        else:
            raise ValueError("media encoder_profile is not frozen")
        for label, value in (
            ("encoder_command_sha256", self.encoder_command_sha256),
            ("encoder_contract_sha256", self.encoder_contract_sha256),
        ):
            if _SHA256.fullmatch(value) is None:
                raise ValueError(f"media {label} is invalid")
        if (
            self.encoder_contract_sha256
            != FROZEN_ENCODER_CONTRACT_SHA256
        ):
            raise ValueError("media encoder contract SHA-256 differs from frozen")
        _required_string(
            self.encoder_input_pcm_format,
            "media encoder_input_pcm_format",
        )
        for label, value in (
            (
                "encoder_production_image_uri",
                self.encoder_production_image_uri,
            ),
            (
                "encoder_production_revision",
                self.encoder_production_revision,
            ),
        ):
            if value is not None:
                _required_string(value, f"media {label}")

    def as_dict(self) -> dict[str, object]:
        """Return exact media evidence without re-reading or decoding audio."""
        return dataclasses.asdict(self)


def media_evidence_from_bindings(
    schedule: eval_views.PriorContextSchedule,
    bindings: collections.abc.Sequence[manifests.MaterializationBinding],
) -> dict[str, RequestMediaEvidence]:
    """Bind frozen slice receipts to the schedule, trusting frozen decodability.

    This function does not read, decode, or re-hash any frozen media.
    """
    by_request: dict[str, manifests.MaterializationBinding] = {}
    for binding in bindings:
        if binding.request_id in by_request:
            raise ValueError(
                f"duplicate materialization binding: {binding.request_id}"
            )
        by_request[binding.request_id] = binding
    expected = {request.request_id for request in schedule.requests}
    if set(by_request) != expected:
        raise ValueError(
            "materialization bindings do not exactly cover context schedule"
        )
    result: dict[str, RequestMediaEvidence] = {}
    for request in schedule.requests:
        receipt = by_request[request.request_id].receipt
        result[request.request_id] = RequestMediaEvidence(
            request_id=request.request_id,
            output_uri=request.audio_uri,
            source_uri=receipt.source_uri,
            source_generation=receipt.source_generation,
            start_frame=receipt.start_frame,
            end_frame=receipt.end_frame,
            duration_seconds=receipt.duration_seconds,
            sample_rate=receipt.sample_rate,
            channels=receipt.channels,
            container_format=receipt.output_format,
            codec=receipt.output_codec,
            subtype=receipt.subtype,
            encoded_size_bytes=receipt.output_encoded_size_bytes,
            output_encoded_sha256=receipt.output_encoded_sha256,
            source_encoded_sha256=receipt.source_encoded_sha256,
            source_pcm_sha256=receipt.source_pcm_sha256,
            output_pcm_sha256=receipt.output_decoded_pcm_sha256,
            encoder_profile=receipt.encoder_profile,
            encoder_version=receipt.encoder_version,
            encoder_binary_sha256=receipt.encoder_binary_sha256,
            encoder_python_artifact_sha256=(
                receipt.encoder_python_artifact_sha256
            ),
            encoder_cffi_artifact_sha256=(
                receipt.encoder_cffi_artifact_sha256
            ),
            encoder_embedded_libflac_version=(
                receipt.encoder_embedded_libflac_version
            ),
            encoder_command_sha256=receipt.encoder_command_sha256,
            encoder_contract_sha256=receipt.encoder_contract_sha256,
            encoder_input_pcm_format=receipt.encoder_input_pcm_format,
            encoder_production_image_uri=(
                receipt.encoder_production_image_uri
            ),
            encoder_production_revision=(
                receipt.encoder_production_revision
            ),
        )
    return result


def load_staged_prior_context_inputs(
    staged_root: str | pathlib.Path,
) -> tuple[
    eval_views.PriorContextSchedule,
    dict[str, RequestMediaEvidence],
]:
    """Load the frozen schedule and its materialization receipts.

    Frozen media are trusted as already valid.  This loader performs no media
    reads, decode probes, or checksum recomputation; it only binds the exact
    staged request identities to their already-recorded evidence.
    """
    root = pathlib.Path(staged_root)
    schedule_rows = _read_jsonl_objects(
        root / STAGED_PRIOR_CONTEXT_SCHEDULE
    )
    schedule_receipt = _read_json_object(
        root / STAGED_PRIOR_CONTEXT_RECEIPT
    )
    schedule = _schedule_from_staged_rows(
        schedule_rows,
        receipt=schedule_receipt,
    )
    mapping_rows = _read_jsonl_objects(
        root / STAGED_MATERIALIZATION_MAPPING
    )
    mappings_by_request: dict[str, dict[str, object]] = {}
    for row in mapping_rows:
        request_id = _record_string(row, "request_id")
        if request_id in mappings_by_request:
            raise ValueError(
                f"duplicate staged materialization mapping: {request_id}"
            )
        mappings_by_request[request_id] = row
    scheduled_ids = {request.request_id for request in schedule.requests}
    missing = sorted(scheduled_ids - set(mappings_by_request))
    if missing:
        raise ValueError(
            "staged materialization mapping is missing prior-context "
            f"requests: {missing[:5]}"
        )
    media_by_request = {
        request.request_id: _media_evidence_from_mapping_row(
            request,
            mappings_by_request[request.request_id],
        )
        for request in schedule.requests
    }
    return schedule, media_by_request


def _schedule_from_staged_rows(
    rows: collections.abc.Sequence[dict[str, object]],
    *,
    receipt: dict[str, object],
) -> eval_views.PriorContextSchedule:
    if not rows:
        raise ValueError("staged prior-context schedule is empty")
    expected_keys = {
        "audio_uri",
        "context_episode_id",
        "example_id",
        "manifest_index",
        "request_id",
        "segment_id",
        "source_ordinal",
        "source_uri",
    }
    requests: list[eval_views.ScheduledRequest] = []
    request_ids: set[str] = set()
    manifest_indices: set[int] = set()
    for row in rows:
        _require_exact_keys(row, expected_keys, "staged schedule row")
        request = eval_views.ScheduledRequest(
            manifest_index=_record_int(
                row,
                "manifest_index",
                minimum=0,
            ),
            request_id=_record_string(row, "request_id"),
            context_episode_id=_record_string(
                row,
                "context_episode_id",
            ),
            source_uri=_record_string(row, "source_uri"),
            source_ordinal=_record_int(
                row,
                "source_ordinal",
                minimum=0,
            ),
            audio_uri=_record_string(row, "audio_uri"),
            example_id=_record_optional_string(row, "example_id"),
            segment_id=_record_optional_string(row, "segment_id"),
        )
        if request.request_id in request_ids:
            raise ValueError(
                f"duplicate staged schedule request: {request.request_id}"
            )
        if request.manifest_index in manifest_indices:
            raise ValueError(
                "duplicate staged schedule manifest index: "
                f"{request.manifest_index}"
            )
        request_ids.add(request.request_id)
        manifest_indices.add(request.manifest_index)
        requests.append(request)
    grouped: dict[str, list[eval_views.ScheduledRequest]] = (
        collections.defaultdict(list)
    )
    for request in requests:
        grouped[request.context_episode_id].append(request)
    observed_episode_order = tuple(
        dict.fromkeys(request.context_episode_id for request in requests)
    )
    if observed_episode_order != tuple(sorted(grouped)):
        raise ValueError("staged context episodes are not deterministically ordered")
    for episode_id, episode_requests in grouped.items():
        observed_ordinals = [
            request.source_ordinal for request in episode_requests
        ]
        if observed_ordinals != list(range(len(episode_requests))):
            raise ValueError(
                "staged context episode has noncontiguous request order: "
                f"{episode_id}"
            )
    if _record_int(receipt, "schema_version", minimum=1) != 1:
        raise ValueError("unsupported staged prior-context receipt schema")
    if (
        _record_int(receipt, "context_cap", minimum=1)
        != eval_views.MAX_PRIOR_CONTEXT
    ):
        raise ValueError("staged prior-context cap differs from evaluator")
    if _record_int(receipt, "request_count", minimum=1) != len(requests):
        raise ValueError("staged prior-context request count mismatches schedule")
    _record_string(receipt, "request_schedule_sha256")
    if receipt.get("reference_transcript_accessed_for_context") is not False:
        raise ValueError("staged context receipt permits reference access")
    reference_fields = receipt.get("reference_field_names_in_request_schedule")
    if reference_fields != []:
        raise ValueError("staged context schedule contains reference fields")
    return eval_views.PriorContextSchedule(
        requests=tuple(requests),
        receipt=copy.deepcopy(receipt),
    )


def _media_evidence_from_mapping_row(
    request: eval_views.ScheduledRequest,
    row: dict[str, object],
) -> RequestMediaEvidence:
    _require_exact_keys(
        row,
        {
            "canonical_audio_uri",
            "encoder",
            "local_output_path",
            "output",
            "request_id",
            "source",
        },
        "staged materialization mapping",
    )
    if _record_string(row, "request_id") != request.request_id:
        raise ValueError("staged materialization request identity drifted")
    canonical_audio_uri = _record_string(row, "canonical_audio_uri")
    if canonical_audio_uri != request.audio_uri:
        raise ValueError("staged materialization audio URI differs from schedule")
    _record_string(row, "local_output_path")
    encoder = _record_object(row, "encoder")
    output = _record_object(row, "output")
    source = _record_object(row, "source")
    _require_exact_keys(
        encoder,
        {
            "binary_sha256",
            "cffi_artifact_sha256",
            "command_sha256",
            "contract_sha256",
            "embedded_libflac_version",
            "input_pcm_format",
            "python_artifact_sha256",
            "production_image_uri",
            "production_revision",
            "profile",
            "version",
        },
        "staged encoder evidence",
    )
    _require_exact_keys(
        output,
        {
            "channels",
            "codec",
            "decoded_pcm_sha256",
            "duration_seconds_exact",
            "encoded_size_bytes",
            "encoded_sha256",
            "format",
            "frame_count",
            "sample_rate",
            "subtype",
        },
        "staged output evidence",
    )
    _require_exact_keys(
        source,
        {
            "encoded_sha256",
            "end_frame",
            "generation",
            "pcm_sha256",
            "start_frame",
            "uri",
        },
        "staged source evidence",
    )
    source_uri = _record_string(source, "uri")
    if source_uri != request.source_uri:
        raise ValueError("staged materialization source URI differs from schedule")
    return RequestMediaEvidence(
        request_id=request.request_id,
        output_uri=canonical_audio_uri,
        source_uri=source_uri,
        source_generation=_record_int(source, "generation", minimum=1),
        start_frame=_record_int(source, "start_frame", minimum=0),
        end_frame=_record_int(source, "end_frame", minimum=1),
        duration_seconds=_record_string(
            output,
            "duration_seconds_exact",
        ),
        sample_rate=_record_int(output, "sample_rate", minimum=1),
        channels=_record_int(output, "channels", minimum=1),
        container_format=_record_string(output, "format"),
        codec=_record_string(output, "codec"),
        subtype=_record_string(output, "subtype"),
        encoded_size_bytes=_record_int(
            output,
            "encoded_size_bytes",
            minimum=1,
        ),
        output_encoded_sha256=_record_string(output, "encoded_sha256"),
        source_encoded_sha256=_record_string(source, "encoded_sha256"),
        source_pcm_sha256=_record_string(source, "pcm_sha256"),
        output_pcm_sha256=_record_string(output, "decoded_pcm_sha256"),
        encoder_profile=_record_string(encoder, "profile"),
        encoder_version=_record_string(encoder, "version"),
        encoder_binary_sha256=_record_string(
            encoder,
            "binary_sha256",
        ),
        encoder_python_artifact_sha256=_record_optional_string(
            encoder,
            "python_artifact_sha256",
        ),
        encoder_cffi_artifact_sha256=_record_optional_string(
            encoder,
            "cffi_artifact_sha256",
        ),
        encoder_embedded_libflac_version=_record_optional_string(
            encoder,
            "embedded_libflac_version",
        ),
        encoder_command_sha256=_record_string(
            encoder,
            "command_sha256",
        ),
        encoder_contract_sha256=_record_string(
            encoder,
            "contract_sha256",
        ),
        encoder_input_pcm_format=_record_string(
            encoder,
            "input_pcm_format",
        ),
        encoder_production_image_uri=_record_optional_string(
            encoder,
            "production_image_uri",
        ),
        encoder_production_revision=_record_optional_string(
            encoder,
            "production_revision",
        ),
    )


@dataclasses.dataclass(frozen=True, slots=True)
class ProviderRequest:
    """Exact semantic SDK arguments for one checkpoint/request/context state."""

    target: CheckpointTarget
    request_id: str
    context_episode_id: str
    source_ordinal: int
    audio_uri: str
    context_prompt: str
    contents: tuple[dict[str, object], ...]
    system_instruction: str
    generation_config: dict[str, object]
    safety_settings: tuple[dict[str, str], ...]
    model_input_sha256: str
    request_fingerprint: str
    media: RequestMediaEvidence
    contract_receipt: dict[str, object]

    def semantic_sdk_arguments(self) -> dict[str, object]:
        """Return JSON-safe values that map one-for-one to SDK arguments."""
        return {
            "config": {
                "max_output_tokens": self.generation_config[
                    "max_output_tokens"
                ],
                "safety_settings": [
                    copy.deepcopy(item) for item in self.safety_settings
                ],
                "system_instruction": self.system_instruction,
                "temperature": self.generation_config["temperature"],
                "thinking_config": copy.deepcopy(
                    self.generation_config["thinking_config"]
                ),
            },
            "contents": copy.deepcopy(list(self.contents)),
            "model": self.target.model_resource,
        }


def build_provider_request(
    envelope: eval_views.PriorContextEnvelope,
    *,
    target: CheckpointTarget,
    contract: ProductionRequestContract,
    media: RequestMediaEvidence,
) -> ProviderRequest:
    """Compile one envelope without consulting its canonical reference row."""
    if envelope.checkpoint_id != target.checkpoint_id:
        raise ValueError("envelope and checkpoint target disagree")
    if media.request_id != envelope.request.request_id:
        raise ValueError("media evidence belongs to another request")
    if media.output_uri != envelope.request.audio_uri:
        raise ValueError("media output URI differs from scheduled audio URI")
    model_input_sha256 = envelope.receipt.get("model_input_sha256")
    if (
        not isinstance(model_input_sha256, str)
        or _SHA256.fullmatch(model_input_sha256) is None
    ):
        raise ValueError("envelope model input SHA-256 is invalid")
    turns = [
        gemini_context.ContextTurn(
            audio_uri=turn.audio_uri,
            text=turn.prediction,
        )
        for turn in envelope.prior_predictions
    ]
    context_prompt = (
        gemini_context.build_guarded_transcript_context_prompt(
            turns,
            CONTEXT_USER_PROMPT,
        )
    )
    contents = (
        {
            "role": "user",
            "parts": [
                {"text": context_prompt},
                gemini_context.audio_file_data_part(
                    envelope.request.audio_uri
                ),
            ],
        },
    )
    generation_config: dict[str, object] = {
        "max_output_tokens": contract.max_output_tokens,
        "temperature": contract.temperature,
        "thinking_config": {"thinking_budget": contract.thinking_budget},
    }
    safety_settings = tuple(
        {"category": category, "threshold": threshold}
        for category, threshold in contract.safety_settings
    )
    request_basis = {
        "checkpoint_id": target.checkpoint_id,
        "config": {
            "generation": generation_config,
            "safety_settings": list(safety_settings),
            "system_instruction": contract.system_prompt,
        },
        "contents": list(contents),
        "location": target.location,
        "media": media.as_dict(),
        "model": target.model_resource,
        "model_input_sha256": model_input_sha256,
        "request_id": envelope.request.request_id,
    }
    request_fingerprint = _sha256(_canonical_json(request_basis))
    return ProviderRequest(
        target=target,
        request_id=envelope.request.request_id,
        context_episode_id=envelope.request.context_episode_id,
        source_ordinal=envelope.request.source_ordinal,
        audio_uri=envelope.request.audio_uri,
        context_prompt=context_prompt,
        contents=contents,
        system_instruction=contract.system_prompt,
        generation_config=generation_config,
        safety_settings=safety_settings,
        model_input_sha256=model_input_sha256,
        request_fingerprint=request_fingerprint,
        media=media,
        contract_receipt=contract.receipt(),
    )


@dataclasses.dataclass(frozen=True, slots=True)
class RawProviderAttempt:
    """One logical SDK invocation before application-level classification."""

    logical_attempt: int
    candidate_present: bool
    finish_reason: str | None = None
    text_parts: tuple[str, ...] = ()
    http_status: int | None = None
    grpc_code: int | None = None
    provider_status: str | None = None
    provider_message: str | None = None
    response_id: str | None = None
    error_type: str | None = None
    policy_blocked: bool = False
    policy_reasons: tuple[str, ...] = ()
    transport_kind: str | None = None
    latency_ms: int | None = None
    provider_evidence: dict[str, object] = dataclasses.field(
        default_factory=dict
    )

    def __post_init__(self) -> None:
        _positive_int(self.logical_attempt, "logical_attempt")
        if not isinstance(self.candidate_present, bool):
            raise TypeError("candidate_present must be boolean")
        if not isinstance(self.policy_blocked, bool):
            raise TypeError("policy_blocked must be boolean")
        if not isinstance(self.policy_reasons, tuple) or any(
            not isinstance(reason, str) or not reason
            for reason in self.policy_reasons
        ):
            raise TypeError("policy_reasons must be nonblank string tuple")
        if self.transport_kind not in (None, "connect", "timeout"):
            raise ValueError("transport_kind must be connect, timeout, or None")
        if self.finish_reason is not None:
            _required_string(self.finish_reason, "finish_reason")
        if not isinstance(self.text_parts, tuple) or any(
            not isinstance(part, str) for part in self.text_parts
        ):
            raise TypeError("text_parts must be a tuple of strings")
        for label, value in (
            ("http_status", self.http_status),
            ("grpc_code", self.grpc_code),
            ("latency_ms", self.latency_ms),
        ):
            if value is not None:
                _nonnegative_int(value, label)
        for label, value in (
            ("provider_status", self.provider_status),
            ("provider_message", self.provider_message),
            ("response_id", self.response_id),
            ("error_type", self.error_type),
        ):
            if value is not None:
                _required_string(value, label)
        if not isinstance(self.provider_evidence, dict):
            raise TypeError("provider_evidence must be a dict")
        _canonical_json(self.provider_evidence)
        object.__setattr__(
            self,
            "provider_evidence",
            copy.deepcopy(self.provider_evidence),
        )


@dataclasses.dataclass(frozen=True, slots=True)
class ClassifiedAttempt:
    """One mutually exclusive category plus orthogonal acceptance semantics."""

    category: OutcomeCategory
    raw_prediction: str
    normalized_prediction: str | None
    accepted_effective_prediction: bool
    retain_for_context: bool
    retry_application_attempt: bool
    partial: bool
    effective_empty_kind: str | None = None


def classify_attempt(attempt: RawProviderAttempt) -> ClassifiedAttempt:
    """Classify one logical attempt using frozen precedence."""
    raw_prediction = "".join(attempt.text_parts)
    normalized = _normalize_prediction(raw_prediction)
    finish_reason = (
        attempt.finish_reason.strip().upper()
        if attempt.finish_reason is not None
        else None
    )

    if _is_decode_rejection(attempt):
        return _classified(
            OutcomeCategory.PROVIDER_INPUT_DECODE_REJECTED,
            raw_prediction,
        )
    if attempt.http_status == 499:
        return _classified(OutcomeCategory.HTTP_499, raw_prediction)
    if attempt.http_status == 504:
        return _classified(OutcomeCategory.HTTP_504, raw_prediction)
    if (
        attempt.http_status not in (None, 200)
        or attempt.grpc_code not in (None, 0)
    ):
        return _classified(OutcomeCategory.OTHER_ERROR, raw_prediction)
    if attempt.transport_kind == "timeout":
        return _classified(OutcomeCategory.TRANSPORT_TIMEOUT, raw_prediction)
    if attempt.transport_kind == "connect":
        return _classified(
            OutcomeCategory.TRANSPORT_CONNECT_ERROR,
            raw_prediction,
        )
    if attempt.policy_blocked:
        return _classified(OutcomeCategory.SAFETY_BLOCKED, raw_prediction)
    if not attempt.candidate_present:
        return _classified(
            OutcomeCategory.NO_CANDIDATE,
            raw_prediction,
            retry=attempt.error_type == "no_candidate",
        )
    if finish_reason is None:
        return _classified(
            OutcomeCategory.FINISH_REASON_NONE,
            raw_prediction,
            retry=True,
        )
    if finish_reason == "MAX_TOKENS":
        accepted = normalized is not None
        return ClassifiedAttempt(
            category=OutcomeCategory.MAX_TOKENS,
            raw_prediction=raw_prediction,
            normalized_prediction=normalized,
            accepted_effective_prediction=accepted,
            retain_for_context=accepted,
            retry_application_attempt=False,
            partial=True,
            effective_empty_kind=None if accepted else _empty_kind(
                raw_prediction
            ),
        )
    if finish_reason == "STOP":
        if raw_prediction == "":
            return _classified(
                OutcomeCategory.BLANK_STOP,
                raw_prediction,
                retry=True,
                empty_kind="zero_length",
            )
        if normalized is None:
            empty_kind = _empty_kind(raw_prediction)
            return _classified(
                OutcomeCategory.EFFECTIVE_EMPTY_STOP,
                raw_prediction,
                retry=empty_kind == "whitespace_only",
                empty_kind=empty_kind,
            )
        return ClassifiedAttempt(
            category=OutcomeCategory.ACCEPTED_STOP,
            raw_prediction=raw_prediction,
            normalized_prediction=normalized,
            accepted_effective_prediction=True,
            retain_for_context=True,
            retry_application_attempt=False,
            partial=False,
        )
    return _classified(
        OutcomeCategory.OTHER_FINISH_REASON,
        raw_prediction,
        retry=finish_reason == "OTHER",
    )


def _classified(
    category: OutcomeCategory,
    raw_prediction: str,
    *,
    retry: bool = False,
    empty_kind: str | None = None,
) -> ClassifiedAttempt:
    return ClassifiedAttempt(
        category=category,
        raw_prediction=raw_prediction,
        normalized_prediction=None,
        accepted_effective_prediction=False,
        retain_for_context=False,
        retry_application_attempt=retry,
        partial=False,
        effective_empty_kind=empty_kind,
    )


def _is_decode_rejection(attempt: RawProviderAttempt) -> bool:
    return (
        attempt.provider_message == DECODE_REJECTION_MESSAGE
        and (
            attempt.http_status == 400
            or attempt.grpc_code == 3
        )
        and attempt.provider_status in (None, "INVALID_ARGUMENT")
    )


def _empty_kind(text: str) -> str:
    if text == "":
        return "zero_length"
    normalized_whitespace = " ".join(text.split())
    if not normalized_whitespace:
        return "whitespace_only"
    if normalized_whitespace.casefold() in _EFFECTIVE_EMPTY_MARKERS:
        return "placeholder_only"
    raise AssertionError("text is not effective-empty")


def _normalize_prediction(text: str | None) -> str | None:
    if not isinstance(text, str):
        return None
    normalized = " ".join(text.split())
    if (
        not normalized
        or normalized.casefold() in _EFFECTIVE_EMPTY_MARKERS
    ):
        return None
    return normalized


@runtime_checkable
class ProviderAdapter(Protocol):
    """One logical production-configured SDK invocation."""

    async def invoke(
        self,
        request: ProviderRequest,
        *,
        logical_attempt: int,
    ) -> RawProviderAttempt:
        """Invoke the checkpoint endpoint once at the application layer."""


class GoogleGenAIProviderAdapter:
    """Concrete adapter around an injected production-configured Gen AI client.

    The client should be created by :func:`create_production_genai_client`.
    Injection keeps construction testable and ensures importing this module
    performs no authentication or provider work.
    """

    def __init__(self, client: object, *, location: str) -> None:
        self._client = client
        self._location = _required_string(location, "adapter location")

    async def invoke(
        self,
        request: ProviderRequest,
        *,
        logical_attempt: int,
    ) -> RawProviderAttempt:
        if request.target.location != self._location:
            raise ValueError("adapter location differs from checkpoint target")
        try:
            from google.genai import types
        except ImportError as error:
            raise RuntimeError(
                "google-genai is required for provider evaluation"
            ) from error
        parts = [
            types.Part.from_text(text=request.context_prompt),
            types.Part.from_uri(
                file_uri=request.audio_uri,
                mime_type="audio/flac",
            ),
        ]
        contents = types.Content(role="user", parts=parts)
        config = types.GenerateContentConfig(
            temperature=PRODUCTION_TEMPERATURE,
            max_output_tokens=PRODUCTION_MAX_OUTPUT_TOKENS,
            system_instruction=request.system_instruction,
            thinking_config=types.ThinkingConfig(
                thinking_budget=PRODUCTION_THINKING_BUDGET
            ),
            safety_settings=[
                types.SafetySetting(
                    category=types.HarmCategory(item["category"]),
                    threshold=types.HarmBlockThreshold(item["threshold"]),
                )
                for item in request.safety_settings
            ],
        )
        started = time.monotonic()
        try:
            response = await self._client.aio.models.generate_content(
                model=request.target.model_resource,
                contents=contents,
                config=config,
            )
        except Exception as error:
            latency_ms = round((time.monotonic() - started) * 1_000)
            if not _is_provider_runtime_failure(error):
                raise
            attempt = _provider_exception_attempt(
                error,
                logical_attempt=logical_attempt,
                latency_ms=latency_ms,
            )
            fatal_reason = _fatal_target_failure_reason(attempt)
            if fatal_reason is not None:
                raise ProviderTargetFatalError(
                    fatal_reason,
                    http_status=attempt.http_status,
                    grpc_code=attempt.grpc_code,
                ) from error
            return attempt
        latency_ms = round((time.monotonic() - started) * 1_000)
        return _provider_response_attempt(
            response,
            logical_attempt=logical_attempt,
            latency_ms=latency_ms,
        )


def create_production_genai_client(
    *,
    project: str,
    location: str,
) -> object:
    """Create but do not call a client with current production HTTP settings."""
    _required_string(project, "project")
    _required_string(location, "location")
    try:
        import httpx
        from google import genai
        from google.genai import types
    except ImportError as error:
        raise RuntimeError(
            "google-genai and httpx are required for provider evaluation"
        ) from error
    return genai.Client(
        enterprise=True,
        project=project,
        location=location,
        http_options=types.HttpOptions(
            timeout=PRODUCTION_CLIENT_TIMEOUT_MS,
            httpx_async_client=httpx.AsyncClient(
                limits=httpx.Limits(
                    max_connections=PRODUCTION_MAX_CONNECTIONS,
                    max_keepalive_connections=(
                        PRODUCTION_MAX_KEEPALIVE_CONNECTIONS
                    ),
                ),
            ),
            retry_options=types.HttpRetryOptions(
                attempts=PRODUCTION_SDK_RETRY_ATTEMPTS,
                initial_delay=PRODUCTION_SDK_RETRY_INITIAL_DELAY,
                max_delay=PRODUCTION_SDK_RETRY_MAX_DELAY,
                exp_base=PRODUCTION_SDK_RETRY_MULTIPLIER,
            ),
        ),
    )


def _provider_response_attempt(
    response: object,
    *,
    logical_attempt: int,
    latency_ms: int,
) -> RawProviderAttempt:
    candidates = getattr(response, "candidates", None)
    response_id = _optional_string(getattr(response, "response_id", None))
    if not candidates:
        prompt_feedback = getattr(response, "prompt_feedback", None)
        prompt_block_reason = _enum_name(
            getattr(prompt_feedback, "block_reason", None)
        )
        if prompt_block_reason in (
            "BLOCK_REASON_UNSPECIFIED",
            "UNSPECIFIED",
        ):
            prompt_block_reason = None
        prompt_safety_evidence = _safety_rating_evidence(
            getattr(prompt_feedback, "safety_ratings", None) or ()
        )
        policy_reasons = tuple(
            sorted(
                {
                    *(
                        (f"prompt:{prompt_block_reason}",)
                        if prompt_block_reason is not None
                        else ()
                    ),
                    *(
                        f"prompt_rating:{rating['category']}:"
                        f"{rating['probability']}"
                        for rating in prompt_safety_evidence
                        if rating["blocked"] is True
                    ),
                }
            )
        )
        return RawProviderAttempt(
            logical_attempt=logical_attempt,
            candidate_present=False,
            response_id=response_id,
            latency_ms=latency_ms,
            error_type=(
                "prompt_block" if policy_reasons else "no_candidate"
            ),
            policy_blocked=bool(policy_reasons),
            policy_reasons=policy_reasons,
            provider_evidence={
                "prompt_block_reason": prompt_block_reason,
                "prompt_block_reason_message": _optional_string(
                    getattr(
                        prompt_feedback,
                        "block_reason_message",
                        None,
                    )
                ),
                "prompt_safety_ratings": prompt_safety_evidence,
            },
        )
    candidate = candidates[0]
    finish_reason = _enum_name(getattr(candidate, "finish_reason", None))
    candidate_safety_evidence = _safety_rating_evidence(
        getattr(candidate, "safety_ratings", None) or ()
    )
    policy_reasons = _candidate_policy_reasons(
        finish_reason=finish_reason,
        safety_evidence=candidate_safety_evidence,
    )
    content = getattr(candidate, "content", None)
    parts = getattr(content, "parts", None) or ()
    text_parts = tuple(
        text
        for part in parts
        if isinstance((text := getattr(part, "text", None)), str)
        and text
    )
    return RawProviderAttempt(
        logical_attempt=logical_attempt,
        candidate_present=True,
        finish_reason=finish_reason,
        text_parts=text_parts,
        response_id=response_id,
        latency_ms=latency_ms,
        policy_blocked=bool(policy_reasons),
        policy_reasons=policy_reasons,
        provider_evidence={
            "finish_message": _optional_string(
                getattr(candidate, "finish_message", None)
            ),
            "part_count": len(parts),
            "policy_reasons": list(policy_reasons),
            "safety_ratings": candidate_safety_evidence,
            "text_part_count": len(text_parts),
        },
    )


def _candidate_policy_reasons(
    *,
    finish_reason: str | None,
    safety_evidence: collections.abc.Sequence[dict[str, object]],
) -> tuple[str, ...]:
    blocked_finish_reasons = {
        "BLOCKLIST",
        "PROHIBITED_CONTENT",
        "RECITATION",
        "SAFETY",
        "SPII",
    }
    reasons: list[str] = []
    normalized_finish = finish_reason.upper() if finish_reason else None
    if normalized_finish in blocked_finish_reasons:
        reasons.append(f"finish:{normalized_finish}")
    for rating in safety_evidence:
        if rating["blocked"] is not True:
            continue
        category = str(rating["category"])
        probability = str(rating["probability"])
        reasons.append(f"rating:{category}:{probability}")
    return tuple(sorted(set(reasons)))


def _safety_rating_evidence(
    ratings: collections.abc.Iterable[object],
) -> list[dict[str, object]]:
    result: list[dict[str, object]] = []
    for rating in ratings:
        blocked = getattr(rating, "blocked", None)
        if not isinstance(blocked, bool):
            blocked = None
        record: dict[str, object] = {
            "blocked": blocked,
            "category": (
                _enum_name(getattr(rating, "category", None)) or "UNKNOWN"
            ),
            "probability": (
                _enum_name(getattr(rating, "probability", None)) or "UNKNOWN"
            ),
            "severity": (
                _enum_name(getattr(rating, "severity", None)) or "UNKNOWN"
            ),
        }
        for attribute in ("probability_score", "severity_score"):
            value = getattr(rating, attribute, None)
            record[attribute] = (
                float(value)
                if isinstance(value, (int, float))
                and not isinstance(value, bool)
                else None
            )
        result.append(record)
    return result


def _provider_exception_attempt(
    error: Exception,
    *,
    logical_attempt: int,
    latency_ms: int,
) -> RawProviderAttempt:
    http_status, grpc_code, grpc_name = _provider_status_codes(error)
    message = _provider_error_message(error)
    provider_status = (
        _optional_string(getattr(error, "status", None)) or grpc_name
    )
    return RawProviderAttempt(
        logical_attempt=logical_attempt,
        candidate_present=False,
        http_status=http_status,
        grpc_code=grpc_code,
        provider_status=provider_status,
        provider_message=message,
        error_type=type(error).__name__,
        transport_kind=_transport_kind(error),
        latency_ms=latency_ms,
        provider_evidence={
            "exception_module": type(error).__module__,
            "grpc_status_name": grpc_name,
        },
    )


def _is_provider_runtime_failure(error: Exception) -> bool:
    http_status, grpc_code, _grpc_name = _provider_status_codes(error)
    if http_status is not None or grpc_code is not None:
        return True
    if isinstance(error, (ConnectionError, TimeoutError)):
        return True
    try:
        import grpc
    except ImportError:
        pass
    else:
        if isinstance(error, grpc.RpcError):
            return True
    module = type(error).__module__
    provider_modules = (
        "aiohttp",
        "google.api_core",
        "google.genai.errors",
        "grpc",
        "httpx",
    )
    return any(
        module == prefix or module.startswith(f"{prefix}.")
        for prefix in provider_modules
    )


def _fatal_target_failure_reason(
    attempt: RawProviderAttempt,
) -> str | None:
    if _is_decode_rejection(attempt):
        return None
    if attempt.grpc_code is not None:
        return {
            5: "target_not_found",
            7: "permission_denied",
            9: "failed_precondition",
            16: "unauthenticated",
        }.get(attempt.grpc_code)
    if attempt.http_status is not None:
        explicit = {
            401: "unauthenticated",
            403: "permission_denied",
            404: "target_not_found",
            412: "failed_precondition",
        }.get(attempt.http_status)
        if explicit is not None:
            return explicit
    return None


def _provider_status_codes(
    error: Exception,
) -> tuple[int | None, int | None, str | None]:
    raw_code = getattr(error, "code", None)
    if callable(raw_code):
        raw_code = raw_code()
    if isinstance(raw_code, bool):
        return None, None, None
    if isinstance(raw_code, int):
        if raw_code >= 100:
            return raw_code, None, None
        return None, raw_code, _grpc_name_for_code(raw_code)
    name = getattr(raw_code, "name", None)
    grpc_name = (
        name.strip().upper()
        if isinstance(name, str) and name.strip()
        else None
    )
    value = getattr(raw_code, "value", None)
    if isinstance(value, tuple) and value:
        value = value[0]
    if isinstance(value, int) and not isinstance(value, bool):
        return None, value, grpc_name or _grpc_name_for_code(value)
    if grpc_name in _GRPC_CODE_BY_NAME:
        return None, _GRPC_CODE_BY_NAME[grpc_name], grpc_name
    return None, None, grpc_name


def _grpc_name_for_code(code: int) -> str | None:
    return next(
        (
            name
            for name, candidate in _GRPC_CODE_BY_NAME.items()
            if candidate == code
        ),
        None,
    )


def _transport_kind(error: Exception) -> str | None:
    if isinstance(error, TimeoutError):
        return "timeout"
    if isinstance(error, ConnectionError):
        return "connect"
    name = type(error).__name__.casefold()
    if "timeout" in name or "deadlineexceeded" in name:
        return "timeout"
    if (
        "connect" in name
        or "network" in name
        or "serverdisconnected" in name
    ):
        return "connect"
    return None


def _provider_error_message(error: Exception) -> str:
    direct = getattr(error, "message", None)
    if isinstance(direct, str) and direct:
        return direct
    details = getattr(error, "details", None)
    if callable(details):
        rendered = details()
        if isinstance(rendered, str) and rendered:
            return rendered
    return str(error) or type(error).__name__


@dataclasses.dataclass(frozen=True, slots=True)
class AttemptLedgerRow:
    """Auditable logical-attempt row; never interpreted as physical HTTP."""

    checkpoint_id: str
    request_id: str
    context_episode_id: str
    source_ordinal: int
    logical_attempt: int
    request_fingerprint: str
    model_input_sha256: str
    category: OutcomeCategory
    accepted_effective_prediction: bool
    retain_for_context: bool
    retry_application_attempt: bool
    partial: bool
    normalized_prediction: str | None
    effective_empty_kind: str | None
    raw: RawProviderAttempt
    media: RequestMediaEvidence

    def as_dict(self) -> dict[str, object]:
        raw = dataclasses.asdict(self.raw)
        message = self.raw.provider_message
        return {
            "accepted_effective_prediction": (
                self.accepted_effective_prediction
            ),
            "attempt_unit": "logical_application_attempt",
            "category": self.category.value,
            "checkpoint_id": self.checkpoint_id,
            "context_episode_id": self.context_episode_id,
            "effective_empty_kind": self.effective_empty_kind,
            "logical_attempt": self.logical_attempt,
            "media": self.media.as_dict(),
            "message_sha256": (
                _sha256(message.encode()) if message is not None else None
            ),
            "model_input_sha256": self.model_input_sha256,
            "normalized_prediction": self.normalized_prediction,
            "partial": self.partial,
            "physical_http_attempt_count_claimed": False,
            "provider": raw,
            "request_fingerprint": self.request_fingerprint,
            "request_id": self.request_id,
            "retain_for_context": self.retain_for_context,
            "retry_application_attempt": self.retry_application_attempt,
            "source_ordinal": self.source_ordinal,
        }


@dataclasses.dataclass(frozen=True, slots=True)
class RequestLedgerRow:
    """One terminal request row retained for operational WER."""

    checkpoint_id: str
    request_id: str
    context_episode_id: str
    source_ordinal: int
    request_fingerprint: str
    model_input_sha256: str
    terminal_category: OutcomeCategory
    terminal_finish_reason: str | None
    logical_attempt_count: int
    attempt_category_counts: dict[str, int]
    accepted_effective_prediction: bool
    retained_for_future_context: bool
    partial: bool
    normalized_prediction: str | None
    prediction_for_scoring: str
    media: RequestMediaEvidence

    def as_dict(self) -> dict[str, object]:
        return {
            "accepted_effective_prediction": (
                self.accepted_effective_prediction
            ),
            "attempt_category_counts": dict(
                sorted(self.attempt_category_counts.items())
            ),
            "checkpoint_id": self.checkpoint_id,
            "context_episode_id": self.context_episode_id,
            "logical_attempt_count": self.logical_attempt_count,
            "media": self.media.as_dict(),
            "model_input_sha256": self.model_input_sha256,
            "normalized_prediction": self.normalized_prediction,
            "partial": self.partial,
            "prediction_for_scoring": self.prediction_for_scoring,
            "request_fingerprint": self.request_fingerprint,
            "request_id": self.request_id,
            "retained_for_future_context": (
                self.retained_for_future_context
            ),
            "source_ordinal": self.source_ordinal,
            "terminal_category": self.terminal_category.value,
            "terminal_finish_reason": self.terminal_finish_reason,
        }


class LocalCheckpointEvidenceStore:
    """Create-only per-request evidence used for sole-worker crash recovery."""

    def __init__(
        self,
        root: str | pathlib.Path,
        *,
        checkpoint_id: str,
    ) -> None:
        self.root = pathlib.Path(root) / _safe_component(checkpoint_id)

    def load_attempt_payloads(
        self,
        request: ProviderRequest,
        *,
        maximum_attempts: int,
    ) -> tuple[dict[str, object], ...]:
        """Load the existing contiguous logical-attempt prefix."""
        request_root = self._attempt_root(request.request_id)
        if not request_root.exists():
            return ()
        if not request_root.is_dir():
            raise ValueError(
                f"attempt evidence path is not a directory: {request_root}"
            )
        observed = sorted(request_root.glob("*.json"))
        expected_names = [
            f"{index:02d}.json" for index in range(1, len(observed) + 1)
        ]
        if [path.name for path in observed] != expected_names:
            raise ValueError(
                f"logical attempts are not contiguous: {request.request_id}"
            )
        if len(observed) > maximum_attempts:
            raise ValueError(
                f"too many persisted attempts: {request.request_id}"
            )
        return tuple(
            self._load_json(path, label="logical attempt")
            for path in observed
        )

    def write_attempt(self, row: AttemptLedgerRow) -> artifacts.ArtifactReceipt:
        """Persist one logical attempt before deciding whether to advance."""
        return artifacts.write_json_create_only(
            self._attempt_root(row.request_id)
            / f"{row.logical_attempt:02d}.json",
            row.as_dict(),
        )

    def load_terminal_payload(
        self,
        request: ProviderRequest,
    ) -> dict[str, object] | None:
        """Load a terminal request outcome when it already exists."""
        path = self._terminal_path(request.request_id)
        if not path.exists():
            return None
        return self._load_json(path, label="terminal request")

    def write_terminal(
        self,
        row: RequestLedgerRow,
    ) -> artifacts.ArtifactReceipt:
        """Persist terminal effective output before advancing context state."""
        return artifacts.write_json_create_only(
            self._terminal_path(row.request_id),
            row.as_dict(),
        )

    def _attempt_root(self, request_id: str) -> pathlib.Path:
        return self.root / "attempts" / _safe_component(request_id)

    def _terminal_path(self, request_id: str) -> pathlib.Path:
        return (
            self.root
            / "terminal_requests_by_id"
            / f"{_safe_component(request_id)}.json"
        )

    @staticmethod
    def _load_json(
        path: pathlib.Path,
        *,
        label: str,
    ) -> dict[str, object]:
        try:
            value = json.loads(path.read_bytes())
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
            raise ValueError(f"cannot load {label}: {path}") from error
        if not isinstance(value, dict):
            raise ValueError(f"{label} must be a JSON object: {path}")
        return value


@dataclasses.dataclass(frozen=True, slots=True)
class CheckpointRunResult:
    """Complete attempt and terminal ledgers for one checkpoint."""

    target: CheckpointTarget
    attempts: tuple[AttemptLedgerRow, ...]
    requests: tuple[RequestLedgerRow, ...]
    context_receipt: dict[str, object]
    request_contract_receipt: dict[str, object]

    def attempt_category_counts(self) -> dict[str, int]:
        return dict(
            sorted(
                collections.Counter(
                    row.category.value for row in self.attempts
                ).items()
            )
        )

    def terminal_category_counts(self) -> dict[str, int]:
        return dict(
            sorted(
                collections.Counter(
                    row.terminal_category.value for row in self.requests
                ).items()
            )
        )

    def scoring_rows(self) -> tuple[dict[str, object], ...]:
        """Return every request exactly once, including empty hypotheses."""
        return tuple(
            {
                "checkpoint_id": row.checkpoint_id,
                "outcome_category": row.terminal_category.value,
                "pred_text": row.prediction_for_scoring,
                "request_id": row.request_id,
            }
            for row in self.requests
        )


async def _run_fail_fast(
    coroutines: collections.abc.Sequence[
        collections.abc.Coroutine[Any, Any, _T]
    ],
) -> tuple[_T, ...]:
    """Cancel and await every sibling before propagating the first failure."""
    tasks = tuple(asyncio.create_task(coroutine) for coroutine in coroutines)
    try:
        return tuple(await asyncio.gather(*tasks))
    except BaseException:
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        raise


async def run_predicted_prior_context_checkpoints(
    *,
    schedule: eval_views.PriorContextSchedule,
    targets: collections.abc.Sequence[CheckpointTarget],
    adapters_by_checkpoint: collections.abc.Mapping[str, ProviderAdapter],
    contract: ProductionRequestContract,
    media_by_request: collections.abc.Mapping[str, RequestMediaEvidence],
    max_parallel_episodes_per_checkpoint: int,
    evidence_root: str | pathlib.Path,
) -> tuple[CheckpointRunResult, ...]:
    """Run checkpoints concurrently with isolated per-episode causal state."""
    _positive_int(
        max_parallel_episodes_per_checkpoint,
        "max_parallel_episodes_per_checkpoint",
    )
    target_list = tuple(targets)
    if not target_list:
        raise ValueError("at least one checkpoint target is required")
    checkpoint_ids = [target.checkpoint_id for target in target_list]
    if len(checkpoint_ids) != len(set(checkpoint_ids)):
        raise ValueError("checkpoint targets contain duplicate IDs")
    if set(adapters_by_checkpoint) != set(checkpoint_ids):
        raise ValueError("adapter map must exactly match checkpoint targets")
    request_ids = {request.request_id for request in schedule.requests}
    if set(media_by_request) != request_ids:
        raise ValueError("media evidence must exactly cover the schedule")
    results = await _run_fail_fast(
        tuple(
            _run_checkpoint(
                schedule=schedule,
                target=target,
                adapter=adapters_by_checkpoint[target.checkpoint_id],
                contract=contract,
                media_by_request=media_by_request,
                max_parallel_episodes=max_parallel_episodes_per_checkpoint,
                evidence_root=evidence_root,
            )
            for target in target_list
        )
    )
    return tuple(results)


async def _run_checkpoint(
    *,
    schedule: eval_views.PriorContextSchedule,
    target: CheckpointTarget,
    adapter: ProviderAdapter,
    contract: ProductionRequestContract,
    media_by_request: collections.abc.Mapping[str, RequestMediaEvidence],
    max_parallel_episodes: int,
    evidence_root: str | pathlib.Path,
) -> CheckpointRunResult:
    if not isinstance(adapter, ProviderAdapter):
        raise TypeError("adapter does not implement ProviderAdapter")
    builder = eval_views.PredictedPriorContextBuilder(
        schedule,
        checkpoint_id=target.checkpoint_id,
    )
    store = LocalCheckpointEvidenceStore(
        evidence_root,
        checkpoint_id=target.checkpoint_id,
    )
    grouped: dict[str, list[eval_views.ScheduledRequest]] = (
        collections.defaultdict(list)
    )
    for request in schedule.requests:
        grouped[request.context_episode_id].append(request)
    for episode_id, requests in grouped.items():
        requests.sort(key=lambda request: request.source_ordinal)
        if [request.source_ordinal for request in requests] != list(
            range(len(requests))
        ):
            raise ValueError(
                f"context episode has noncontiguous order: {episode_id}"
            )
    semaphore = asyncio.Semaphore(max_parallel_episodes)
    attempts_by_request: dict[str, list[AttemptLedgerRow]] = {}
    requests_by_id: dict[str, RequestLedgerRow] = {}

    async def run_episode(
        episode_requests: collections.abc.Sequence[
            eval_views.ScheduledRequest
        ],
    ) -> None:
        for scheduled in episode_requests:
            envelope = builder.open_request(scheduled.request_id)
            provider_request = build_provider_request(
                envelope,
                target=target,
                contract=contract,
                media=media_by_request[scheduled.request_id],
            )
            attempt_rows = [
                _attempt_from_payload(
                    payload,
                    request=provider_request,
                    logical_attempt=logical_attempt,
                )
                for logical_attempt, payload in enumerate(
                    store.load_attempt_payloads(
                        provider_request,
                        maximum_attempts=(
                            contract.tuned_application_attempts
                        ),
                    ),
                    1,
                )
            ]
            terminal_payload = store.load_terminal_payload(provider_request)
            if terminal_payload is not None:
                request_row = _request_from_payload(
                    terminal_payload,
                    request=provider_request,
                    attempts=attempt_rows,
                )
                _commit_request_to_context(
                    builder,
                    provider_request,
                    request_row,
                )
                attempts_by_request[scheduled.request_id] = attempt_rows
                requests_by_id[scheduled.request_id] = request_row
                continue
            terminal = (
                _classified_from_attempt_row(attempt_rows[-1])
                if attempt_rows
                else None
            )
            next_attempt = len(attempt_rows) + 1
            should_call = (
                terminal is None
                or (
                    terminal.retry_application_attempt
                    and next_attempt <= contract.tuned_application_attempts
                )
            )
            while should_call:
                logical_attempt = next_attempt
                async with semaphore:
                    raw = await adapter.invoke(
                        provider_request,
                        logical_attempt=logical_attempt,
                    )
                if raw.logical_attempt != logical_attempt:
                    raise ValueError(
                        "adapter returned the wrong logical attempt ordinal"
                    )
                classified = classify_attempt(raw)
                attempt_row = _attempt_ledger_row(
                    provider_request,
                    classified=classified,
                    raw=raw,
                )
                store.write_attempt(attempt_row)
                attempt_rows.append(attempt_row)
                terminal = classified
                next_attempt += 1
                should_call = (
                    classified.retry_application_attempt
                    and next_attempt <= contract.tuned_application_attempts
                )
            if terminal is None:
                raise AssertionError("provider request produced no attempt")
            request_row = _request_ledger_row(
                provider_request,
                terminal=terminal,
                attempts=attempt_rows,
            )
            store.write_terminal(request_row)
            _commit_request_to_context(
                builder,
                provider_request,
                request_row,
            )
            attempts_by_request[scheduled.request_id] = attempt_rows
            requests_by_id[scheduled.request_id] = request_row

    await _run_fail_fast(
        tuple(
            run_episode(grouped[episode_id])
            for episode_id in sorted(grouped)
        )
    )
    ordered_requests = tuple(
        requests_by_id[request.request_id] for request in schedule.requests
    )
    ordered_attempts = tuple(
        attempt
        for request in schedule.requests
        for attempt in attempts_by_request[request.request_id]
    )
    if len(ordered_requests) != len(schedule.requests):
        raise AssertionError("operational ledger dropped scheduled requests")
    result = CheckpointRunResult(
        target=target,
        attempts=ordered_attempts,
        requests=ordered_requests,
        context_receipt=builder.receipt(require_complete=True),
        request_contract_receipt=contract.receipt(),
    )
    if len(result.scoring_rows()) != len(schedule.requests):
        raise AssertionError("operational WER projection dropped requests")
    write_checkpoint_result_create_only(evidence_root, result)
    return result


def _attempt_ledger_row(
    request: ProviderRequest,
    *,
    classified: ClassifiedAttempt,
    raw: RawProviderAttempt,
) -> AttemptLedgerRow:
    return AttemptLedgerRow(
        checkpoint_id=request.target.checkpoint_id,
        request_id=request.request_id,
        context_episode_id=request.context_episode_id,
        source_ordinal=request.source_ordinal,
        logical_attempt=raw.logical_attempt,
        request_fingerprint=request.request_fingerprint,
        model_input_sha256=request.model_input_sha256,
        category=classified.category,
        accepted_effective_prediction=(
            classified.accepted_effective_prediction
        ),
        retain_for_context=classified.retain_for_context,
        retry_application_attempt=classified.retry_application_attempt,
        partial=classified.partial,
        normalized_prediction=classified.normalized_prediction,
        effective_empty_kind=classified.effective_empty_kind,
        raw=raw,
        media=request.media,
    )


def _request_ledger_row(
    request: ProviderRequest,
    *,
    terminal: ClassifiedAttempt,
    attempts: collections.abc.Sequence[AttemptLedgerRow],
) -> RequestLedgerRow:
    if not attempts:
        raise ValueError("terminal request must have at least one attempt")
    category_counts = collections.Counter(
        row.category.value for row in attempts
    )
    return RequestLedgerRow(
        checkpoint_id=request.target.checkpoint_id,
        request_id=request.request_id,
        context_episode_id=request.context_episode_id,
        source_ordinal=request.source_ordinal,
        request_fingerprint=request.request_fingerprint,
        model_input_sha256=request.model_input_sha256,
        terminal_category=terminal.category,
        terminal_finish_reason=attempts[-1].raw.finish_reason,
        logical_attempt_count=len(attempts),
        attempt_category_counts=dict(category_counts),
        accepted_effective_prediction=(
            terminal.accepted_effective_prediction
        ),
        retained_for_future_context=terminal.retain_for_context,
        partial=terminal.partial,
        normalized_prediction=terminal.normalized_prediction,
        prediction_for_scoring=terminal.normalized_prediction or "",
        media=request.media,
    )


def _commit_request_to_context(
    builder: eval_views.PredictedPriorContextBuilder,
    request: ProviderRequest,
    row: RequestLedgerRow,
) -> None:
    builder.record_prediction(
        eval_views.PredictionOutcome(
            checkpoint_id=request.target.checkpoint_id,
            request_id=request.request_id,
            model_input_sha256=request.model_input_sha256,
            prediction=row.normalized_prediction,
            accepted=row.accepted_effective_prediction,
            provider_request_fingerprint=request.request_fingerprint,
        )
    )


def _classified_from_attempt_row(
    row: AttemptLedgerRow,
) -> ClassifiedAttempt:
    classified = classify_attempt(row.raw)
    if (
        classified.category != row.category
        or classified.accepted_effective_prediction
        != row.accepted_effective_prediction
        or classified.retain_for_context != row.retain_for_context
        or classified.retry_application_attempt
        != row.retry_application_attempt
        or classified.partial != row.partial
        or classified.normalized_prediction != row.normalized_prediction
        or classified.effective_empty_kind != row.effective_empty_kind
    ):
        raise ValueError("persisted attempt classification is inconsistent")
    return classified


def _attempt_from_payload(
    payload: dict[str, object],
    *,
    request: ProviderRequest,
    logical_attempt: int,
) -> AttemptLedgerRow:
    provider = payload.get("provider")
    if not isinstance(provider, dict):
        raise ValueError("persisted attempt lacks provider evidence")
    raw_fields = copy.deepcopy(provider)
    for field in ("policy_reasons", "text_parts"):
        values = raw_fields.get(field)
        if not isinstance(values, list) or any(
            not isinstance(value, str) for value in values
        ):
            raise ValueError(f"persisted provider {field} are invalid")
        raw_fields[field] = tuple(values)
    try:
        raw = RawProviderAttempt(**raw_fields)
    except (TypeError, ValueError) as error:
        raise ValueError("persisted provider attempt is invalid") from error
    if raw.logical_attempt != logical_attempt:
        raise ValueError("persisted logical-attempt ordinal is invalid")
    classified = classify_attempt(raw)
    row = _attempt_ledger_row(
        request,
        classified=classified,
        raw=raw,
    )
    if _canonical_json(row.as_dict()) != _canonical_json(payload):
        raise ValueError(
            "persisted logical attempt differs from current request contract"
        )
    return row


def _request_from_payload(
    payload: dict[str, object],
    *,
    request: ProviderRequest,
    attempts: collections.abc.Sequence[AttemptLedgerRow],
) -> RequestLedgerRow:
    if not attempts:
        raise ValueError("persisted terminal request has no attempts")
    terminal = _classified_from_attempt_row(attempts[-1])
    if (
        terminal.retry_application_attempt
        and len(attempts)
        < TUNED_APPLICATION_ATTEMPTS
    ):
        raise ValueError("persisted terminal request stopped before retry limit")
    row = _request_ledger_row(
        request,
        terminal=terminal,
        attempts=attempts,
    )
    if _canonical_json(row.as_dict()) != _canonical_json(payload):
        raise ValueError(
            "persisted terminal outcome differs from current request contract"
        )
    return row


def write_checkpoint_result_create_only(
    output_root: str | pathlib.Path,
    result: CheckpointRunResult,
) -> tuple[artifacts.ArtifactReceipt, ...]:
    """Persist final ledgers create-only for crash/result recovery."""
    root = pathlib.Path(output_root) / _safe_component(
        result.target.checkpoint_id
    )
    receipts = [
        artifacts.write_jsonl_create_only(
            root / "logical_attempts.jsonl",
            (row.as_dict() for row in result.attempts),
        ),
        artifacts.write_jsonl_create_only(
            root / "terminal_requests.jsonl",
            (row.as_dict() for row in result.requests),
        ),
        artifacts.write_jsonl_create_only(
            root / "operational_wer_predictions.jsonl",
            result.scoring_rows(),
        ),
        artifacts.write_json_create_only(
            root / "context_receipt.json",
            result.context_receipt,
        ),
        artifacts.write_json_create_only(
            root / "request_contract.json",
            result.request_contract_receipt,
        ),
        artifacts.write_json_create_only(
            root / "_SUCCESS.json",
            {
                "attempt_category_counts": (
                    result.attempt_category_counts()
                ),
                "checkpoint_id": result.target.checkpoint_id,
                "logical_attempt_count": len(result.attempts),
                "request_count": len(result.requests),
                "terminal_category_counts": (
                    result.terminal_category_counts()
                ),
            },
        ),
    ]
    return tuple(receipts)


def _safe_component(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def _enum_name(value: object) -> str | None:
    if value is None:
        return None
    name = getattr(value, "name", None)
    if isinstance(name, str) and name:
        return name
    rendered = str(value)
    return rendered or None


def _required_string(value: object, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{label} must be a nonblank string")
    return value.strip()


def _optional_string(value: object) -> str | None:
    return value if isinstance(value, str) and value else None


def _read_json_object(path: pathlib.Path) -> dict[str, object]:
    try:
        value = json.loads(path.read_bytes())
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError(f"cannot read staged JSON object: {path}") from error
    if not isinstance(value, dict):
        raise ValueError(f"staged JSON must contain one object: {path}")
    return value


def _read_jsonl_objects(path: pathlib.Path) -> tuple[dict[str, object], ...]:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeDecodeError) as error:
        raise ValueError(f"cannot read staged JSONL: {path}") from error
    rows: list[dict[str, object]] = []
    for line_number, line in enumerate(lines, 1):
        if not line:
            raise ValueError(
                f"staged JSONL contains a blank line: {path}:{line_number}"
            )
        try:
            row = json.loads(line)
        except json.JSONDecodeError as error:
            raise ValueError(
                f"staged JSONL is invalid: {path}:{line_number}"
            ) from error
        if not isinstance(row, dict):
            raise ValueError(
                f"staged JSONL row is not an object: {path}:{line_number}"
            )
        rows.append(row)
    if not rows:
        raise ValueError(f"staged JSONL is empty: {path}")
    return tuple(rows)


def _require_exact_keys(
    record: collections.abc.Mapping[str, object],
    expected: set[str],
    label: str,
) -> None:
    observed = set(record)
    if observed != expected:
        raise ValueError(
            f"{label} keys differ; missing={sorted(expected - observed)}; "
            f"unexpected={sorted(observed - expected)}"
        )


def _record_object(
    record: collections.abc.Mapping[str, object],
    key: str,
) -> dict[str, object]:
    value = record.get(key)
    if not isinstance(value, dict):
        raise ValueError(f"{key} must be an object")
    return value


def _record_string(
    record: collections.abc.Mapping[str, object],
    key: str,
) -> str:
    value = record.get(key)
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
    ):
        raise ValueError(
            f"{key} must be a nonblank string without surrounding whitespace"
        )
    return value


def _record_optional_string(
    record: collections.abc.Mapping[str, object],
    key: str,
) -> str | None:
    value = record.get(key)
    if value is None:
        return None
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
    ):
        raise ValueError(
            f"{key} must be null or a nonblank string without surrounding "
            "whitespace"
        )
    return value


def _record_int(
    record: collections.abc.Mapping[str, object],
    key: str,
    *,
    minimum: int,
) -> int:
    value = record.get(key)
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or value < minimum
    ):
        raise ValueError(f"{key} must be an integer >= {minimum}")
    return value


def _positive_int(value: object, label: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{label} must be a positive integer")


def _nonnegative_int(value: object, label: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{label} must be a nonnegative integer")


def _canonical_json(value: object) -> bytes:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode()
    except (TypeError, ValueError) as error:
        raise ValueError("value is not canonical JSON") from error


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()
