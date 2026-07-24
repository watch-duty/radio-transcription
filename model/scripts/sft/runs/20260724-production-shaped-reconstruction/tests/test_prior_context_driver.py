"""Tests for runnable predicted-prior-context provider evaluation."""

# ruff: noqa: EM101, FBT001, INP001, PLC0415, TRY003

from __future__ import annotations

import asyncio
import dataclasses
import hashlib
import json
import pathlib
import sys
import types as stdlib_types

import grpc
import pytest

RUN_ROOT = pathlib.Path(__file__).resolve().parents[1]
MODEL_SRC = pathlib.Path(__file__).resolve().parents[5] / "src"
for path in (RUN_ROOT, MODEL_SRC):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from dataset_run import artifacts, cli, eval_views  # noqa: E402
from dataset_run import prior_context_driver as driver  # noqa: E402


def _primary_row(
    request_id: str,
    *,
    source_uri: str,
    offset: float,
    reference: str,
) -> dict[str, object]:
    start_frame = int(offset * 16_000)
    return {
        "audio_filepath": f"gs://materialized/{request_id}.flac",
        "context_episode_id": source_uri,
        "dataset": {"family": "bcfy_feeds", "name": "bcfy_feeds"},
        "duration": 1.0,
        "example_id": request_id,
        "offset": offset,
        "request_id": request_id,
        "segment_id": "0",
        "source_audio": {
            "audio_filepath": source_uri,
            "duration": 1.0,
            "end_frame": start_frame + 16_000,
            "offset": offset,
            "sample_rate": 16_000,
            "start_frame": start_frame,
        },
        "source_lineage": {"context_episode_id": source_uri},
        "split": "eval",
        "text": reference,
    }


def _contract() -> driver.ProductionRequestContract:
    prompt = "Production system prompt"
    return driver.ProductionRequestContract(
        system_prompt=prompt,
        system_prompt_sha256=hashlib.sha256(prompt.encode()).hexdigest(),
    )


def _media(
    request: eval_views.ScheduledRequest,
) -> driver.RequestMediaEvidence:
    return driver.RequestMediaEvidence(
        request_id=request.request_id,
        output_uri=request.audio_uri,
        source_uri=request.source_uri,
        source_generation=1,
        start_frame=request.source_ordinal * 16_000,
        end_frame=(request.source_ordinal + 1) * 16_000,
        duration_seconds="1",
        sample_rate=16_000,
        channels=1,
        container_format="FLAC",
        codec="FLAC",
        subtype="PCM_16",
        encoded_size_bytes=1_024,
        output_encoded_sha256="a" * 64,
        source_encoded_sha256="b" * 64,
        source_pcm_sha256="c" * 64,
        output_pcm_sha256="c" * 64,
        encoder_profile=driver.FEED_ENCODER_PROFILE,
        encoder_version="0.13.1",
        encoder_binary_sha256="f" * 64,
        encoder_python_artifact_sha256="1" * 64,
        encoder_cffi_artifact_sha256="2" * 64,
        encoder_embedded_libflac_version="1.4.3",
        encoder_command_sha256="d" * 64,
        encoder_contract_sha256=driver.FROZEN_ENCODER_CONTRACT_SHA256,
        encoder_input_pcm_format="s16le",
        encoder_production_image_uri=None,
        encoder_production_revision=None,
    )


def _raw(
    *,
    finish_reason: str | None = "STOP",
    text_parts: tuple[str, ...] = ("transcript",),
    candidate_present: bool = True,
    http_status: int | None = None,
    grpc_code: int | None = None,
    provider_status: str | None = None,
    provider_message: str | None = None,
    error_type: str | None = None,
) -> driver.RawProviderAttempt:
    return driver.RawProviderAttempt(
        logical_attempt=1,
        candidate_present=candidate_present,
        finish_reason=finish_reason,
        text_parts=text_parts,
        http_status=http_status,
        grpc_code=grpc_code,
        provider_status=provider_status,
        provider_message=provider_message,
        error_type=error_type,
    )


def _single_provider_request() -> driver.ProviderRequest:
    row = _primary_row(
        "one",
        source_uri="gs://raw/a.wav",
        offset=0,
        reference="REFERENCE",
    )
    schedule = eval_views.build_prior_context_schedule([row])
    builder = eval_views.PredictedPriorContextBuilder(
        schedule,
        checkpoint_id="checkpoint-5",
    )
    envelope = builder.open_request("one")
    return driver.build_provider_request(
        envelope,
        target=driver.CheckpointTarget(
            "checkpoint-5",
            "projects/p/locations/us/endpoints/checkpoint-5",
            "us",
        ),
        contract=_contract(),
        media=_media(envelope.request),
    )


@pytest.mark.parametrize(
    ("attempt", "category", "accepted", "retry", "empty_kind"),
    [
        (
            _raw(
                candidate_present=False,
                finish_reason=None,
                text_parts=(),
                http_status=400,
                provider_status="INVALID_ARGUMENT",
                provider_message=driver.DECODE_REJECTION_MESSAGE,
            ),
            "provider_input_decode_rejected",
            False,
            False,
            None,
        ),
        (
            _raw(
                candidate_present=False,
                finish_reason=None,
                text_parts=(),
                grpc_code=3,
                provider_status="INVALID_ARGUMENT",
                provider_message=driver.DECODE_REJECTION_MESSAGE,
            ),
            "provider_input_decode_rejected",
            False,
            False,
            None,
        ),
        (
            _raw(text_parts=()),
            "blank_stop",
            False,
            True,
            "zero_length",
        ),
        (
            _raw(text_parts=(" \n\t",)),
            "effective_empty_stop",
            False,
            True,
            "whitespace_only",
        ),
        (
            _raw(text_parts=("[UNINTELLIGIBLE]",)),
            "effective_empty_stop",
            False,
            False,
            "placeholder_only",
        ),
        (
            _raw(finish_reason=None),
            "finish_reason_none",
            False,
            True,
            None,
        ),
        (
            _raw(
                candidate_present=False,
                finish_reason=None,
                text_parts=(),
                error_type="no_candidate",
            ),
            "no_candidate",
            False,
            True,
            None,
        ),
        (
            _raw(
                candidate_present=False,
                finish_reason=None,
                text_parts=(),
                http_status=499,
            ),
            "http_499",
            False,
            False,
            None,
        ),
        (
            _raw(
                candidate_present=False,
                finish_reason=None,
                text_parts=(),
                http_status=504,
            ),
            "http_504",
            False,
            False,
            None,
        ),
        (
            _raw(finish_reason="MAX_TOKENS", text_parts=(" partial\ntext ",)),
            "max_tokens",
            True,
            False,
            None,
        ),
        (
            _raw(finish_reason="OTHER", text_parts=()),
            "other_finish_reason",
            False,
            True,
            None,
        ),
        (
            _raw(text_parts=(" accepted\n text ",)),
            "accepted_stop",
            True,
            False,
            None,
        ),
    ],
)
def test_outcome_taxonomy_has_frozen_precedence(
    attempt: driver.RawProviderAttempt,
    category: str,
    accepted: bool,
    retry: bool,
    empty_kind: str | None,
) -> None:
    classified = driver.classify_attempt(attempt)

    assert classified.category.value == category
    assert classified.accepted_effective_prediction is accepted
    assert classified.retain_for_context is accepted
    assert classified.retry_application_attempt is retry
    assert classified.effective_empty_kind == empty_kind


def test_similar_but_not_exact_decode_message_is_not_decode_rejection() -> None:
    attempt = _raw(
        candidate_present=False,
        finish_reason=None,
        text_parts=(),
        http_status=400,
        provider_status="INVALID_ARGUMENT",
        provider_message="Failed to decode audio or visual data",
    )

    assert (
        driver.classify_attempt(attempt).category
        == driver.OutcomeCategory.OTHER_ERROR
    )


def test_provider_request_uses_only_predictions_and_production_contract() -> (
    None
):
    rows = [
        _primary_row(
            "a-0",
            source_uri="gs://raw/a.wav",
            offset=0,
            reference="REFERENCE_SECRET_ZERO",
        ),
        _primary_row(
            "a-1",
            source_uri="gs://raw/a.wav",
            offset=1,
            reference="REFERENCE_SECRET_ONE",
        ),
    ]
    schedule = eval_views.build_prior_context_schedule(rows)
    builder = eval_views.PredictedPriorContextBuilder(
        schedule,
        checkpoint_id="checkpoint-5",
    )
    first = builder.open_request("a-0")
    builder.record_prediction(
        eval_views.PredictionOutcome(
            checkpoint_id="checkpoint-5",
            request_id="a-0",
            model_input_sha256=first.receipt["model_input_sha256"],
            prediction="predicted prior",
            accepted=True,
        )
    )
    second = builder.open_request("a-1")
    target = driver.CheckpointTarget(
        "checkpoint-5",
        "projects/p/locations/us/endpoints/checkpoint-5",
        "us",
    )

    request = driver.build_provider_request(
        second,
        target=target,
        contract=_contract(),
        media=_media(second.request),
    )
    arguments = request.semantic_sdk_arguments()

    assert arguments["model"] == target.model_resource
    assert arguments["config"]["system_instruction"] == (
        "Production system prompt"
    )
    assert arguments["config"]["thinking_config"] == {"thinking_budget": 0}
    assert len(arguments["config"]["safety_settings"]) == 5
    assert request.contents[0]["parts"][-1] == {
        "fileData": {
            "fileUri": "gs://materialized/a-1.flac",
            "mimeType": "audio/flac",
        }
    }
    assert "predicted prior" in request.context_prompt
    assert all(
        str(row["text"]) not in request.context_prompt for row in rows
    )


class _RaisingModels:
    def __init__(self, error: Exception) -> None:
        self.error = error

    async def generate_content(self, **kwargs):
        del kwargs
        raise self.error


def _raising_client(error: Exception) -> object:
    return stdlib_types.SimpleNamespace(
        aio=stdlib_types.SimpleNamespace(
            models=_RaisingModels(error),
        )
    )


class _GrpcLikeError(grpc.RpcError):
    def __init__(self, status: object, message: str) -> None:
        super().__init__(message)
        self._status = status
        self._message = message

    def code(self) -> object:
        return self._status

    def details(self) -> str:
        return self._message


def test_concrete_adapter_records_exact_decode_rejection() -> None:
    from google.genai import errors

    error = errors.APIError(
        400,
        {
            "error": {
                "code": 400,
                "message": driver.DECODE_REJECTION_MESSAGE,
                "status": "INVALID_ARGUMENT",
            }
        },
    )
    adapter = driver.GoogleGenAIProviderAdapter(
        _raising_client(error),
        location="us",
    )

    attempt = asyncio.run(
        adapter.invoke(_single_provider_request(), logical_attempt=1)
    )

    assert (
        driver.classify_attempt(attempt).category
        == driver.OutcomeCategory.PROVIDER_INPUT_DECODE_REJECTED
    )
    assert attempt.http_status == 400
    assert attempt.provider_status == "INVALID_ARGUMENT"
    assert attempt.provider_message == driver.DECODE_REJECTION_MESSAGE


def test_concrete_adapter_aborts_target_and_programming_failures() -> None:
    from google.genai import errors

    request = _single_provider_request()
    authentication = driver.GoogleGenAIProviderAdapter(
        _raising_client(
            errors.APIError(
                401,
                {
                    "error": {
                        "code": 401,
                        "message": "invalid credentials",
                        "status": "UNAUTHENTICATED",
                    }
                },
            )
        ),
        location="us",
    )
    programming = driver.GoogleGenAIProviderAdapter(
        _raising_client(AttributeError("broken parser")),
        location="us",
    )

    with pytest.raises(driver.ProviderTargetFatalError) as caught:
        asyncio.run(authentication.invoke(request, logical_attempt=1))
    assert caught.value.reason == "unauthenticated"
    assert caught.value.http_status == 401
    assert caught.value.grpc_code is None
    with pytest.raises(AttributeError, match="broken parser"):
        asyncio.run(programming.invoke(request, logical_attempt=1))


@pytest.mark.parametrize(
    ("status_name", "expected_code", "expected_reason"),
    [
        ("NOT_FOUND", 5, "target_not_found"),
        ("PERMISSION_DENIED", 7, "permission_denied"),
        ("FAILED_PRECONDITION", 9, "failed_precondition"),
        ("UNAUTHENTICATED", 16, "unauthenticated"),
    ],
)
def test_concrete_adapter_aborts_real_shaped_grpc_target_failures(
    status_name: str,
    expected_code: int,
    expected_reason: str,
) -> None:
    status = getattr(grpc.StatusCode, status_name)
    adapter = driver.GoogleGenAIProviderAdapter(
        _raising_client(_GrpcLikeError(status, "provider details")),
        location="us",
    )

    with pytest.raises(driver.ProviderTargetFatalError) as caught:
        asyncio.run(
            adapter.invoke(_single_provider_request(), logical_attempt=1)
        )

    assert caught.value.reason == expected_reason
    assert caught.value.http_status is None
    assert caught.value.grpc_code == expected_code


@pytest.mark.parametrize(
    "status_name",
    [
        "INVALID_ARGUMENT",
        "DEADLINE_EXCEEDED",
        "RESOURCE_EXHAUSTED",
        "ABORTED",
        "INTERNAL",
        "UNAVAILABLE",
        "DATA_LOSS",
    ],
)
def test_concrete_adapter_retains_non_target_grpc_failures(
    status_name: str,
) -> None:
    status = getattr(grpc.StatusCode, status_name)
    adapter = driver.GoogleGenAIProviderAdapter(
        _raising_client(_GrpcLikeError(status, "row-level provider failure")),
        location="us",
    )

    attempt = asyncio.run(
        adapter.invoke(_single_provider_request(), logical_attempt=1)
    )

    assert attempt.grpc_code == status.value[0]
    assert attempt.provider_status == status_name
    assert (
        driver.classify_attempt(attempt).category
        == driver.OutcomeCategory.OTHER_ERROR
    )
    assert driver.classify_attempt(attempt).retry_application_attempt is False


@pytest.mark.parametrize(
    ("message", "expected_category"),
    [
        (
            driver.DECODE_REJECTION_MESSAGE,
            driver.OutcomeCategory.PROVIDER_INPUT_DECODE_REJECTED,
        ),
        (
            "Failed to decode the supplied media payload.",
            driver.OutcomeCategory.OTHER_ERROR,
        ),
    ],
)
def test_real_shaped_grpc_invalid_argument_uses_exact_decode_fingerprint(
    message: str,
    expected_category: driver.OutcomeCategory,
) -> None:
    adapter = driver.GoogleGenAIProviderAdapter(
        _raising_client(
            _GrpcLikeError(grpc.StatusCode.INVALID_ARGUMENT, message)
        ),
        location="us",
    )

    attempt = asyncio.run(
        adapter.invoke(_single_provider_request(), logical_attempt=1)
    )

    assert attempt.grpc_code == 3
    assert attempt.provider_status == "INVALID_ARGUMENT"
    assert driver.classify_attempt(attempt).category == expected_category


def test_generic_http_400_is_retained_as_row_outcome() -> None:
    from google.genai import errors

    adapter = driver.GoogleGenAIProviderAdapter(
        _raising_client(
            errors.APIError(
                400,
                {
                    "error": {
                        "code": 400,
                        "message": "request-specific invalid argument",
                        "status": "INVALID_ARGUMENT",
                    }
                },
            )
        ),
        location="us",
    )

    attempt = asyncio.run(
        adapter.invoke(_single_provider_request(), logical_attempt=1)
    )
    classified = driver.classify_attempt(attempt)

    assert attempt.http_status == 400
    assert classified.category == driver.OutcomeCategory.OTHER_ERROR
    assert classified.retry_application_attempt is False


def test_generic_http_400_remains_in_terminal_wer_ledger(
    tmp_path: pathlib.Path,
) -> None:
    from google.genai import errors

    schedule = eval_views.build_prior_context_schedule(
        [
            _primary_row(
                "one",
                source_uri="gs://raw/a.wav",
                offset=0,
                reference="all of these words become deletions",
            )
        ]
    )
    target = driver.CheckpointTarget(
        "checkpoint-5",
        "projects/p/locations/us/endpoints/checkpoint-5",
        "us",
    )
    adapter = driver.GoogleGenAIProviderAdapter(
        _raising_client(
            errors.APIError(
                400,
                {
                    "error": {
                        "code": 400,
                        "message": "row-specific invalid request",
                        "status": "INVALID_ARGUMENT",
                    }
                },
            )
        ),
        location="us",
    )

    result = asyncio.run(
        driver.run_predicted_prior_context_checkpoints(
            schedule=schedule,
            targets=[target],
            adapters_by_checkpoint={"checkpoint-5": adapter},
            contract=_contract(),
            media_by_request={"one": _media(schedule.requests[0])},
            max_parallel_episodes_per_checkpoint=1,
            evidence_root=tmp_path,
        )
    )[0]

    assert len(result.attempts) == 1
    assert result.attempt_category_counts() == {"other_error": 1}
    assert result.terminal_category_counts() == {"other_error": 1}
    assert result.scoring_rows() == (
        {
            "checkpoint_id": "checkpoint-5",
            "outcome_category": "other_error",
            "pred_text": "",
            "request_id": "one",
        },
    )
    assert result.attempts[0].raw.provider_message == (
        "row-specific invalid request"
    )


@pytest.mark.parametrize(
    ("error_factory", "expected_kind", "expected_category"),
    [
        (
            lambda: __import__("httpx").ReadTimeout("SDK retries exhausted"),
            "timeout",
            driver.OutcomeCategory.TRANSPORT_TIMEOUT,
        ),
        (
            lambda: __import__("httpx").ConnectError("SDK retries exhausted"),
            "connect",
            driver.OutcomeCategory.TRANSPORT_CONNECT_ERROR,
        ),
    ],
)
def test_codeless_httpx_failures_are_terminal(
    error_factory,
    expected_kind: str,
    expected_category: driver.OutcomeCategory,
) -> None:
    adapter = driver.GoogleGenAIProviderAdapter(
        _raising_client(error_factory()),
        location="us",
    )

    attempt = asyncio.run(
        adapter.invoke(_single_provider_request(), logical_attempt=1)
    )
    classified = driver.classify_attempt(attempt)

    assert attempt.http_status is None
    assert attempt.grpc_code is None
    assert attempt.transport_kind == expected_kind
    assert classified.category == expected_category
    assert classified.retry_application_attempt is False


def test_candidate_safety_evidence_precedes_finish_none() -> None:
    rating = stdlib_types.SimpleNamespace(
        blocked=True,
        category=stdlib_types.SimpleNamespace(name="HARM_CATEGORY_DANGEROUS"),
        probability=stdlib_types.SimpleNamespace(name="HIGH"),
        severity=stdlib_types.SimpleNamespace(name="MEDIUM"),
        probability_score=0.9,
        severity_score=0.5,
    )
    candidate = stdlib_types.SimpleNamespace(
        finish_reason=None,
        finish_message="blocked",
        safety_ratings=[rating],
        content=stdlib_types.SimpleNamespace(parts=[]),
    )
    response = stdlib_types.SimpleNamespace(
        candidates=[candidate],
        response_id="response-1",
    )

    attempt = driver._provider_response_attempt(
        response,
        logical_attempt=1,
        latency_ms=12,
    )
    classified = driver.classify_attempt(attempt)

    assert classified.category == driver.OutcomeCategory.SAFETY_BLOCKED
    assert classified.retry_application_attempt is False
    assert attempt.policy_reasons == (
        "rating:HARM_CATEGORY_DANGEROUS:HIGH",
    )
    assert attempt.provider_evidence["safety_ratings"] == [
        {
            "blocked": True,
            "category": "HARM_CATEGORY_DANGEROUS",
            "probability": "HIGH",
            "probability_score": 0.9,
            "severity": "MEDIUM",
            "severity_score": 0.5,
        }
    ]


def test_prompt_safety_evidence_is_distinct_and_terminal() -> None:
    rating = stdlib_types.SimpleNamespace(
        blocked=True,
        category=stdlib_types.SimpleNamespace(name="HARM_CATEGORY_HARASSMENT"),
        probability=stdlib_types.SimpleNamespace(name="MEDIUM"),
        severity=None,
        probability_score=None,
        severity_score=None,
    )
    prompt_feedback = stdlib_types.SimpleNamespace(
        block_reason=stdlib_types.SimpleNamespace(name="SAFETY"),
        block_reason_message="prompt rejected",
        safety_ratings=[rating],
    )
    response = stdlib_types.SimpleNamespace(
        candidates=[],
        prompt_feedback=prompt_feedback,
        response_id="response-2",
    )

    attempt = driver._provider_response_attempt(
        response,
        logical_attempt=1,
        latency_ms=5,
    )
    classified = driver.classify_attempt(attempt)

    assert classified.category == driver.OutcomeCategory.SAFETY_BLOCKED
    assert classified.retry_application_attempt is False
    assert attempt.policy_reasons == (
        "prompt:SAFETY",
        "prompt_rating:HARM_CATEGORY_HARASSMENT:MEDIUM",
    )
    assert attempt.provider_evidence["prompt_block_reason"] == "SAFETY"
    assert (
        attempt.provider_evidence["prompt_block_reason_message"]
        == "prompt rejected"
    )


class _StaticAttemptAdapter:
    def __init__(self, attempt: driver.RawProviderAttempt) -> None:
        self.attempt = attempt
        self.call_count = 0

    async def invoke(
        self,
        request: driver.ProviderRequest,
        *,
        logical_attempt: int,
    ) -> driver.RawProviderAttempt:
        del request
        self.call_count += 1
        assert logical_attempt == self.attempt.logical_attempt
        return self.attempt


class _TaxonomyAdapter:
    def __init__(self) -> None:
        self.calls: list[tuple[str, int]] = []

    async def invoke(
        self,
        request: driver.ProviderRequest,
        *,
        logical_attempt: int,
    ) -> driver.RawProviderAttempt:
        self.calls.append((request.request_id, logical_attempt))
        if request.request_id == "no-candidate":
            return driver.RawProviderAttempt(
                logical_attempt=logical_attempt,
                candidate_present=False,
                error_type="no_candidate",
            )
        return driver.RawProviderAttempt(
            logical_attempt=logical_attempt,
            candidate_present=True,
            finish_reason="MALFORMED_FUNCTION_CALL",
        )


def test_no_candidate_and_other_finish_reason_have_distinct_summaries(
    tmp_path: pathlib.Path,
) -> None:
    schedule = eval_views.build_prior_context_schedule(
        [
            _primary_row(
                "no-candidate",
                source_uri="gs://raw/a.wav",
                offset=0,
                reference="REFERENCE_A",
            ),
            _primary_row(
                "other-finish",
                source_uri="gs://raw/b.wav",
                offset=0,
                reference="REFERENCE_B",
            ),
        ]
    )
    target = driver.CheckpointTarget(
        "checkpoint-5",
        "projects/p/locations/us/endpoints/checkpoint-5",
        "us",
    )
    adapter = _TaxonomyAdapter()

    result = asyncio.run(
        driver.run_predicted_prior_context_checkpoints(
            schedule=schedule,
            targets=[target],
            adapters_by_checkpoint={"checkpoint-5": adapter},
            contract=_contract(),
            media_by_request={
                request.request_id: _media(request)
                for request in schedule.requests
            },
            max_parallel_episodes_per_checkpoint=2,
            evidence_root=tmp_path,
        )
    )[0]

    assert result.attempt_category_counts() == {
        "no_candidate": 3,
        "other_finish_reason": 1,
    }
    assert result.terminal_category_counts() == {
        "no_candidate": 1,
        "other_finish_reason": 1,
    }
    requests = {row.request_id: row for row in result.requests}
    assert requests["no-candidate"].terminal_finish_reason is None
    assert requests["other-finish"].terminal_finish_reason == (
        "MALFORMED_FUNCTION_CALL"
    )
    assert [
        call for call in adapter.calls if call[0] == "no-candidate"
    ] == [
        ("no-candidate", 1),
        ("no-candidate", 2),
        ("no-candidate", 3),
    ]


def test_safety_evidence_is_durable_and_not_retried(
    tmp_path: pathlib.Path,
) -> None:
    schedule = eval_views.build_prior_context_schedule(
        [
            _primary_row(
                "one",
                source_uri="gs://raw/a.wav",
                offset=0,
                reference="REFERENCE",
            )
        ]
    )
    target = driver.CheckpointTarget(
        "checkpoint-5",
        "projects/p/locations/us/endpoints/checkpoint-5",
        "us",
    )
    attempt = driver.RawProviderAttempt(
        logical_attempt=1,
        candidate_present=True,
        finish_reason=None,
        policy_blocked=True,
        policy_reasons=("rating:HARM_CATEGORY_DANGEROUS:HIGH",),
        provider_evidence={
            "safety_ratings": [
                {
                    "blocked": True,
                    "category": "HARM_CATEGORY_DANGEROUS",
                    "probability": "HIGH",
                }
            ]
        },
    )
    adapter = _StaticAttemptAdapter(attempt)

    result = asyncio.run(
        driver.run_predicted_prior_context_checkpoints(
            schedule=schedule,
            targets=[target],
            adapters_by_checkpoint={"checkpoint-5": adapter},
            contract=_contract(),
            media_by_request={"one": _media(schedule.requests[0])},
            max_parallel_episodes_per_checkpoint=1,
            evidence_root=tmp_path,
        )
    )[0]

    assert adapter.call_count == 1
    assert result.terminal_category_counts() == {"safety_blocked": 1}
    attempt_path = (
        tmp_path
        / hashlib.sha256(b"checkpoint-5").hexdigest()
        / "attempts"
        / hashlib.sha256(b"one").hexdigest()
        / "01.json"
    )
    persisted = json.loads(attempt_path.read_text(encoding="utf-8"))
    assert persisted["category"] == "safety_blocked"
    assert persisted["provider"]["policy_blocked"] is True
    assert persisted["provider"]["policy_reasons"] == [
        "rating:HARM_CATEGORY_DANGEROUS:HIGH"
    ]
    assert persisted["provider"]["provider_evidence"]["safety_ratings"] == [
        {
            "blocked": True,
            "category": "HARM_CATEGORY_DANGEROUS",
            "probability": "HIGH",
        }
    ]


class _FakeAdapter:
    def __init__(self, checkpoint_id: str) -> None:
        self.checkpoint_id = checkpoint_id
        self.calls: list[tuple[str, int, str]] = []
        self.active_by_episode: set[str] = set()
        self.max_active = 0
        self.active = 0

    async def invoke(
        self,
        request: driver.ProviderRequest,
        *,
        logical_attempt: int,
    ) -> driver.RawProviderAttempt:
        assert request.target.checkpoint_id == self.checkpoint_id
        assert request.context_episode_id not in self.active_by_episode
        self.active_by_episode.add(request.context_episode_id)
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        self.calls.append(
            (request.request_id, logical_attempt, request.context_prompt)
        )
        await asyncio.sleep(0.001)
        self.active -= 1
        self.active_by_episode.remove(request.context_episode_id)
        if request.request_id == "a-0" and logical_attempt == 1:
            return driver.RawProviderAttempt(
                logical_attempt=logical_attempt,
                candidate_present=True,
                finish_reason="STOP",
                text_parts=(),
            )
        if request.request_id == "b-0":
            return driver.RawProviderAttempt(
                logical_attempt=logical_attempt,
                candidate_present=False,
                http_status=400,
                provider_status="INVALID_ARGUMENT",
                provider_message=driver.DECODE_REJECTION_MESSAGE,
            )
        finish = (
            "MAX_TOKENS" if request.request_id == "a-1" else "STOP"
        )
        return driver.RawProviderAttempt(
            logical_attempt=logical_attempt,
            candidate_present=True,
            finish_reason=finish,
            text_parts=(
                f"prediction {self.checkpoint_id} {request.request_id}",
            ),
        )


class _CrashAfterPrefixAdapter:
    def __init__(self, *, crash_request_id: str | None) -> None:
        self.crash_request_id = crash_request_id
        self.calls: list[tuple[str, str]] = []

    async def invoke(
        self,
        request: driver.ProviderRequest,
        *,
        logical_attempt: int,
    ) -> driver.RawProviderAttempt:
        del logical_attempt
        self.calls.append((request.request_id, request.context_prompt))
        if request.request_id == self.crash_request_id:
            raise RuntimeError("simulated process crash")
        return driver.RawProviderAttempt(
            logical_attempt=1,
            candidate_present=True,
            finish_reason="STOP",
            text_parts=(f"durable prediction {request.request_id}",),
        )


class _EpisodeFatalAdapter:
    def __init__(self) -> None:
        self.blocker_started = asyncio.Event()
        self.blocker_cleaned = asyncio.Event()
        self.calls: list[str] = []

    async def invoke(
        self,
        request: driver.ProviderRequest,
        *,
        logical_attempt: int,
    ) -> driver.RawProviderAttempt:
        del logical_attempt
        self.calls.append(request.request_id)
        if request.request_id == "a-0":
            await self.blocker_started.wait()
            raise driver.ProviderTargetFatalError(
                "unauthenticated",
                http_status=None,
                grpc_code=16,
            )
        if request.request_id == "b-0":
            self.blocker_started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                await asyncio.sleep(0)
                self.blocker_cleaned.set()
                raise
        raise AssertionError(
            "a sibling issued another provider call after target failure"
        )


class _CheckpointFatalAdapter:
    def __init__(
        self,
        *,
        fatal: bool,
        sibling_started: asyncio.Event,
        sibling_cleaned: asyncio.Event,
    ) -> None:
        self.fatal = fatal
        self.sibling_started = sibling_started
        self.sibling_cleaned = sibling_cleaned
        self.calls: list[str] = []

    async def invoke(
        self,
        request: driver.ProviderRequest,
        *,
        logical_attempt: int,
    ) -> driver.RawProviderAttempt:
        del logical_attempt
        self.calls.append(request.request_id)
        if self.fatal:
            await self.sibling_started.wait()
            raise driver.ProviderTargetFatalError(
                "permission_denied",
                http_status=None,
                grpc_code=7,
            )
        self.sibling_started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            await asyncio.sleep(0)
            self.sibling_cleaned.set()
            raise


def test_episode_failure_cancels_and_awaits_blocked_siblings(
    tmp_path: pathlib.Path,
) -> None:
    rows = [
        _primary_row(
            request_id,
            source_uri=source_uri,
            offset=offset,
            reference=f"REFERENCE_{request_id}",
        )
        for request_id, source_uri, offset in (
            ("a-0", "gs://raw/a.wav", 0),
            ("a-1", "gs://raw/a.wav", 1),
            ("b-0", "gs://raw/b.wav", 0),
            ("b-1", "gs://raw/b.wav", 1),
        )
    ]
    schedule = eval_views.build_prior_context_schedule(rows)
    target = driver.CheckpointTarget(
        "checkpoint-5",
        "projects/p/locations/us/endpoints/checkpoint-5",
        "us",
    )

    async def run() -> None:
        adapter = _EpisodeFatalAdapter()
        with pytest.raises(driver.ProviderTargetFatalError) as caught:
            await driver.run_predicted_prior_context_checkpoints(
                schedule=schedule,
                targets=[target],
                adapters_by_checkpoint={"checkpoint-5": adapter},
                contract=_contract(),
                media_by_request={
                    request.request_id: _media(request)
                    for request in schedule.requests
                },
                max_parallel_episodes_per_checkpoint=2,
                evidence_root=tmp_path,
            )
        assert caught.value.reason == "unauthenticated"
        assert adapter.calls == ["a-0", "b-0"]
        assert adapter.blocker_cleaned.is_set()

    asyncio.run(run())


def test_checkpoint_failure_cancels_and_awaits_blocked_checkpoint(
    tmp_path: pathlib.Path,
) -> None:
    schedule = eval_views.build_prior_context_schedule(
        [
            _primary_row(
                "a-0",
                source_uri="gs://raw/a.wav",
                offset=0,
                reference="REFERENCE_A0",
            ),
            _primary_row(
                "a-1",
                source_uri="gs://raw/a.wav",
                offset=1,
                reference="REFERENCE_A1",
            ),
        ]
    )
    targets = [
        driver.CheckpointTarget(
            checkpoint_id,
            f"projects/p/locations/us/endpoints/{checkpoint_id}",
            "us",
        )
        for checkpoint_id in ("checkpoint-5", "checkpoint-8")
    ]

    async def run() -> None:
        sibling_started = asyncio.Event()
        sibling_cleaned = asyncio.Event()
        adapters = {
            "checkpoint-5": _CheckpointFatalAdapter(
                fatal=True,
                sibling_started=sibling_started,
                sibling_cleaned=sibling_cleaned,
            ),
            "checkpoint-8": _CheckpointFatalAdapter(
                fatal=False,
                sibling_started=sibling_started,
                sibling_cleaned=sibling_cleaned,
            ),
        }
        with pytest.raises(driver.ProviderTargetFatalError) as caught:
            await driver.run_predicted_prior_context_checkpoints(
                schedule=schedule,
                targets=targets,
                adapters_by_checkpoint=adapters,
                contract=_contract(),
                media_by_request={
                    request.request_id: _media(request)
                    for request in schedule.requests
                },
                max_parallel_episodes_per_checkpoint=1,
                evidence_root=tmp_path,
            )
        assert caught.value.reason == "permission_denied"
        assert adapters["checkpoint-5"].calls == ["a-0"]
        assert adapters["checkpoint-8"].calls == ["a-0"]
        assert sibling_cleaned.is_set()

    asyncio.run(run())


def test_all_checkpoints_are_isolated_and_episodes_are_serial(
    tmp_path: pathlib.Path,
) -> None:
    rows = [
        _primary_row(
            "a-1",
            source_uri="gs://raw/a.wav",
            offset=1,
            reference="REFERENCE_A1",
        ),
        _primary_row(
            "b-0",
            source_uri="gs://raw/b.wav",
            offset=0,
            reference="REFERENCE_B0",
        ),
        _primary_row(
            "a-0",
            source_uri="gs://raw/a.wav",
            offset=0,
            reference="REFERENCE_A0",
        ),
    ]
    schedule = eval_views.build_prior_context_schedule(rows)
    targets = [
        driver.CheckpointTarget(
            checkpoint,
            f"projects/p/locations/us/endpoints/{checkpoint}",
            "us",
        )
        for checkpoint in ("checkpoint-5", "checkpoint-8")
    ]
    adapters = {
        target.checkpoint_id: _FakeAdapter(target.checkpoint_id)
        for target in targets
    }

    results = asyncio.run(
        driver.run_predicted_prior_context_checkpoints(
            schedule=schedule,
            targets=targets,
            adapters_by_checkpoint=adapters,
            contract=_contract(),
            media_by_request={
                request.request_id: _media(request)
                for request in schedule.requests
            },
            max_parallel_episodes_per_checkpoint=2,
            evidence_root=tmp_path,
        )
    )

    assert [result.target.checkpoint_id for result in results] == [
        "checkpoint-5",
        "checkpoint-8",
    ]
    for result in results:
        assert len(result.requests) == len(schedule.requests)
        assert len(result.scoring_rows()) == len(schedule.requests)
        by_id = {row.request_id: row for row in result.requests}
        assert by_id["a-0"].logical_attempt_count == 2
        assert by_id["a-0"].terminal_category.value == "accepted_stop"
        assert by_id["a-1"].terminal_category.value == "max_tokens"
        assert by_id["a-1"].partial is True
        assert by_id["a-1"].retained_for_future_context is True
        assert (
            by_id["b-0"].terminal_category.value
            == "provider_input_decode_rejected"
        )
        assert by_id["b-0"].prediction_for_scoring == ""
        assert by_id["b-0"].media.encoded_size_bytes == 1_024
        a1_prompt = next(
            prompt
            for request_id, _attempt, prompt in adapters[
                result.target.checkpoint_id
            ].calls
            if request_id == "a-1"
        )
        assert (
            f"prediction {result.target.checkpoint_id} a-0" in a1_prompt
        )
        other_checkpoint = (
            "checkpoint-8"
            if result.target.checkpoint_id == "checkpoint-5"
            else "checkpoint-5"
        )
        assert other_checkpoint not in a1_prompt
        assert adapters[result.target.checkpoint_id].max_active == 2


def test_restart_replays_completed_prefix_without_provider_recall(
    tmp_path: pathlib.Path,
) -> None:
    rows = [
        _primary_row(
            "a-0",
            source_uri="gs://raw/a.wav",
            offset=0,
            reference="REFERENCE_ZERO",
        ),
        _primary_row(
            "a-1",
            source_uri="gs://raw/a.wav",
            offset=1,
            reference="REFERENCE_ONE",
        ),
    ]
    schedule = eval_views.build_prior_context_schedule(rows)
    target = driver.CheckpointTarget(
        "checkpoint-5",
        "projects/p/locations/us/endpoints/checkpoint-5",
        "us",
    )
    media = {
        request.request_id: _media(request)
        for request in schedule.requests
    }
    first = _CrashAfterPrefixAdapter(crash_request_id="a-1")

    with pytest.raises(RuntimeError, match="simulated process crash"):
        asyncio.run(
            driver.run_predicted_prior_context_checkpoints(
                schedule=schedule,
                targets=[target],
                adapters_by_checkpoint={"checkpoint-5": first},
                contract=_contract(),
                media_by_request=media,
                max_parallel_episodes_per_checkpoint=1,
                evidence_root=tmp_path,
            )
        )
    assert [request_id for request_id, _prompt in first.calls] == [
        "a-0",
        "a-1",
    ]

    resumed = _CrashAfterPrefixAdapter(crash_request_id=None)
    result = asyncio.run(
        driver.run_predicted_prior_context_checkpoints(
            schedule=schedule,
            targets=[target],
            adapters_by_checkpoint={"checkpoint-5": resumed},
            contract=_contract(),
            media_by_request=media,
            max_parallel_episodes_per_checkpoint=1,
            evidence_root=tmp_path,
        )
    )[0]

    assert [request_id for request_id, _prompt in resumed.calls] == ["a-1"]
    assert "durable prediction a-0" in resumed.calls[0][1]
    assert [row.prediction_for_scoring for row in result.requests] == [
        "durable prediction a-0",
        "durable prediction a-1",
    ]


def test_decode_rejection_stays_in_attempt_and_terminal_ledgers(
    tmp_path: pathlib.Path,
) -> None:
    row = _primary_row(
        "decode",
        source_uri="gs://raw/a.wav",
        offset=0,
        reference="words that become deletion errors",
    )
    schedule = eval_views.build_prior_context_schedule([row])
    target = driver.CheckpointTarget(
        "checkpoint-5",
        "projects/p/locations/us/endpoints/checkpoint-5",
        "us",
    )
    adapter = _FakeAdapter("checkpoint-5")
    # This fake maps b-0 to decode rejection, so use the exact request ID here.
    row["request_id"] = "b-0"
    row["example_id"] = "b-0"
    row["audio_filepath"] = "gs://materialized/b-0.flac"
    schedule = eval_views.build_prior_context_schedule([row])

    result = asyncio.run(
        driver.run_predicted_prior_context_checkpoints(
            schedule=schedule,
            targets=[target],
            adapters_by_checkpoint={"checkpoint-5": adapter},
            contract=_contract(),
            media_by_request={
                schedule.requests[0].request_id: _media(
                    schedule.requests[0]
                )
            },
            max_parallel_episodes_per_checkpoint=1,
            evidence_root=tmp_path,
        )
    )[0]

    assert result.attempt_category_counts() == {
        "provider_input_decode_rejected": 1
    }
    assert result.terminal_category_counts() == {
        "provider_input_decode_rejected": 1
    }
    assert result.scoring_rows() == (
        {
            "checkpoint_id": "checkpoint-5",
            "outcome_category": "provider_input_decode_rejected",
            "pred_text": "",
            "request_id": "b-0",
        },
    )
    attempt = result.attempts[0].as_dict()
    assert attempt["provider"]["provider_message"] == (
        driver.DECODE_REJECTION_MESSAGE
    )
    assert attempt["provider"]["http_status"] == 400
    assert attempt["message_sha256"] == hashlib.sha256(
        driver.DECODE_REJECTION_MESSAGE.encode()
    ).hexdigest()


def test_checkpoint_result_is_create_only_and_byte_stable(
    tmp_path: pathlib.Path,
) -> None:
    row = _primary_row(
        "one",
        source_uri="gs://raw/a.wav",
        offset=0,
        reference="REFERENCE",
    )
    schedule = eval_views.build_prior_context_schedule([row])
    target = driver.CheckpointTarget(
        "checkpoint-5",
        "projects/p/locations/us/endpoints/checkpoint-5",
        "us",
    )
    adapter = _FakeAdapter("checkpoint-5")
    result = asyncio.run(
        driver.run_predicted_prior_context_checkpoints(
            schedule=schedule,
            targets=[target],
            adapters_by_checkpoint={"checkpoint-5": adapter},
            contract=_contract(),
            media_by_request={"one": _media(schedule.requests[0])},
            max_parallel_episodes_per_checkpoint=1,
            evidence_root=tmp_path,
        )
    )[0]

    first = driver.write_checkpoint_result_create_only(tmp_path, result)
    second = driver.write_checkpoint_result_create_only(tmp_path, result)

    assert all(not receipt.created for receipt in first)
    assert all(not receipt.created for receipt in second)
    checkpoint_root = tmp_path / hashlib.sha256(
        b"checkpoint-5"
    ).hexdigest()
    assert (checkpoint_root / "_SUCCESS.json").is_file()


def _staged_mapping_row(
    request: eval_views.ScheduledRequest,
) -> dict[str, object]:
    evidence = _media(request)
    return {
        "canonical_audio_uri": request.audio_uri,
        "encoder": {
            "binary_sha256": evidence.encoder_binary_sha256,
            "cffi_artifact_sha256": (
                evidence.encoder_cffi_artifact_sha256
            ),
            "command_sha256": evidence.encoder_command_sha256,
            "contract_sha256": evidence.encoder_contract_sha256,
            "embedded_libflac_version": (
                evidence.encoder_embedded_libflac_version
            ),
            "input_pcm_format": evidence.encoder_input_pcm_format,
            "python_artifact_sha256": (
                evidence.encoder_python_artifact_sha256
            ),
            "production_image_uri": evidence.encoder_production_image_uri,
            "production_revision": evidence.encoder_production_revision,
            "profile": evidence.encoder_profile,
            "version": evidence.encoder_version,
        },
        "local_output_path": f"/frozen/nonexistent/{request.request_id}.flac",
        "output": {
            "channels": evidence.channels,
            "codec": evidence.codec,
            "decoded_pcm_sha256": evidence.output_pcm_sha256,
            "duration_seconds_exact": evidence.duration_seconds,
            "encoded_size_bytes": evidence.encoded_size_bytes,
            "encoded_sha256": evidence.output_encoded_sha256,
            "format": evidence.container_format,
            "frame_count": evidence.end_frame - evidence.start_frame,
            "sample_rate": evidence.sample_rate,
            "subtype": evidence.subtype,
        },
        "request_id": request.request_id,
        "source": {
            "encoded_sha256": evidence.source_encoded_sha256,
            "end_frame": evidence.end_frame,
            "generation": evidence.source_generation,
            "pcm_sha256": evidence.source_pcm_sha256,
            "start_frame": evidence.start_frame,
            "uri": evidence.source_uri,
        },
    }


def test_staged_loader_trusts_frozen_media_without_opening_it(
    tmp_path: pathlib.Path,
) -> None:
    schedule = eval_views.build_prior_context_schedule(
        [
            _primary_row(
                "a-0",
                source_uri="gs://raw/a.wav",
                offset=0,
                reference="REFERENCE_A0",
            ),
            _primary_row(
                "a-1",
                source_uri="gs://raw/a.wav",
                offset=1,
                reference="REFERENCE_A1",
            ),
        ]
    )
    schedule_rows = [
        {
            "audio_uri": request.audio_uri,
            "context_episode_id": request.context_episode_id,
            "example_id": request.example_id,
            "manifest_index": request.manifest_index,
            "request_id": request.request_id,
            "segment_id": request.segment_id,
            "source_ordinal": request.source_ordinal,
            "source_uri": request.source_uri,
        }
        for request in schedule.requests
    ]
    artifacts.write_jsonl_create_only(
        tmp_path / driver.STAGED_PRIOR_CONTEXT_SCHEDULE,
        schedule_rows,
    )
    artifacts.write_json_create_only(
        tmp_path / driver.STAGED_PRIOR_CONTEXT_RECEIPT,
        schedule.receipt,
    )
    artifacts.write_jsonl_create_only(
        tmp_path / driver.STAGED_MATERIALIZATION_MAPPING,
        [_staged_mapping_row(request) for request in schedule.requests],
    )

    loaded, media_by_request = driver.load_staged_prior_context_inputs(
        tmp_path
    )

    assert loaded == schedule
    assert set(media_by_request) == {"a-0", "a-1"}
    assert media_by_request["a-0"].output_uri == (
        "gs://materialized/a-0.flac"
    )
    assert media_by_request["a-0"].encoder_contract_sha256 == (
        driver.FROZEN_ENCODER_CONTRACT_SHA256
    )
    assert media_by_request["a-0"].encoder_binary_sha256 == "f" * 64
    assert media_by_request["a-0"].encoder_python_artifact_sha256 == "1" * 64
    assert media_by_request["a-0"].encoder_cffi_artifact_sha256 == "2" * 64
    assert media_by_request[
        "a-0"
    ].encoder_embedded_libflac_version == "1.4.3"
    assert not pathlib.Path(
        "/frozen/nonexistent/a-0.flac"
    ).exists()


def test_media_evidence_enforces_frozen_encoder_profile_shapes() -> None:
    schedule = eval_views.build_prior_context_schedule(
        [
            _primary_row(
                "one",
                source_uri="gs://raw/a.wav",
                offset=0,
                reference="REFERENCE",
            )
        ]
    )
    feed = _media(schedule.requests[0])

    with pytest.raises(ValueError, match="lacks Python/CFFI/libFLAC"):
        dataclasses.replace(feed, encoder_cffi_artifact_sha256=None)
    with pytest.raises(ValueError, match="contract SHA-256"):
        dataclasses.replace(feed, encoder_contract_sha256="0" * 64)

    ffmpeg = dataclasses.replace(
        feed,
        encoder_profile=driver.PRESEGMENTED_ENCODER_PROFILE,
        encoder_python_artifact_sha256=None,
        encoder_cffi_artifact_sha256=None,
        encoder_embedded_libflac_version=None,
    )
    assert ffmpeg.encoder_binary_sha256 == "f" * 64


def test_prior_context_cli_loads_explicit_targets_and_closes_clients(
    tmp_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    schedule = eval_views.build_prior_context_schedule(
        [
            _primary_row(
                "one",
                source_uri="gs://raw/a.wav",
                offset=0,
                reference="REFERENCE",
            )
        ]
    )
    media_by_request = {"one": _media(schedule.requests[0])}
    config_path = tmp_path / "checkpoints.json"
    config_path.write_text(
        json.dumps(
            {
                "max_parallel_episodes_per_checkpoint": 7,
                "project": "p",
                "targets": [
                    {
                        "checkpoint_id": checkpoint_id,
                        "location": "us",
                        "model_resource": (
                            "projects/p/locations/us/endpoints/"
                            f"{checkpoint_id}"
                        ),
                    }
                    for checkpoint_id in ("checkpoint-5", "checkpoint-8")
                ],
            }
        ),
        encoding="utf-8",
    )
    clients: list[object] = []
    captured: dict[str, object] = {}

    class _FakeAio:
        def __init__(self) -> None:
            self.closed = False
            self.models = stdlib_types.SimpleNamespace()

        async def aclose(self) -> None:
            self.closed = True

    def create_client(*, project: str, location: str) -> object:
        assert project == "p"
        assert location == "us"
        client = stdlib_types.SimpleNamespace(aio=_FakeAio())
        clients.append(client)
        return client

    async def run_checkpoints(**kwargs):
        captured.update(kwargs)
        return tuple(
            driver.CheckpointRunResult(
                target=target,
                attempts=(),
                requests=(),
                context_receipt={},
                request_contract_receipt={},
            )
            for target in kwargs["targets"]
        )

    monkeypatch.setattr(
        driver,
        "load_staged_prior_context_inputs",
        lambda _root: (schedule, media_by_request),
    )
    monkeypatch.setattr(
        driver,
        "load_production_request_contract",
        lambda _root: _contract(),
    )
    monkeypatch.setattr(
        driver,
        "create_production_genai_client",
        create_client,
    )
    monkeypatch.setattr(
        driver,
        "run_predicted_prior_context_checkpoints",
        run_checkpoints,
    )

    status = cli.main(
        [
            "prior-context-eval",
            "--staged-root",
            str(tmp_path / "staged"),
            "--checkpoint-config",
            str(config_path),
            "--evidence-root",
            str(tmp_path / "resume-evidence"),
        ]
    )

    assert status == 0
    assert captured["schedule"] is schedule
    assert captured["media_by_request"] is media_by_request
    assert captured["max_parallel_episodes_per_checkpoint"] == 7
    assert captured["evidence_root"] == tmp_path / "resume-evidence"
    assert all(client.aio.closed for client in clients)
    output = json.loads(capsys.readouterr().out)
    assert output["status"] == "COMPLETE"
    assert [
        checkpoint["checkpoint_id"] for checkpoint in output["checkpoints"]
    ] == ["checkpoint-5", "checkpoint-8"]
