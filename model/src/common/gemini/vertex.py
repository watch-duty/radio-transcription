"""Vertex AI Gemini tuning job and batch inference submission (behind [vertex] extra).

Provides ``submit_tuning_job`` (submit-only, returns job.name), ``poll_tuning_job``
(re-fetch by name and poll to terminal, returns endpoint), and
``submit_batch_inference`` for Vertex AI Gemini SFT tuning and batch inference
via ``google-genai``.

``submit_tuning_job`` returns ``job.name`` immediately so the caller can persist
the server-side job resource before polling. That prevents job loss if the
process crashes between submission and the first poll.

``submit_batch_inference`` returns the destination GCS URI, not the raw
``BatchJobDestination`` object.

Important behavior:
- Validation dataset uses ``types.TuningValidationDataset``
- Hyperparameters (epoch_count, adapter_size, lr_multiplier) are required parameters,
  not hidden defaults.
- ``project`` and ``location`` are required keyword parameters — no silent defaults.
- Raises ``RuntimeError`` on non-success terminal state (library code, not CLI — no sys.exit)
- No GCP project or bucket constants are defined at module level.

``google-genai`` is deferred behind the ``[vertex]`` extra so
``import common.gemini.vertex`` succeeds with only the light core installed.
"""

import json
import logging
import re
import time
from collections.abc import Sequence
from typing import Any

from common.gemini.context import (
    ContextTurn,
    build_transcription_contents,
)

logger = logging.getLogger(__name__)

# Canonical Gemini transcription inference setup shared by ``gemini_sft.evaluate``
# and the ``gemini_transcribe_audio`` notebook. Keep these as plain stdlib
# dict/list values so prompt/config tests can import them without the [vertex] extra.
GEMINI_GENERATION_CONFIG = {"temperature": 0.0, "max_output_tokens": 512}

GEMINI_SAFETY_SETTINGS = [
    {"category": "HARM_CATEGORY_HATE_SPEECH", "threshold": "BLOCK_NONE"},
    {"category": "HARM_CATEGORY_SEXUALLY_EXPLICIT", "threshold": "BLOCK_NONE"},
    {"category": "HARM_CATEGORY_DANGEROUS_CONTENT", "threshold": "BLOCK_NONE"},
    {"category": "HARM_CATEGORY_HARASSMENT", "threshold": "BLOCK_NONE"},
]

_US_BATCH_MODEL_IDS = {"gemini-3.1-flash-lite"}


def build_request(
    audio_uri: str,
    *,
    system_prompt: str,
    user_prompt: str,
    history: Sequence[ContextTurn] | None = None,
    history_mode: str = "audio",
    generation_config: dict = GEMINI_GENERATION_CONFIG,
    safety_settings: list = GEMINI_SAFETY_SETTINGS,
) -> dict:
    """Build the canonical Vertex batch-inference request dict for one audio segment.

    Returns the plain-dict batch request consumed by ``submit_batch_inference`` and the
    Gemini batch API — the single shape used by both ``gemini_sft.evaluate`` and
    the ``gemini_transcribe_audio`` notebook. ``generation_config`` is ``.copy()``-ed so a
    caller mutating the result never touches the module-level default. Pure dict
    construction — does not require the ``[vertex]`` extra.

    Field keys are canonical camelCase JSON (``fileData``/``fileUri``/
    ``mimeType``/``systemInstruction``/``generationConfig``/
    ``safetySettings``). SFT JSONL, batch input JSONL, and batch output parsing
    all use the same shape.
    """
    contents = build_transcription_contents(
        audio_uri=audio_uri,
        user_prompt=user_prompt,
        history=history,
        history_mode=history_mode,
    )
    return {
        "request": {
            "contents": contents,
            "systemInstruction": {
                "role": "system",
                "parts": [{"text": system_prompt}],
            },
            "generationConfig": generation_config.copy(),
            "safetySettings": list(safety_settings),
        }
    }


def parse_batch_output(text: str) -> dict[str, str]:
    """Parse Vertex Gemini batch output JSONL into ``{audio_uri: prediction}``.

    Vertex echoes the original request in batch output. This parser accepts the
    canonical camelCase request shape emitted by ``build_request``. Error/status
    rows, malformed JSONL rows, and output rows without an identifiable audio
    URI are skipped.
    """
    result: dict[str, str] = {}
    for line in text.splitlines():
        if not line.strip():
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            logger.warning("Skipping malformed batch output JSONL row")
            continue
        if not isinstance(obj, dict):
            continue
        if obj.get("status") or not isinstance(obj.get("request"), dict):
            continue
        uri = _extract_request_audio_uri(obj["request"])
        if not uri:
            continue
        result[uri] = _extract_prediction_text(obj.get("response"))
    return result


def _extract_request_audio_uri(request: dict[str, Any]) -> str | None:
    contents = request.get("contents", [])
    if not isinstance(contents, list) or not contents:
        return None
    audio_uri: str | None = None
    for content in contents:
        if not isinstance(content, dict):
            continue
        parts = content.get("parts", [])
        if not isinstance(parts, list):
            continue
        for part in parts:
            if not isinstance(part, dict):
                continue
            file_data = part.get("fileData")
            if not isinstance(file_data, dict):
                continue
            candidate = file_data.get("fileUri")
            if isinstance(candidate, str) and candidate:
                audio_uri = candidate
    return audio_uri


def _extract_prediction_text(response: Any) -> str:
    if not isinstance(response, dict):
        return ""
    first_candidate = _first_dict(response.get("candidates"))
    content = first_candidate.get("content", {}) if first_candidate else {}
    if not isinstance(content, dict):
        return ""
    parts = content.get("parts", [])
    if not isinstance(parts, list):
        return ""
    for part in parts:
        if isinstance(part, dict) and isinstance(part.get("text"), str):
            return part["text"]
    return ""


def _first_dict(value: Any) -> dict[str, Any] | None:
    if isinstance(value, list) and value and isinstance(value[0], dict):
        return value[0]
    return None


try:
    from google import genai
    from google.genai import types
except ImportError as _e:
    _VERTEX_MISSING = _e
    # Keep these names bound even without the optional SDK. That lets unit tests
    # patch ``common.gemini.vertex.genai`` and lets lightweight imports fail only
    # when a caller actually uses Vertex functionality.
    genai = None
    types = None
else:
    _VERTEX_MISSING = None

_ADAPTER_ENUM = {
    "ONE": "ADAPTER_SIZE_ONE",
    "TWO": "ADAPTER_SIZE_TWO",
    "FOUR": "ADAPTER_SIZE_FOUR",
    "EIGHT": "ADAPTER_SIZE_EIGHT",
    "SIXTEEN": "ADAPTER_SIZE_SIXTEEN",
}

# Vertex returns both short enum names and full JOB_STATE_* names across job
# resources/SDK versions, so pollers normalize by accepting both spellings.
_TERMINAL_STATES = {
    "JOB_STATE_SUCCEEDED",
    "JOB_STATE_FAILED",
    "JOB_STATE_CANCELLED",
    "SUCCEEDED",
    "FAILED",
    "CANCELLED",
}

_BATCH_TERMINAL_STATES = {
    "JOB_STATE_SUCCEEDED",
    "SUCCEEDED",
    "JOB_STATE_PARTIALLY_SUCCEEDED",
    "PARTIALLY_SUCCEEDED",
    "JOB_STATE_FAILED",
    "FAILED",
    "JOB_STATE_CANCELLED",
    "CANCELLED",
}

# PARTIALLY_SUCCEEDED still writes usable prediction JSONL. Missing predictions
# are handled by the scorer as full deletions, which is more useful than throwing
# away the completed rows.
_BATCH_SUCCESS_STATES = {
    "JOB_STATE_SUCCEEDED",
    "SUCCEEDED",
    "JOB_STATE_PARTIALLY_SUCCEEDED",
    "PARTIALLY_SUCCEEDED",
}

_TUNING_SUCCESS_STATES = {"JOB_STATE_SUCCEEDED", "SUCCEEDED"}
_POLL_GET_RETRY_LIMIT = 3
_POLL_GET_RETRY_SLEEP_SECONDS = 5.0
_RESOURCE_LOCATION_RE = re.compile(r"/locations/([^/]+)/")


def _require_vertex() -> None:
    """Raise a clear error if the [vertex] extra is not installed."""
    if _VERTEX_MISSING:
        msg = (
            "vertex requires the [vertex] extra: "
            "pip install 'radio-transcription-model[vertex]'"
        )
        raise ImportError(msg) from _VERTEX_MISSING


def get_tuning_job_status(
    name: str, project: str, location: str
) -> tuple[str, str | None]:
    """Return the current tuning job state and endpoint if one is exposed."""
    _require_vertex()
    client = genai.Client(vertexai=True, project=project, location=location)
    cur = client.tunings.get(name=name)
    state = getattr(cur.state, "name", str(cur.state))
    tuned = getattr(cur, "tuned_model", None)
    endpoint = getattr(tuned, "endpoint", None) if tuned else None
    return state, endpoint


def submit_tuning_job(
    *,
    train_uri: str,
    display_name: str,
    project: str,
    location: str,
    base_model: str = "gemini-3.1-flash-lite",
    val_uri: str | None = None,
    epoch_count: int = 5,
    adapter_size: str = "ONE",
    lr_multiplier: float = 1.0,
) -> str:
    """Submit a Vertex AI Gemini SFT tuning job and return job.name immediately.

    Returns job.name without polling so the caller can persist it to
    config.json before entering the poll loop. This prevents losing the job
    reference if the process crashes between submission and first poll.

    Args:
        train_uri: GCS URI for training JSONL (gs://...).
        display_name: Display name for the tuned model resource (encode round-id here).
        project: GCP project ID (required — no silent default).
        location: GCP region (use 'us-central1' for evaluation feature availability).
        base_model: Foundation model name. Defaults to 'gemini-3.1-flash-lite'.
        val_uri: Optional GCS URI for validation JSONL. Wires eval_total_loss.
        epoch_count: Number of training epochs (1-100). SDK default is 5.
        adapter_size: Adapter size key — one of ONE, TWO, FOUR, EIGHT, SIXTEEN.
        lr_multiplier: Learning-rate multiplier (0.001-10.0). Defaults to 1.0.

    Returns:
        job.name — the stable Vertex AI resource name for the tuning job.
        Example: "projects/123/locations/us-central1/tuningJobs/456"

    Raises:
        ImportError: If the [vertex] extra is not installed.
        ValueError: If adapter_size is not in _ADAPTER_ENUM.
    """
    _require_vertex()
    if adapter_size not in _ADAPTER_ENUM:
        msg = f"adapter_size must be one of {sorted(_ADAPTER_ENUM)}; got {adapter_size!r}"
        raise ValueError(msg)
    client = genai.Client(vertexai=True, project=project, location=location)

    cfg_kwargs: dict[str, Any] = {
        "tuned_model_display_name": display_name,
        "epoch_count": epoch_count,
        "adapter_size": _ADAPTER_ENUM[adapter_size],
        "learning_rate_multiplier": lr_multiplier,
    }
    if val_uri:
        cfg_kwargs["validation_dataset"] = types.TuningValidationDataset(
            gcs_uri=val_uri
        )

    job = client.tunings.tune(
        base_model=base_model,
        training_dataset=types.TuningDataset(gcs_uri=train_uri),
        config=types.CreateTuningJobConfig(**cfg_kwargs),
    )
    logger.info(f"Submitted tuning job: {job.name}")
    return job.name


def poll_tuning_job(
    name: str,
    project: str,
    location: str,
    poll_interval: int = 300,
    timeout_hours: float = 24.0,
) -> str:
    """Re-fetch a Vertex AI tuning job by name and poll until terminal state.

    Tuning jobs are server-side, stable by resource name, and re-fetchable
    from a fresh client process. Used by the resume state machine.

    Args:
        name: Vertex AI tuning job resource name (from submit_tuning_job return value).
        project: GCP project ID.
        location: GCP region.
        poll_interval: Seconds between state-poll requests (default 300, or
            5 minutes).
        timeout_hours: Max wall-clock hours to poll before raising TimeoutError
            (default 24) -- guards against an indefinite hang on an API/network stall.

    Returns:
        Tuned model endpoint string (cur.tuned_model.endpoint).

    Raises:
        ImportError: If the [vertex] extra is not installed.
        RuntimeError: If the tuning job ends in a non-success terminal state.
        TimeoutError: If no terminal state is reached within timeout_hours.
    """
    _require_vertex()
    client = genai.Client(vertexai=True, project=project, location=location)
    last_state: str | None = None
    state: str = ""
    deadline = time.monotonic() + timeout_hours * 3600
    consecutive_get_errors = 0
    while True:
        try:
            cur = client.tunings.get(name=name)
        except Exception as e:
            consecutive_get_errors += 1
            if time.monotonic() >= deadline:
                msg = (
                    f"Tuning job {name} could not be fetched before the "
                    f"{timeout_hours}h timeout elapsed (last state: {state or 'unknown'}). "
                    "It may still be running on Vertex; re-run tune to resume polling by job name."
                )
                raise TimeoutError(msg) from e
            if consecutive_get_errors > _POLL_GET_RETRY_LIMIT:
                msg = (
                    f"Could not fetch tuning job {name} after "
                    f"{_POLL_GET_RETRY_LIMIT} retries; re-run tune to resume polling."
                )
                raise RuntimeError(msg) from e
            logger.warning(
                f"Transient error fetching tuning job {name}; retrying "
                f"({consecutive_get_errors}/{_POLL_GET_RETRY_LIMIT}): {e}"
            )
            time.sleep(_POLL_GET_RETRY_SLEEP_SECONDS)
            continue
        consecutive_get_errors = 0
        state = getattr(cur.state, "name", str(cur.state))
        if state != last_state:
            logger.info(f"[{time.strftime('%H:%M:%S')}] tuning state: {state}")
            last_state = state
        if state in _TERMINAL_STATES:
            break
        if time.monotonic() >= deadline:
            msg = (
                f"Tuning job {name} did not reach a terminal state within "
                f"{timeout_hours}h (last state: {state}). It may still be running "
                "on Vertex; re-run tune to resume polling by job name."
            )
            raise TimeoutError(msg)
        time.sleep(poll_interval)

    if state not in _TUNING_SUCCESS_STATES:
        msg = f"Tuning job ended in non-success state: {state}"
        raise RuntimeError(msg)

    tuned = getattr(cur, "tuned_model", None)
    endpoint = getattr(tuned, "endpoint", None) if tuned else None
    if not endpoint:
        msg = (
            f"Tuning job {name} succeeded but returned no tuned_model.endpoint"
        )
        raise RuntimeError(msg)
    logger.info(f"Tuned model endpoint: {endpoint}")
    return endpoint


def submit_batch_inference(
    *,
    input_uri: str,
    output_uri: str,
    model: str,
    project: str,
    location: str,
    poll_interval: int = 300,
    timeout_hours: float = 24.0,
) -> str:
    """Submit a Vertex AI batch inference job and poll until a terminal state.

    Args:
        input_uri: GCS URI for the batch input file (gs://...).
        output_uri: GCS URI for the batch output destination (gs://...).
        model: Model resource name or base model ID to use for inference.
        project: GCP project ID (required — no silent default).
        location: GCP region for the batch job.
        poll_interval: Seconds between state-poll requests (default 300, or
            5 minutes).
        timeout_hours: Max wall-clock hours to poll before raising TimeoutError
            (default 24) -- guards against an indefinite hang on an API/network stall.

    Returns:
        Resolved batch output location (GCS URI string).
        The resolved GCS URI from the batch job destination.

    Raises:
        ImportError: If the ``[vertex]`` extra is not installed.
        RuntimeError: If the batch job ends in FAILED or CANCELLED state.
        TimeoutError: If no terminal state is reached within timeout_hours.
    """
    _require_vertex()
    batch_location = _batch_location(model, location)
    if batch_location != location:
        logger.info(
            "Using model resource location %s for batch inference instead of %s",
            batch_location,
            location,
        )
    client = genai.Client(
        vertexai=True, project=project, location=batch_location
    )

    batch_job = client.batches.create(
        model=model,
        src=input_uri,
        config={"dest": output_uri},
    )
    logger.info(f"Submitted batch inference job: {batch_job.name}")

    last_state: str | None = None
    state: str = ""
    deadline = time.monotonic() + timeout_hours * 3600
    consecutive_get_errors = 0
    while True:
        try:
            cur = client.batches.get(name=batch_job.name)
        except Exception as e:
            consecutive_get_errors += 1
            if time.monotonic() >= deadline:
                msg = (
                    f"Batch job {batch_job.name} could not be fetched before "
                    f"the {timeout_hours}h timeout elapsed (last state: {state or 'unknown'})."
                )
                raise TimeoutError(msg) from e
            if consecutive_get_errors > _POLL_GET_RETRY_LIMIT:
                msg = (
                    f"Could not fetch batch job {batch_job.name} after "
                    f"{_POLL_GET_RETRY_LIMIT} retries."
                )
                raise RuntimeError(msg) from e
            logger.warning(
                f"Transient error fetching batch job {batch_job.name}; retrying "
                f"({consecutive_get_errors}/{_POLL_GET_RETRY_LIMIT}): {e}"
            )
            time.sleep(_POLL_GET_RETRY_SLEEP_SECONDS)
            continue
        consecutive_get_errors = 0
        state = getattr(cur.state, "name", str(cur.state))
        if state != last_state:
            logger.info(f"[{time.strftime('%H:%M:%S')}] batch state: {state}")
            last_state = state
        if state in _BATCH_TERMINAL_STATES:
            break
        if time.monotonic() >= deadline:
            msg = (
                f"Batch job {batch_job.name} did not reach a terminal state "
                f"within {timeout_hours}h (last state: {state})."
            )
            raise TimeoutError(msg)
        time.sleep(poll_interval)

    if state not in _BATCH_SUCCESS_STATES:
        msg = f"Batch inference job ended in non-success state: {state}"
        raise RuntimeError(msg)

    dest_uri = getattr(cur.dest, "gcs_uri", None) if cur.dest else None
    if dest_uri:
        output_location: str = dest_uri
    else:
        # The requested output URI is still where Vertex was asked to write.
        # Falling back preserves the path operators need for manual inspection.
        logger.warning(
            f"Batch job returned no destination GCS URI; falling back to requested "
            f"output URI: {output_uri}"
        )
        output_location = output_uri
    logger.info(f"Batch output location: {output_location}")
    return output_location


def resource_location(
    resource_name: str, default: str | None = None
) -> str | None:
    """Extract a Vertex resource location from a full resource name."""
    match = _RESOURCE_LOCATION_RE.search(resource_name)
    return match.group(1) if match else default


def _batch_location(model: str, location: str) -> str:
    """Return the Vertex location to use for batch inference."""
    model_location = resource_location(model)
    if model_location:
        return model_location
    if model.rsplit("/", maxsplit=1)[-1] in _US_BATCH_MODEL_IDS:
        return "us"
    return location
