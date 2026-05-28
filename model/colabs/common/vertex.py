"""Vertex AI Gemini tuning job and batch inference submission (behind [vertex] extra).

Provides ``submit_tuning_job`` (submit-only, returns job.name), ``poll_tuning_job``
(re-fetch by name and poll to terminal, returns endpoint), and
``submit_batch_inference`` for Vertex AI Gemini SFT tuning and batch inference
via ``google-genai`` 2.x.

PR3 refactors ``submit_tuning_job`` to return ``job.name`` immediately (D-10):
the caller persists ``job.name`` to ``config.json`` before calling
``poll_tuning_job``, preventing job loss on crash between submission and first
poll.

PR3 also fixes ``submit_batch_inference``: ``cur.dest`` is a
``BatchJobDestination`` object — the correct expression is
``cur.dest.gcs_uri if cur.dest else output_uri``.

Key corrections from the frozen autoresearch-gemini-sft source (submit_sft.py):
- Validation dataset uses ``types.TuningValidationDataset``
- Hyperparameters (epoch_count, adapter_size, lr_multiplier) are required parameters,
  not hardcoded defaults from the frozen repo (D-08)
- ``project`` and ``location`` are required keyword parameters — no silent defaults (Pitfall 4)
- Raises ``RuntimeError`` on non-success terminal state (library code, not CLI — no sys.exit)
- No GCP project or bucket constants defined at module level (T-01-05)

``google-genai`` is deferred behind the ``[vertex]`` extra so ``import common.vertex``
succeeds with only the light core installed.
"""

import logging
import time
from typing import Any

logger = logging.getLogger(__name__)

# Canonical Gemini transcription inference setup — shared by the SFT pipeline ``_eval``
# stage and the ``gemini_transcribe_audio`` eval notebook (single source, prevents drift).
# Plain stdlib dicts/lists only, defined BEFORE the google-genai guard so they import
# without the [vertex] extra.
GEMINI_GENERATION_CONFIG = {"temperature": 0.0, "max_output_tokens": 512}

GEMINI_SAFETY_SETTINGS = [
    {"category": "HARM_CATEGORY_HATE_SPEECH", "threshold": "BLOCK_NONE"},
    {"category": "HARM_CATEGORY_SEXUALLY_EXPLICIT", "threshold": "BLOCK_NONE"},
    {"category": "HARM_CATEGORY_DANGEROUS_CONTENT", "threshold": "BLOCK_NONE"},
    {"category": "HARM_CATEGORY_HARASSMENT", "threshold": "BLOCK_NONE"},
]


def build_request(
    audio_uri: str,
    *,
    system_prompt: str,
    user_prompt: str,
    generation_config: dict = GEMINI_GENERATION_CONFIG,
    safety_settings: list = GEMINI_SAFETY_SETTINGS,
) -> dict:
    """Build the canonical Vertex batch-inference request dict for one audio segment.

    Returns the plain-dict batch request consumed by ``submit_batch_inference`` and the
    Gemini batch API — the single shape used by both the SFT pipeline ``_eval`` stage and
    the ``gemini_transcribe_audio`` notebook. ``generation_config`` is ``.copy()``-ed so a
    caller mutating the result never touches the module-level default. Pure dict
    construction — does not require the ``[vertex]`` extra.

    Field keys are snake_case (``file_data``/``file_uri``/``mime_type``/
    ``system_instruction``/``generation_config``) on purpose: the google-genai batch
    endpoint (``client.batches.create``) accepts the proto field names, and this matches
    the proven ``gemini_transcribe_audio`` notebook. NOTE: Vertex echoes the request back
    in camelCase in the batch OUTPUT, so any output parser must read both casings.
    """
    return {
        "request": {
            "contents": [
                {
                    "role": "user",
                    "parts": [
                        # snake_case keys are intentional — see the docstring note.
                        {
                            "file_data": {
                                "file_uri": audio_uri,
                                "mime_type": "audio/flac",
                            }
                        },
                        {"text": user_prompt},
                    ],
                }
            ],
            "system_instruction": {
                "role": "system",
                "parts": [{"text": system_prompt.strip()}],
            },
            "generation_config": generation_config.copy(),
            "safety_settings": list(safety_settings),
        }
    }


try:
    from google import genai
    from google.genai import types
except ImportError as _e:
    _VERTEX_MISSING = _e
    genai = None
    types = None
else:
    _VERTEX_MISSING = None

_ADAPTER_ENUM = {
    "ONE": "ADAPTER_SIZE_ONE",
    "FOUR": "ADAPTER_SIZE_FOUR",
    "EIGHT": "ADAPTER_SIZE_EIGHT",
    "SIXTEEN": "ADAPTER_SIZE_SIXTEEN",
}

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

_BATCH_SUCCESS_STATES = {
    "JOB_STATE_SUCCEEDED",
    "SUCCEEDED",
    "JOB_STATE_PARTIALLY_SUCCEEDED",
    "PARTIALLY_SUCCEEDED",
}

_TUNING_SUCCESS_STATES = {"JOB_STATE_SUCCEEDED", "SUCCEEDED"}
_POLL_GET_RETRY_LIMIT = 3
_POLL_GET_RETRY_SLEEP_SECONDS = 5.0


def _require_vertex() -> None:
    """Raise a clear error if the [vertex] extra is not installed."""
    if _VERTEX_MISSING:
        raise ImportError(
            "vertex requires the [vertex] extra: pip install 'common[vertex]'"
        ) from _VERTEX_MISSING


def submit_tuning_job(
    *,
    train_uri: str,
    display_name: str,
    project: str,
    location: str,
    base_model: str = "gemini-3.1-flash-lite",
    val_uri: "str | None" = None,
    epoch_count: int = 5,
    adapter_size: str = "ONE",
    lr_multiplier: float = 1.0,
) -> str:
    """Submit a Vertex AI Gemini SFT tuning job and return job.name immediately.

    D-10: Returns job.name without polling so the caller can persist it to
    config.json before entering the poll loop. This prevents losing the job
    reference if the process crashes between submission and first poll.

    Args:
        train_uri: GCS URI for training JSONL (gs://...).
        display_name: Display name for the tuned model resource (encode round-id here).
        project: GCP project ID (required — no silent default).
        location: GCP region (use 'us-central1' for evaluation feature availability).
        base_model: Base model name. Defaults to 'gemini-3.1-flash-lite'.
        val_uri: Optional GCS URI for validation JSONL. Wires eval_total_loss (D-12/PIPE-09).
        epoch_count: Number of training epochs (1-100). SDK default is 5.
        adapter_size: Adapter size key — one of ONE, FOUR, EIGHT, SIXTEEN.
        lr_multiplier: Learning-rate multiplier (0.001-10.0). Defaults to 1.0.

    Returns:
        job.name — the stable Vertex AI resource name for the tuning job.
        Example: "projects/123/locations/us-central1/tuningJobs/456"

    Raises:
        ImportError: If the [vertex] extra is not installed.
        KeyError: If adapter_size is not in _ADAPTER_ENUM.
    """
    _require_vertex()
    if adapter_size not in _ADAPTER_ENUM:
        raise ValueError(
            f"adapter_size must be one of {sorted(_ADAPTER_ENUM)}; got {adapter_size!r}"
        )
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
    return job.name  # RETURN IMMEDIATELY — caller persists before polling


def poll_tuning_job(
    name: str,
    project: str,
    location: str,
    poll_interval: int = 30,
    timeout_hours: float = 24.0,
) -> str:
    """Re-fetch a Vertex AI tuning job by name and poll until terminal state.

    D-09: Tuning jobs are server-side, stable by resource name, re-fetchable
    from a fresh client process. Used by the resume state machine (D-08/D-11).

    Args:
        name: Vertex AI tuning job resource name (from submit_tuning_job return value).
        project: GCP project ID.
        location: GCP region.
        poll_interval: Seconds between state-poll requests.
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
    last_state: "str | None" = None
    state: str = ""
    deadline = time.monotonic() + timeout_hours * 3600
    consecutive_get_errors = 0
    while True:
        try:
            cur = client.tunings.get(name=name)
        except Exception as e:
            consecutive_get_errors += 1
            if time.monotonic() >= deadline:
                raise TimeoutError(
                    f"Tuning job {name} could not be fetched before the "
                    f"{timeout_hours}h timeout elapsed (last state: {state or 'unknown'}). "
                    "It may still be running on Vertex; re-run tune to resume polling by job name."
                ) from e
            if consecutive_get_errors > _POLL_GET_RETRY_LIMIT:
                raise RuntimeError(
                    f"Could not fetch tuning job {name} after "
                    f"{_POLL_GET_RETRY_LIMIT} retries; re-run tune to resume polling."
                ) from e
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
            raise TimeoutError(
                f"Tuning job {name} did not reach a terminal state within "
                f"{timeout_hours}h (last state: {state}). It may still be running "
                "on Vertex; re-run tune to resume polling by job name."
            )
        time.sleep(poll_interval)

    if state not in _TUNING_SUCCESS_STATES:
        raise RuntimeError(f"Tuning job ended in non-success state: {state}")

    endpoint: str = cur.tuned_model.endpoint
    logger.info(f"Tuned model endpoint: {endpoint}")
    return endpoint


def submit_batch_inference(
    *,
    input_uri: str,
    output_uri: str,
    model: str,
    project: str,
    location: str,
    poll_interval: int = 60,
    timeout_hours: float = 24.0,
) -> str:
    """Submit a Vertex AI batch inference job and poll until a terminal state.

    Args:
        input_uri: GCS URI for the batch input file (gs://...).
        output_uri: GCS URI for the batch output destination (gs://...).
        model: Model resource name or base model ID to use for inference.
        project: GCP project ID (required — no silent default).
        location: GCP region for the batch job.
        poll_interval: Seconds between state-poll requests.
        timeout_hours: Max wall-clock hours to poll before raising TimeoutError
            (default 24) -- guards against an indefinite hang on an API/network stall.

    Returns:
        Resolved batch output location (GCS URI string).
        PR3 fix: cur.dest is a BatchJobDestination object — use cur.dest.gcs_uri.

    Raises:
        ImportError: If the ``[vertex]`` extra is not installed.
        RuntimeError: If the batch job ends in FAILED or CANCELLED state.
        TimeoutError: If no terminal state is reached within timeout_hours.
    """
    _require_vertex()
    client = genai.Client(vertexai=True, project=project, location=location)

    batch_job = client.batches.create(
        model=model,
        src=input_uri,
        config={"dest": output_uri},
    )
    logger.info(f"Submitted batch inference job: {batch_job.name}")

    last_state: "str | None" = None
    state: str = ""
    deadline = time.monotonic() + timeout_hours * 3600
    consecutive_get_errors = 0
    while True:
        try:
            cur = client.batches.get(name=batch_job.name)
        except Exception as e:
            consecutive_get_errors += 1
            if time.monotonic() >= deadline:
                raise TimeoutError(
                    f"Batch job {batch_job.name} could not be fetched before "
                    f"the {timeout_hours}h timeout elapsed (last state: {state or 'unknown'})."
                ) from e
            if consecutive_get_errors > _POLL_GET_RETRY_LIMIT:
                raise RuntimeError(
                    f"Could not fetch batch job {batch_job.name} after "
                    f"{_POLL_GET_RETRY_LIMIT} retries."
                ) from e
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
            raise TimeoutError(
                f"Batch job {batch_job.name} did not reach a terminal state "
                f"within {timeout_hours}h (last state: {state})."
            )
        time.sleep(poll_interval)

    if state not in _BATCH_SUCCESS_STATES:
        raise RuntimeError(
            f"Batch inference job ended in non-success state: {state}"
        )

    dest_uri = getattr(cur.dest, "gcs_uri", None) if cur.dest else None
    if dest_uri:
        output_location: str = dest_uri
    else:
        logger.warning(
            f"Batch job returned no destination GCS URI; falling back to requested "
            f"output URI: {output_uri}"
        )
        output_location = output_uri
    logger.info(f"Batch output location: {output_location}")
    return output_location
