"""Vertex AI Gemini tuning job and batch inference submission (behind [vertex] extra).

Provides ``submit_tuning_job`` (LIB-05) and ``submit_batch_inference`` for submitting
and polling Vertex AI Gemini SFT tuning and batch inference jobs via ``google-genai``
2.x.

Key corrections from the frozen autoresearch-gemini-sft source (submit_sft.py):
- Validation dataset uses ``types.TuningDataset`` (STACK.md §2 corrects the stale type — Pitfall 2)
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
    base_model: str = "gemini-2.5-flash",
    val_uri: "str | None" = None,
    epoch_count: int = 5,
    adapter_size: str = "ONE",
    lr_multiplier: float = 1.0,
    poll_interval: int = 30,
) -> str:
    """Submit a Vertex AI Gemini SFT tuning job and poll until a terminal state.

    Ported from ``autoresearch-gemini-sft/src/submit_sft.py`` with mandatory
    corrections: validation dataset uses ``types.TuningDataset`` (corrected per STACK.md §2), parameterized
    hyperparameters, required project/location, RuntimeError on failure.

    Args:
        train_uri: GCS URI for training JSONL (gs://...).
        display_name: Display name for the tuned model resource.
        project: GCP project ID (required — no silent default, Pitfall 4).
        location: GCP region (use 'us-central1' for evaluation feature availability).
        base_model: Base model name. Defaults to 'gemini-2.5-flash'.
        val_uri: Optional GCS URI for validation JSONL.
        epoch_count: Number of training epochs (1-100). SDK default is 5.
        adapter_size: Adapter size key — one of ONE, FOUR, EIGHT, SIXTEEN.
        lr_multiplier: Learning-rate multiplier (0.001-10.0). Defaults to 1.0.
        poll_interval: Seconds between state-poll requests.

    Returns:
        Tuned model endpoint name (resource name string).

    Raises:
        ImportError: If the ``[vertex]`` extra is not installed.
        RuntimeError: If the tuning job ends in a non-success terminal state.
        KeyError: If ``adapter_size`` is not a valid key in ``_ADAPTER_ENUM``.
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
        # D-08: use TuningDataset for validation set (STACK.md §2 correction)
        cfg_kwargs["validation_dataset"] = types.TuningDataset(gcs_uri=val_uri)

    job = client.tunings.tune(
        base_model=base_model,
        training_dataset=types.TuningDataset(gcs_uri=train_uri),
        config=types.CreateTuningJobConfig(**cfg_kwargs),
    )
    logger.info(f"Submitted tuning job: {job.name}")

    last_state: "str | None" = None
    state: str = ""
    while True:
        cur = client.tunings.get(name=job.name)
        state = getattr(cur.state, "name", str(cur.state))
        if state != last_state:
            logger.info(f"[{time.strftime('%H:%M:%S')}] state: {state}")
            last_state = state
        if state in _TERMINAL_STATES:
            break
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
) -> str:
    """Submit a Vertex AI batch inference job and poll until a terminal state.

    Args:
        input_uri: GCS URI for the batch input file (gs://...).
        output_uri: GCS URI for the batch output destination (gs://...).
        model: Model resource name or base model ID to use for inference.
        project: GCP project ID (required — no silent default).
        location: GCP region for the batch job.
        poll_interval: Seconds between state-poll requests.

    Returns:
        Resolved batch output location (GCS URI string).

    Raises:
        ImportError: If the ``[vertex]`` extra is not installed.
        RuntimeError: If the batch job ends in FAILED or CANCELLED state.
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
    while True:
        cur = client.batches.get(name=batch_job.name)
        state = getattr(cur.state, "name", str(cur.state))
        if state != last_state:
            logger.info(f"[{time.strftime('%H:%M:%S')}] batch state: {state}")
            last_state = state
        if state in _BATCH_TERMINAL_STATES:
            break
        time.sleep(poll_interval)

    if state not in _BATCH_SUCCESS_STATES:
        raise RuntimeError(
            f"Batch inference job ended in non-success state: {state}"
        )

    output_location: str = getattr(cur, "dest", output_uri)
    logger.info(f"Batch output location: {output_location}")
    return output_location
