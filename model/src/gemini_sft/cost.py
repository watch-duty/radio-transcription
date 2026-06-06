"""Gemini SFT cost estimate helpers."""

from __future__ import annotations

import logging
from typing import Final

logger = logging.getLogger(__name__)

DEFAULT_BASE_MODEL: Final = "gemini-3.1-flash-lite"
FALLBACK_SEGMENT_DURATION_SECONDS: Final = 15.0
SUPPORTED_SFT_BASE_MODELS: Final = frozenset(
    {
        "gemini-3.1-flash-lite",
        "gemini-2.5-pro",
        "gemini-2.5-flash",
        "gemini-2.5-flash-lite",
    }
)
SFT_MODEL_DISPLAY_NAMES: Final = {
    "gemini-3.1-flash-lite": "Gemini 3.1 Flash-Lite",
    "gemini-2.5-pro": "Gemini 2.5 Pro",
    "gemini-2.5-flash": "Gemini 2.5 Flash",
    "gemini-2.5-flash-lite": "Gemini 2.5 Flash-Lite",
}
SFT_TRAINING_COST_PER_MILLION: Final = {
    "gemini-3.1-flash-lite": 3.00,
    "gemini-2.5-pro": 25.00,
    "gemini-2.5-flash": 5.00,
    "gemini-2.5-flash-lite": 1.50,
}


def validate_supported_model(base_model: str) -> None:
    """Raise ``ValueError`` when a base model is unsupported."""
    if base_model not in SUPPORTED_SFT_BASE_MODELS:
        raise ValueError(
            f"Base model {base_model!r} is not supported for this Gemini SFT "
            "workflow. Use one of: "
            f"{', '.join(sorted(SUPPORTED_SFT_BASE_MODELS))}."
        )


def print_tune_cost_estimate(
    *,
    n_examples: int,
    epochs: int,
    total_secs: float,
    base_model: str,
    basis: str,
) -> None:
    """Print an operator-facing estimated tuning cost."""
    audio_tokens_per_sec: Final = 32
    cost_per_million_tokens = SFT_TRAINING_COST_PER_MILLION[base_model]
    model_display_name = SFT_MODEL_DISPLAY_NAMES[base_model]
    estimated_tokens = total_secs * audio_tokens_per_sec * epochs
    estimated_cost = (estimated_tokens / 1_000_000) * cost_per_million_tokens

    print("\n--- Tune Cost Estimate ---")
    print(f"  Examples:          {n_examples}")
    print(f"  Epochs:            {epochs}")
    print(f"  Est. audio tokens: {estimated_tokens:,.0f} ({basis} x 32 tok/s)")
    print(f"  Est. cost:        ~${estimated_cost:.2f} USD")
    print(
        f"  NOTE: Using {model_display_name} SFT rate "
        f"(${cost_per_million_tokens:.2f}/M training tokens)."
    )
    print(
        "        Actual billing may differ. You accept responsibility for GCP charges.\n"
    )


def confirm_tune_cost(confirm: bool) -> int:
    """Return 0 when the operator confirms the paid tune run."""
    if confirm:
        return 0
    try:
        answer = input("Type 'yes' to proceed with tune: ").strip().lower()
    except EOFError:
        answer = ""
    if answer != "yes":
        logger.info("Tune aborted by operator.")
        return 130
    return 0
