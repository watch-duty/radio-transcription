"""Target backend resolution and execution helpers for Gemini SFT eval."""

from __future__ import annotations

from gemini_sft.config import EvalExecutionConfig, EvalModelTarget


def resolve_target_backend(
    target: EvalModelTarget,
    execution: EvalExecutionConfig,
) -> str:
    """Return the backend to use for an eval target.

    Backend selection is intentionally offline and conservative. Full Vertex
    endpoint resources default to online generation; all other model strings
    default to batch unless the config-wide execution backend forces a choice.
    """
    if execution.backend is not None:
        return execution.backend
    if "/endpoints/" in target.model:
        return "online"
    return "batch"
