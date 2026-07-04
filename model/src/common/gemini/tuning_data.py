"""Gemini/Vertex AI audio-SFT JSONL builder and schema validator.

Provides ``build_audio_tuning_example`` for the current Gemini audio-SFT JSONL
schema and ``validate_audio_tuning_example`` for local schema-shape validation
before submitting a paid tuning job.

No GCP project or bucket constants are defined in this module. All GCP identifiers are
caller-supplied parameters.
"""

from collections.abc import Sequence
from typing import Any

from common.gemini.context import (
    ContextTurn,
    build_transcription_contents,
)


def build_audio_tuning_example(
    audio_uri: str,
    gt_text: str,
    system_prompt: str,
    user_prompt: str,
    history: Sequence[ContextTurn] | None = None,
    history_mode: str = "text_turns",
) -> dict[str, Any]:
    """Build a single Gemini/Vertex AI audio-SFT JSONL example.

    The caller supplies ``system_prompt`` and ``user_prompt`` so prompt text stays
    centralized instead of being hardcoded here.

    Args:
        audio_uri: GCS URI (gs://...) to the audio segment; must be audio/flac.
        gt_text: Ground-truth transcript text.
        system_prompt: Caller-supplied system instruction (e.g. from
            ``common.gemini.prompts``).
        user_prompt: Per-turn user instruction (e.g. from
            ``common.gemini.prompts``).
        history: Previous same-source turns. Prior audio URIs are retained for
            provenance, but prior context is emitted as transcript text only.
        history_mode: ``text_turns`` emits prior user/model turns with
            text-only user turns and transcript model turns. ``transcript``
            folds prior transcripts into the current user prompt.
            ``guarded_transcript_block`` folds prior transcripts into a
            guarded block that explicitly says not to re-transcribe or continue
            prior turns.

    Returns:
        A dict matching the current Vertex AI audio-SFT JSONL schema:
        ``{systemInstruction, contents: [history..., current user, target]}``.
    """
    contents = build_transcription_contents(
        audio_uri=audio_uri,
        user_prompt=user_prompt,
        history=history,
        history_mode=history_mode,
    )
    contents.append({"role": "model", "parts": [{"text": gt_text}]})
    return {
        "systemInstruction": {
            "role": "system",
            "parts": [{"text": system_prompt}],
        },
        "contents": contents,
    }


def validate_audio_tuning_example(example: dict[str, Any]) -> bool:
    """Return True if the example matches the local audio-SFT data contract.

    This intentionally avoids provider-specific checks such as audio count,
    file URI scheme, MIME type, or other API constraints that may change over
    time. Vertex remains the source of truth for those constraints. Local
    preflight only rejects examples missing the wrapper fields or the
    non-empty target text needed by our training data contract.

    Args:
        example: A dict produced by ``build_audio_tuning_example`` or parsed
            from a JSONL line.

    Returns:
        True if the example is correctly shaped, False otherwise.
    """
    if "systemInstruction" not in example:
        return False
    contents = example.get("contents")
    if not isinstance(contents, list) or not contents:
        return False
    target_turns = (
        turn
        for turn in contents
        if isinstance(turn, dict) and turn.get("role") == "model"
    )
    return any(_extract_model_text(turn).strip() for turn in target_turns)


def _extract_model_text(model_turn: dict[str, Any]) -> str:
    model_parts = model_turn.get("parts", [])
    if not isinstance(model_parts, list) or not model_parts:
        return ""
    first_model_part = model_parts[0]
    if not isinstance(first_model_part, dict):
        return ""
    return first_model_part.get("text") or ""
