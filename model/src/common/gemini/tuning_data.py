"""Gemini/Vertex AI audio-SFT JSONL builder and schema validator.

Provides ``build_audio_tuning_example`` for the current Gemini audio-SFT JSONL
schema and ``validate_audio_tuning_example`` for local schema-shape validation
before submitting a paid tuning job.

No GCP project or bucket constants are defined in this module. All GCP identifiers are
caller-supplied parameters.
"""

import logging
from collections.abc import Sequence
from typing import Any

from common.gemini.context import (
    ContextTurn,
    build_transcription_contents,
)

logger = logging.getLogger(__name__)


def build_audio_tuning_example(
    audio_uri: str,
    gt_text: str,
    system_prompt: str,
    user_prompt: str,
    history: Sequence[ContextTurn] | None = None,
    history_mode: str = "audio",
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
        history: Previous audio/transcript pairs from the same source
            recording. Each pair is emitted as ``user(audio) -> model(text)``.
        history_mode: ``audio`` emits notebook-style audio/text prior turns.
            ``text_turns`` emits prior user/model turns with text-only user
            turns and transcript model turns. ``transcript`` folds prior
            transcripts into the current user prompt. ``vapo_p3_transcript``
            uses the exact transcript-block template from the P3/P13 VAPO gate.

    Returns:
        A dict matching the current Vertex AI audio-SFT JSONL schema:
        ``{systemInstruction, contents: [history..., current user, target]}``.
    """
    contents = build_transcription_contents(
        audio_uri=audio_uri,
        user_prompt=user_prompt,
        history=history,
        history_mode=history_mode,
        file_data_casing="camel",
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
    """Return True if the example matches the Vertex AI audio-SFT JSONL schema.

    Validates the shape locally before submitting a paid tuning job. Rejects
    legacy ``{input_text, output_text}`` and flat ``{prompt, response}`` shapes.

    Args:
        example: A dict produced by ``build_audio_tuning_example`` or parsed
            from a JSONL line.

    Returns:
        True if the example is correctly shaped, False otherwise.
    """
    if "systemInstruction" not in example:
        return False
    contents = example.get("contents")
    if not isinstance(contents, list) or len(contents) < 2 or len(contents) % 2:
        return False
    audio_part_count = 0
    for index in range(0, len(contents), 2):
        user_turn = contents[index]
        model_turn = contents[index + 1]
        if not isinstance(user_turn, dict) or not isinstance(model_turn, dict):
            return False
        if user_turn.get("role") != "user" or model_turn.get("role") != "model":
            return False
        user_text = _extract_user_text(user_turn)
        file_data_parts = _extract_user_file_data_parts(user_turn)
        if not user_text.strip() and not file_data_parts:
            return False
        for file_data in file_data_parts:
            if not _is_valid_audio_file_data(file_data):
                return False
        audio_part_count += len(file_data_parts)
        if index == len(contents) - 2 and not file_data_parts:
            return False
        if not _extract_model_text(model_turn).strip():
            return False
    return audio_part_count == 1


def _extract_user_file_data(user_turn: dict[str, Any]) -> dict[str, Any] | None:
    file_data_parts = _extract_user_file_data_parts(user_turn)
    return file_data_parts[0] if file_data_parts else None


def _extract_user_file_data_parts(
    user_turn: dict[str, Any],
) -> list[dict[str, Any]]:
    user_parts = user_turn.get("parts", [])
    file_parts = [
        p for p in user_parts if isinstance(p, dict) and "fileData" in p
    ]
    file_data_parts: list[dict[str, Any]] = []
    for part in file_parts:
        file_data = part["fileData"]
        if isinstance(file_data, dict):
            file_data_parts.append(file_data)
    return file_data_parts


def _extract_user_text(user_turn: dict[str, Any]) -> str:
    user_parts = user_turn.get("parts", [])
    if not isinstance(user_parts, list):
        return ""
    return "\n".join(
        part["text"]
        for part in user_parts
        if isinstance(part, dict) and isinstance(part.get("text"), str)
    )


def _is_valid_audio_file_data(file_data: dict[str, Any] | None) -> bool:
    if file_data is None:
        return False
    file_uri = file_data.get("fileUri", "")
    return (
        isinstance(file_uri, str)
        and file_uri.startswith("gs://")
        and file_data.get("mimeType") == "audio/flac"
    )


def _extract_model_text(model_turn: dict[str, Any]) -> str:
    model_parts = model_turn.get("parts", [])
    if not isinstance(model_parts, list) or not model_parts:
        return ""
    first_model_part = model_parts[0]
    if not isinstance(first_model_part, dict):
        return ""
    return first_model_part.get("text") or ""
