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

from common.gemini.context import ContextTurn, build_transcript_context_prompt

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
            ``transcript`` folds prior transcripts into the current user prompt
            so Vertex tuning sees only one audio part.

    Returns:
        A dict matching the current Vertex AI audio-SFT JSONL schema:
        ``{systemInstruction, contents: [history..., current user, target]}``.
    """
    contents: list[dict[str, Any]] = []
    if history_mode not in {"audio", "transcript"}:
        msg = "history_mode must be 'audio' or 'transcript'"
        raise ValueError(msg)
    history_turns = list(history or ())
    if history_mode == "audio":
        for turn in history_turns:
            contents.extend(
                [
                    {
                        "role": "user",
                        "parts": [_audio_file_data_part(turn.audio_uri)],
                    },
                    {"role": "model", "parts": [{"text": turn.text}]},
                ]
            )
        current_user_prompt = user_prompt
    else:
        current_user_prompt = build_transcript_context_prompt(
            history_turns,
            user_prompt,
        )
    contents.extend(
        [
            {
                "role": "user",
                "parts": [
                    {"text": current_user_prompt},
                    _audio_file_data_part(audio_uri),
                ],
            },
            {"role": "model", "parts": [{"text": gt_text}]},
        ]
    )
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
    for index in range(0, len(contents), 2):
        user_turn = contents[index]
        model_turn = contents[index + 1]
        if not isinstance(user_turn, dict) or not isinstance(model_turn, dict):
            return False
        if user_turn.get("role") != "user" or model_turn.get("role") != "model":
            return False
        if not _is_valid_audio_file_data(_extract_user_file_data(user_turn)):
            return False
        if not _extract_model_text(model_turn).strip():
            return False
    return True


def _audio_file_data_part(audio_uri: str) -> dict[str, Any]:
    return {
        "fileData": {
            "mimeType": "audio/flac",
            "fileUri": audio_uri,
        }
    }


def _extract_user_file_data(user_turn: dict[str, Any]) -> dict[str, Any] | None:
    user_parts = user_turn.get("parts", [])
    file_parts = [
        p for p in user_parts if isinstance(p, dict) and "fileData" in p
    ]
    if not file_parts:
        return None
    fd = file_parts[0]["fileData"]
    if not isinstance(fd, dict):
        return None
    return fd


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
