"""Vertex AI audio-SFT JSONL builder and schema validator.

Provides ``build_example`` (LIB-04) — the current Vertex AI audio-SFT JSONL schema
with ``systemInstruction`` sibling of ``contents`` — and ``validate_example`` for local
schema-shape validation before submitting a paid tuning job (Pitfall 1).

No GCP project or bucket constants are defined in this module. All GCP identifiers are
caller-supplied parameters.
"""

import json
import logging
from typing import Any

logger = logging.getLogger(__name__)


def build_example(
    audio_uri: str,
    gt_text: str,
    system_prompt: str,
    user_prompt: str,
) -> dict[str, Any]:
    """Build a single Vertex AI audio-SFT JSONL example.

    Ported from ``autoresearch-gemini-sft/src/build_e3_sft_jsonl.py`` (lines 60-76)
    with the ``user_prompt`` parameter added (the source hardcoded ``USER_PROMPT`` —
    the prompt constants now live in ``common.prompts``).

    Args:
        audio_uri: GCS URI (gs://...) to the audio segment; must be audio/flac.
        gt_text: Ground-truth transcript text.
        system_prompt: System instruction text (e.g. ``common.prompts.PRODUCTION_PROMPT``).
        user_prompt: Per-turn user instruction (e.g. ``common.prompts.USER_PROMPT``).

    Returns:
        A dict matching the current Vertex AI audio-SFT JSONL schema:
        ``{systemInstruction, contents: [{role:user, parts:[fileData, text]},
                                          {role:model, parts:[text]}]}``.
    """
    return {
        "systemInstruction": {
            "role": "system",
            "parts": [{"text": system_prompt}],
        },
        "contents": [
            {
                "role": "user",
                "parts": [
                    {
                        "fileData": {
                            "mimeType": "audio/flac",
                            "fileUri": audio_uri,
                        }
                    },
                    {"text": user_prompt},
                ],
            },
            {"role": "model", "parts": [{"text": gt_text}]},
        ],
    }


def validate_example(example: dict[str, Any]) -> bool:
    """Return True if the example matches the Vertex AI audio-SFT JSONL schema.

    Validates the shape locally before submitting a paid tuning job (Pitfall 1).
    Rejects legacy ``{input_text, output_text}`` and flat ``{prompt, response}`` shapes.

    Args:
        example: A dict produced by ``build_example`` or parsed from a JSONL line.

    Returns:
        True if the example is correctly shaped, False otherwise.
    """
    if "contents" not in example or "systemInstruction" not in example:
        return False
    contents = example["contents"]
    if not isinstance(contents, list) or len(contents) != 2:
        return False
    user_turn, model_turn = contents
    if user_turn.get("role") != "user" or model_turn.get("role") != "model":
        return False
    user_parts = user_turn.get("parts", [])
    file_parts = [p for p in user_parts if "fileData" in p]
    if not file_parts:
        return False
    fd = file_parts[0]["fileData"]
    if not fd.get("fileUri", "").startswith("gs://"):
        return False
    if fd.get("mimeType") != "audio/flac":
        return False
    model_parts = model_turn.get("parts", [{}])
    if not model_parts:
        return False
    model_text = model_parts[0].get("text", "")
    return bool(model_text.strip())
