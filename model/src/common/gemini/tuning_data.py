"""Gemini/Vertex AI audio-SFT JSONL builder and schema validator.

Provides ``build_audio_tuning_example`` for the current Gemini audio-SFT JSONL
schema and ``validate_audio_tuning_example`` for local schema-shape validation
before submitting a paid tuning job.

No GCP project or bucket constants are defined in this module. All GCP identifiers are
caller-supplied parameters.
"""

from typing import Any


def build_audio_tuning_example(
    audio_uri: str,
    gt_text: str,
    system_prompt: str,
    user_prompt: str,
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
    if not isinstance(contents, list) or len(contents) != 2:
        return False
    user_turn, model_turn = contents
    if not isinstance(user_turn, dict) or not isinstance(model_turn, dict):
        return False
    if user_turn.get("role") != "user" or model_turn.get("role") != "model":
        return False

    file_data = _extract_user_file_data(user_turn)
    if not _is_valid_audio_file_data(file_data):
        return False

    model_text = _extract_model_text(model_turn)
    return bool(model_text.strip())


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
