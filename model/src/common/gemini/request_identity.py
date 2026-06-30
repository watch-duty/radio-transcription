"""Deterministic Gemini request identity helpers."""

from __future__ import annotations

import hashlib
import json
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Sequence
    from pathlib import Path


def build_request_identity(
    *,
    target_label: str,
    model: str,
    eval_manifest_uri: str,
    audio_uris: Sequence[str],
    system_prompt: str,
    user_prompt: str,
    prior_context_count: int,
    prior_context_mode: str,
    generation_config: dict[str, Any],
    safety_settings: Sequence[dict[str, Any]],
) -> dict[str, Any]:
    """Return the request-defining identity for Gemini eval inference."""
    return {
        "schema_version": 1,
        "target_label": target_label,
        "model": model,
        "eval_manifest_uri": eval_manifest_uri,
        "audio_uris": list(audio_uris),
        "system_prompt": system_prompt,
        "user_prompt": user_prompt,
        "prior_context_count": prior_context_count,
        "prior_context_mode": prior_context_mode,
        "generation_config": _json_safe_copy(generation_config),
        "safety_settings": _json_safe_copy(list(safety_settings)),
    }


def request_identity_hash(identity: dict[str, Any]) -> str:
    """Return a stable SHA-256 hash for a request identity."""
    payload = json.dumps(
        identity,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def metadata_payload(identity: dict[str, Any]) -> dict[str, Any]:
    """Return the metadata sidecar payload for a request identity."""
    return {
        "request_identity_hash": request_identity_hash(identity),
        "request_identity": identity,
    }


def write_metadata(path: Path, identity: dict[str, Any]) -> None:
    """Write a request identity metadata sidecar."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(metadata_payload(identity), sort_keys=True) + "\n",
        encoding="utf-8",
    )


def load_metadata_identity(
    path: Path,
    *,
    error_message: str = "request identity mismatch",
) -> dict[str, Any]:
    """Load and hash-check request identity metadata from a sidecar file."""
    metadata = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(metadata, dict):
        msg = f"{error_message}: invalid metadata"
        raise TypeError(msg)
    identity = metadata.get("request_identity")
    if not isinstance(identity, dict):
        msg = f"{error_message}: missing identity"
        raise TypeError(msg)
    expected_hash = metadata.get("request_identity_hash")
    if expected_hash and expected_hash != request_identity_hash(identity):
        msg = f"{error_message}: hash mismatch"
        raise ValueError(msg)
    return identity


def validate_exact_identity(
    existing_identity: dict[str, Any],
    request_identity: dict[str, Any],
    error_message: str,
) -> None:
    """Raise if two request identities are not exactly equal."""
    if existing_identity != request_identity:
        raise ValueError(error_message)


def validate_prefix_identity(
    existing_identity: dict[str, Any],
    request_identity: dict[str, Any],
    error_message: str,
) -> None:
    """Raise unless identity matches exactly or stored audio is a prefix."""
    existing_audio = list(existing_identity.get("audio_uris") or [])
    request_audio = list(request_identity.get("audio_uris") or [])
    if _identity_without_audio(existing_identity) != _identity_without_audio(
        request_identity
    ):
        raise ValueError(error_message)
    if existing_audio == request_audio:
        return
    if request_audio[: len(existing_audio)] == existing_audio:
        return
    raise ValueError(error_message)


def _identity_without_audio(identity: dict[str, Any]) -> dict[str, Any]:
    result = dict(identity)
    result.pop("audio_uris", None)
    return result


def _json_safe_copy(value: Any) -> Any:
    return json.loads(json.dumps(value, sort_keys=True))
