"""Deterministic Gemini request identity helpers."""

from __future__ import annotations

import hashlib
import json
import typing

from common.gemini import context, vertex

if typing.TYPE_CHECKING:
    import collections.abc
    import pathlib


def build_request_identity(
    *,
    target_label: str,
    model: str,
    eval_manifest_uri: str,
    audio_uris: collections.abc.Sequence[str],
    request_digests: collections.abc.Sequence[str],
    system_prompt: str,
    user_prompt: str,
    prior_context_count: int,
    prior_context_mode: str,
    generation_config: dict[str, typing.Any],
    safety_settings: collections.abc.Sequence[dict[str, typing.Any]],
) -> dict[str, typing.Any]:
    """Build the request-defining identity for Gemini evaluation inference.

    Args:
        target_label: Stable model target label.
        model: Publisher model ID or full Vertex model resource name.
        eval_manifest_uri: Canonical evaluation manifest URI.
        audio_uris: Ordered audio URIs included in the evaluation.
        request_digests: Ordered request hashes aligned with ``audio_uris``.
        system_prompt: System instruction used for inference.
        user_prompt: User instruction used for each current audio turn.
        prior_context_count: Configured prior-turn window size.
        prior_context_mode: Configured context encoding mode.
        generation_config: Generation parameters that affect inference.
        safety_settings: Safety settings that affect inference.

    Returns:
        A JSON-compatible identity dictionary with copied mutable values.

    Raises:
        ValueError: If ``request_digests`` and ``audio_uris`` have different
            lengths.
    """
    audio_uri_list = list(audio_uris)
    request_digest_list = list(request_digests)
    if len(audio_uri_list) != len(request_digest_list):
        msg = "request_digests length must match audio_uris length"
        raise ValueError(msg)
    return {
        "schema_version": 2,
        "target_label": target_label,
        "model": model,
        "eval_manifest_uri": eval_manifest_uri,
        "audio_uris": audio_uri_list,
        "request_digests": request_digest_list,
        "system_prompt": system_prompt,
        "user_prompt": user_prompt,
        "prior_context_count": prior_context_count,
        "prior_context_mode": prior_context_mode,
        "generation_config": _json_safe_copy(generation_config),
        "safety_settings": _json_safe_copy(list(safety_settings)),
    }


def build_gemini_eval_request_identity(
    *,
    target_label: str,
    model: str,
    eval_manifest_uri: str,
    audio_uris: collections.abc.Sequence[str],
    system_prompt: str,
    user_prompt: str,
    prior_context_count: int,
    prior_context_mode: str,
    histories: collections.abc.Sequence[
        collections.abc.Sequence[context.ContextTurn]
    ]
    | None = None,
) -> dict[str, typing.Any]:
    """Build the canonical identity for Gemini evaluation inference.

    Args:
        target_label: Stable model target label.
        model: Publisher model ID or full Vertex model resource name.
        eval_manifest_uri: Canonical evaluation manifest URI.
        audio_uris: Ordered audio URIs included in the evaluation.
        system_prompt: System instruction used for inference.
        user_prompt: User instruction used for each current audio turn.
        prior_context_count: Configured prior-turn window size.
        prior_context_mode: Context encoding mode used to build each request.
        histories: Prior turns aligned one-for-one with ``audio_uris``. Empty
            histories are used when omitted.

    Returns:
        A request identity containing one deterministic digest per audio URI.

    Raises:
        ValueError: If histories are misaligned or ``prior_context_mode`` is
            unsupported.
    """
    audio_uri_list = list(audio_uris)
    history_list = (
        [list(history) for history in histories]
        if histories is not None
        else [[] for _ in audio_uri_list]
    )

    return build_request_identity(
        target_label=target_label,
        model=model,
        eval_manifest_uri=eval_manifest_uri,
        audio_uris=audio_uri_list,
        request_digests=_request_digests(
            audio_uris=audio_uri_list,
            histories=history_list,
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            prior_context_mode=prior_context_mode,
        ),
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        prior_context_count=prior_context_count,
        prior_context_mode=prior_context_mode,
        generation_config=vertex.GEMINI_GENERATION_CONFIG,
        safety_settings=vertex.GEMINI_SAFETY_SETTINGS,
    )


def request_identity_hash(identity: dict[str, typing.Any]) -> str:
    """Hash a request identity using deterministic JSON serialization.

    Args:
        identity: JSON-compatible request identity.

    Returns:
        The lowercase hexadecimal SHA-256 digest.
    """
    payload = json.dumps(
        identity,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def metadata_payload(
    identity: dict[str, typing.Any],
) -> dict[str, typing.Any]:
    """Build the metadata sidecar payload for a request identity.

    Args:
        identity: JSON-compatible request identity.

    Returns:
        A dictionary containing the identity and its deterministic hash.
    """
    return {
        "request_identity_hash": request_identity_hash(identity),
        "request_identity": identity,
    }


def write_metadata(
    path: pathlib.Path,
    identity: dict[str, typing.Any],
) -> None:
    """Write a request identity metadata sidecar as JSON.

    Args:
        path: Local destination path. Missing parent directories are created.
        identity: JSON-compatible request identity to persist.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(metadata_payload(identity), sort_keys=True) + "\n",
        encoding="utf-8",
    )


def load_metadata_identity(
    path: pathlib.Path,
    *,
    error_message: str = "request identity mismatch",
) -> dict[str, typing.Any]:
    """Load and hash-check request identity metadata from a sidecar file.

    Args:
        path: Local metadata sidecar path.
        error_message: Prefix used for validation error messages.

    Returns:
        The validated request identity dictionary.

    Raises:
        TypeError: If the sidecar or its request identity is not an object.
        ValueError: If a stored identity hash does not match its payload.
    """
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
    existing_identity: dict[str, typing.Any],
    request_identity: dict[str, typing.Any],
    error_message: str,
) -> None:
    """Validate exact equality between stored and requested identities.

    Args:
        existing_identity: Identity loaded from reusable output metadata.
        request_identity: Identity required by the current evaluation.
        error_message: Message used when validation fails.

    Raises:
        ValueError: If the identities differ.
    """
    if existing_identity != request_identity:
        raise ValueError(error_message)


def validate_prefix_identity(
    existing_identity: dict[str, typing.Any],
    request_identity: dict[str, typing.Any],
    error_message: str,
) -> None:
    """Validate that stored requests form a reusable prefix.

    Args:
        existing_identity: Identity loaded from reusable online output.
        request_identity: Identity required by the current evaluation.
        error_message: Message used when validation fails.

    Raises:
        ValueError: If fixed identity fields differ, URI and digest sequence
            lengths disagree, or stored URI and request-digest sequences are
            not prefixes of the requested sequences.
    """
    existing_audio = list(existing_identity.get("audio_uris") or [])
    request_audio = list(request_identity.get("audio_uris") or [])
    existing_digests = list(existing_identity.get("request_digests") or [])
    request_digests = list(request_identity.get("request_digests") or [])
    if _identity_without_sequences(
        existing_identity
    ) != _identity_without_sequences(request_identity):
        raise ValueError(error_message)
    if len(existing_audio) != len(existing_digests) or len(
        request_audio
    ) != len(request_digests):
        raise ValueError(error_message)
    if request_audio[: len(existing_audio)] != existing_audio:
        raise ValueError(error_message)
    if request_digests[: len(existing_digests)] == existing_digests:
        return
    raise ValueError(error_message)


def _identity_without_sequences(
    identity: dict[str, typing.Any],
) -> dict[str, typing.Any]:
    result = dict(identity)
    result.pop("audio_uris", None)
    result.pop("request_digests", None)
    return result


def _request_digests(
    *,
    audio_uris: collections.abc.Sequence[str],
    histories: collections.abc.Sequence[
        collections.abc.Sequence[context.ContextTurn]
    ],
    system_prompt: str,
    user_prompt: str,
    prior_context_mode: str,
) -> list[str]:
    if len(histories) != len(audio_uris):
        msg = "histories length must match audio_uris length"
        raise ValueError(msg)
    return [
        _request_digest(
            vertex.build_request(
                audio_uri,
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                history=histories[index],
                history_mode=prior_context_mode,
            )
        )
        for index, audio_uri in enumerate(audio_uris)
    ]


def _request_digest(
    request: collections.abc.Mapping[str, typing.Any],
) -> str:
    payload = json.dumps(
        request,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _json_safe_copy(value: typing.Any) -> typing.Any:
    return json.loads(json.dumps(value, sort_keys=True))
