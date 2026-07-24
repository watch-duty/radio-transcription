"""Frozen production Gemini request semantics for this one-off SFT run."""

from __future__ import annotations

import ast
import dataclasses
import json
import pathlib
import runpy
import subprocess
import typing

from sft_run import artifacts

if typing.TYPE_CHECKING:
    import collections.abc


_PROMPTS_PATH = pathlib.PurePosixPath(
    "backend/pipeline/transcription/transcribers/prompts.py"
)
_GEMINI_PATH = pathlib.PurePosixPath(
    "backend/pipeline/transcription/transcribers/gemini.py"
)
_SOURCE_PATHS = (_GEMINI_PATH, _PROMPTS_PATH)
_DEFAULT_SYMBOLS = (
    "_DEFAULT_TEMPERATURE",
    "_DEFAULT_MAX_OUTPUT_TOKENS",
    "_DEFAULT_SAFETY_SETTINGS",
)


class ProductionContractError(RuntimeError):
    """Raised when the production source no longer has the expected shape."""


def _contract_error(
    message: str, cause: BaseException | None = None
) -> typing.Never:
    if cause is None:
        raise ProductionContractError(message)
    raise ProductionContractError(message) from cause


@dataclasses.dataclass(frozen=True)
class RequestContract:
    """Provider-independent values that define one production request."""

    system_prompt: str
    temperature: float
    max_output_tokens: int
    thinking_budget: int
    safety_settings: tuple[tuple[str, str], ...]
    production_commit: str
    source_sha256: tuple[tuple[str, str], ...] = ()


def _literal_assignments(tree: ast.Module) -> dict[str, object]:
    assignments: dict[str, object] = {}
    for node in tree.body:
        if not isinstance(node, ast.Assign) or len(node.targets) != 1:
            continue
        target = node.targets[0]
        if (
            not isinstance(target, ast.Name)
            or target.id not in _DEFAULT_SYMBOLS
        ):
            continue
        try:
            assignments[target.id] = ast.literal_eval(node.value)
        except (ValueError, TypeError, SyntaxError) as exc:
            message = f"production symbol {target.id} is no longer a literal"
            _contract_error(message, exc)

    missing = sorted(set(_DEFAULT_SYMBOLS) - assignments.keys())
    if missing:
        message = f"missing production contract symbols: {', '.join(missing)}"
        _contract_error(message)
    return assignments


def _call_name(node: ast.expr) -> str | None:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return node.attr
    return None


def _thinking_budget(tree: ast.Module) -> int:
    generation_calls = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and _call_name(node.func) == "GenerateContentConfig"
    ]
    if len(generation_calls) != 1:
        _contract_error(
            "expected exactly one production GenerateContentConfig call"
        )

    thinking_values = [
        keyword.value
        for keyword in generation_calls[0].keywords
        if keyword.arg == "thinking_config"
    ]
    if len(thinking_values) != 1:
        _contract_error(
            "production GenerateContentConfig must set thinking_config once"
        )

    thinking_call = thinking_values[0]
    if not isinstance(thinking_call, ast.Call) or (
        _call_name(thinking_call.func) != "ThinkingConfig"
    ):
        _contract_error(
            "production thinking_config must construct ThinkingConfig"
        )

    budgets = [
        keyword.value
        for keyword in thinking_call.keywords
        if keyword.arg == "thinking_budget"
    ]
    if len(budgets) != 1:
        _contract_error(
            "production ThinkingConfig must set thinking_budget once"
        )
    try:
        budget = ast.literal_eval(budgets[0])
    except (ValueError, TypeError, SyntaxError) as exc:
        _contract_error(
            "production thinking_budget is no longer a literal", exc
        )
    if not isinstance(budget, int) or isinstance(budget, bool):
        _contract_error("production thinking_budget must be an integer")
    return budget


def _source_commit(repo_root: pathlib.Path) -> str:
    try:
        result = subprocess.run(
            [
                "git",
                "-C",
                str(repo_root),
                "log",
                "-1",
                "--format=%H",
                "--",
                _PROMPTS_PATH.as_posix(),
                _GEMINI_PATH.as_posix(),
            ],
            check=True,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError) as exc:
        _contract_error("cannot resolve the production source commit", exc)
    commit = result.stdout.strip()
    if len(commit) != 40:
        _contract_error("production source commit is missing")
    return commit


def _source_sha256(repo_root: pathlib.Path) -> tuple[tuple[str, str], ...]:
    hashes: list[tuple[str, str]] = []
    for relative_path in _SOURCE_PATHS:
        source_path = repo_root / relative_path
        try:
            digest = artifacts.sha256_bytes(source_path.read_bytes())
        except OSError as exc:
            message = f"cannot hash production source: {source_path}"
            _contract_error(message, exc)
        hashes.append((relative_path.as_posix(), digest))
    return tuple(hashes)


def _frozen_source_sha256(value: object) -> tuple[tuple[str, str], ...]:
    expected_paths = {path.as_posix() for path in _SOURCE_PATHS}
    if not isinstance(value, dict) or set(value) != expected_paths:
        _contract_error("frozen source_sha256 has unexpected source paths")
    hashes: list[tuple[str, str]] = []
    for relative_path in _SOURCE_PATHS:
        path_string = relative_path.as_posix()
        digest = value[path_string]
        if not isinstance(digest, str) or len(digest) != 64:
            message = f"frozen source_sha256 is invalid for {path_string}"
            _contract_error(message)
        hashes.append((path_string, digest))
    return tuple(hashes)


def _safety_settings(value: object) -> tuple[tuple[str, str], ...]:
    if not isinstance(value, list):
        _contract_error("production _DEFAULT_SAFETY_SETTINGS must be a list")
    settings: list[tuple[str, str]] = []
    for index, item in enumerate(value):
        if not isinstance(item, dict) or set(item) != {"category", "threshold"}:
            message = f"production safety setting {index} has unexpected fields"
            _contract_error(message)
        category = item["category"]
        threshold = item["threshold"]
        if not isinstance(category, str) or not isinstance(threshold, str):
            message = f"production safety setting {index} must contain strings"
            _contract_error(message)
        settings.append((category, threshold))
    return tuple(settings)


def load_production_contract(repo_root: pathlib.Path) -> RequestContract:
    """Read request values from production source without importing the backend."""
    root = pathlib.Path(repo_root).resolve()
    prompts_path = root / _PROMPTS_PATH
    gemini_path = root / _GEMINI_PATH

    try:
        prompt_namespace = runpy.run_path(str(prompts_path))
    except (OSError, RuntimeError, SyntaxError) as exc:
        message = f"cannot load production prompts source: {prompts_path}"
        _contract_error(message, exc)
    system_prompt = prompt_namespace.get("GEMINI_PROMPT")
    if not isinstance(system_prompt, str):
        _contract_error(
            "production prompts source is missing string GEMINI_PROMPT"
        )

    try:
        tree = ast.parse(gemini_path.read_text(encoding="utf-8"))
    except (OSError, SyntaxError) as exc:
        message = f"cannot parse production Gemini source: {gemini_path}"
        _contract_error(message, exc)
    assignments = _literal_assignments(tree)

    temperature = assignments["_DEFAULT_TEMPERATURE"]
    if not isinstance(temperature, (int, float)) or isinstance(
        temperature, bool
    ):
        _contract_error("production temperature must be numeric")
    max_output_tokens = assignments["_DEFAULT_MAX_OUTPUT_TOKENS"]
    if not isinstance(max_output_tokens, int) or isinstance(
        max_output_tokens, bool
    ):
        _contract_error("production max output tokens must be an integer")

    return RequestContract(
        system_prompt=system_prompt,
        temperature=float(temperature),
        max_output_tokens=max_output_tokens,
        thinking_budget=_thinking_budget(tree),
        safety_settings=_safety_settings(
            assignments["_DEFAULT_SAFETY_SETTINGS"]
        ),
        production_commit=_source_commit(root),
        source_sha256=_source_sha256(root),
    )


def load_frozen_contract(path: pathlib.Path) -> RequestContract:
    """Load the immutable JSON snapshot into its typed representation."""
    source_path = pathlib.Path(path)
    try:
        payload = json.loads(source_path.read_bytes())
    except (OSError, json.JSONDecodeError) as exc:
        message = f"cannot load frozen request contract: {source_path}"
        _contract_error(message, exc)
    if not isinstance(payload, dict):
        _contract_error("frozen request contract must be an object")

    required = {
        "system_prompt",
        "temperature",
        "max_output_tokens",
        "thinking_budget",
        "safety_settings",
        "production_commit",
        "source_sha256",
    }
    missing = sorted(required - payload.keys())
    if missing:
        message = f"frozen request contract is missing: {', '.join(missing)}"
        _contract_error(message)
    return RequestContract(
        system_prompt=payload["system_prompt"],
        temperature=payload["temperature"],
        max_output_tokens=payload["max_output_tokens"],
        thinking_budget=payload["thinking_budget"],
        safety_settings=_safety_settings(payload["safety_settings"]),
        production_commit=payload["production_commit"],
        source_sha256=_frozen_source_sha256(payload["source_sha256"]),
    )


def assert_no_contract_drift(
    frozen: RequestContract, current: RequestContract
) -> None:
    """Raise with field-level evidence when production differs from the snapshot."""
    changed = [
        field.name
        for field in dataclasses.fields(RequestContract)
        if getattr(frozen, field.name) != getattr(current, field.name)
    ]
    if changed:
        message = f"production request contract drifted: {', '.join(changed)}"
        raise AssertionError(message)


def _system_instruction(contract: RequestContract) -> dict[str, object]:
    return {
        "role": "system",
        "parts": [{"text": contract.system_prompt}],
    }


def _audio_user_content(audio_uri: str) -> dict[str, object]:
    if not isinstance(audio_uri, str) or not audio_uri:
        message = "audio URI must be a nonempty string"
        raise ValueError(message)
    return {
        "role": "user",
        "parts": [
            {
                "fileData": {
                    "fileUri": audio_uri,
                    "mimeType": "audio/flac",
                }
            }
        ],
    }


def build_tuning_example(
    row: collections.abc.Mapping[str, object], contract: RequestContract
) -> dict[str, object]:
    """Build one audio-only user turn followed by its target model turn."""
    audio_uri = row["audio_filepath"]
    target_text = row["text"]
    if not isinstance(audio_uri, str) or not isinstance(target_text, str):
        message = "tuning rows require string audio_filepath and text"
        raise TypeError(message)
    return {
        "systemInstruction": _system_instruction(contract),
        "contents": [
            _audio_user_content(audio_uri),
            {"role": "model", "parts": [{"text": target_text}]},
        ],
    }


def build_online_request(
    audio_uri: str, contract: RequestContract
) -> dict[str, object]:
    """Build the online production-parity request for one FLAC URI."""
    return {
        "systemInstruction": _system_instruction(contract),
        "contents": [_audio_user_content(audio_uri)],
        "generationConfig": {
            "temperature": contract.temperature,
            "maxOutputTokens": contract.max_output_tokens,
            "thinkingConfig": {"thinkingBudget": contract.thinking_budget},
        },
        "safetySettings": [
            {"category": category, "threshold": threshold}
            for category, threshold in contract.safety_settings
        ],
    }


def request_digest(request: collections.abc.Mapping[str, object]) -> str:
    """Hash the canonical UTF-8 JSON serialization of a request."""
    return artifacts.sha256_bytes(artifacts.canonical_json_bytes(dict(request)))
