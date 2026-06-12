"""Shared diagnostic text handling for feed-level failures."""

from __future__ import annotations

import re

MAX_QUARANTINE_REASON_LENGTH = 2048
_TRUNCATION_MARKER = " [truncated]"

_SENSITIVE_QUERY_PARAM_RE = re.compile(
    r"(?i)\b("
    r"access_token|api_key|apikey|authorization|id_token|key|password|"
    r"passwd|pwd|refresh_token|signature|token|x-amz-signature|"
    r"x-goog-credential|x-goog-signature"
    r")=([^&\s;]+)"
)
_AUTH_HEADER_RE = re.compile(
    r"(?i)\b(authorization|proxy-authorization)\s*:\s*"
    r"(bearer|basic)\s+[A-Za-z0-9._~+/=-]+"
)
_AUTH_SCHEME_RE = re.compile(r"(?i)\b(bearer|basic)\s+[A-Za-z0-9._~+/=-]+")
_URL_USERINFO_RE = re.compile(r"(?i)\b(https?://)[^/\s:@]+:[^/\s@]+@")
_WHITESPACE_RE = re.compile(r"\s+")


def build_diagnostic(*parts: object | None) -> str:
    """Join diagnostic parts, redact secrets, and cap to storage/API limits."""
    text_parts = [
        str(part).strip()
        for part in parts
        if part is not None and str(part).strip()
    ]
    if not text_parts:
        return "unknown failure"
    return sanitize_diagnostic("; ".join(text_parts))


def sanitize_diagnostic(text: object) -> str:
    """Return redacted, single-line diagnostic text safe for quarantine."""
    sanitized = str(text)
    sanitized = _AUTH_HEADER_RE.sub(_redact_auth_header, sanitized)
    sanitized = _AUTH_SCHEME_RE.sub(r"\1 <redacted>", sanitized)
    sanitized = _URL_USERINFO_RE.sub(r"\1<redacted>@", sanitized)
    sanitized = _SENSITIVE_QUERY_PARAM_RE.sub(
        lambda match: f"{match.group(1)}=<redacted>",
        sanitized,
    )
    sanitized = _WHITESPACE_RE.sub(" ", sanitized).strip()
    if not sanitized:
        sanitized = "unknown failure"
    return cap_diagnostic(sanitized)


def cap_diagnostic(text: str) -> str:
    """Cap diagnostic text while keeping a visible truncation marker."""
    if len(text) <= MAX_QUARANTINE_REASON_LENGTH:
        return text
    prefix_len = MAX_QUARANTINE_REASON_LENGTH - len(_TRUNCATION_MARKER)
    return f"{text[:prefix_len].rstrip()}{_TRUNCATION_MARKER}"


def _redact_auth_header(match: re.Match[str]) -> str:
    return f"{match.group(1)}: <redacted>"
