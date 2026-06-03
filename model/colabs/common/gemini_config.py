"""Shared Gemini transcription generation settings."""

from __future__ import annotations

GEMINI_GENERATION_CONFIG = {"temperature": 0.0, "max_output_tokens": 512}

GEMINI_THINKING_LEVEL = "LOW"

GEMINI_HTTP_RETRY_CONFIG = {
    "attempts": 5,
    "max_delay": 60.0,
    "initial_delay": 1.0,
    "exp_base": 2.0,
}

GEMINI_SAFETY_SETTINGS = [
    {"category": "HARM_CATEGORY_HATE_SPEECH", "threshold": "BLOCK_NONE"},
    {"category": "HARM_CATEGORY_SEXUALLY_EXPLICIT", "threshold": "BLOCK_NONE"},
    {"category": "HARM_CATEGORY_DANGEROUS_CONTENT", "threshold": "BLOCK_NONE"},
    {"category": "HARM_CATEGORY_HARASSMENT", "threshold": "BLOCK_NONE"},
]
