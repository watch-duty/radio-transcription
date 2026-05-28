"""Custom schema-level enums and constants for rules evaluation."""

from enum import StrEnum


class EvaluationErrorType(StrEnum):
    ERROR_UNSPECIFIED = "ERROR_UNSPECIFIED"
    ERROR_FEED_ID_MISSING = "ERROR_FEED_ID_MISSING"
    ERROR_RULES_FETCH_FAILED = "ERROR_RULES_FETCH_FAILED"
