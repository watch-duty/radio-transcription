from enum import StrEnum


class PipelineConstants(StrEnum):
    """Centralized constants and configurations for backend pipeline integration tests."""

    ECHO_CHANNEL_NAME = "regression-test-channel-echo"
    RULE_NAME = "regression-test-fire-rule"
