import pytest

from backend.pipeline.transcription.options import TranscriptionOptions
from backend.pipeline.transcription.orchestration import get_pipeline


def test_pipeline_topology_typehints() -> None:
    """Builds the DAG to trigger Apache Beam's static type checker instantaneously."""
    options = TranscriptionOptions(
        flags=[
            "--project",
            "test-project",
            "--input_subscription",
            "projects/test-project/subscriptions/in",
            "--output_topic",
            "projects/test-project/topics/out",
            "--dlq_topic",
            "projects/test-project/topics/dlq",
        ]
    )

    # Calling get_pipeline() dynamically maps the entire Apache Beam graph in memory.
    # If there are any TypeCheckErrors or missing links between transforms,
    # the Python SDK will immediately raise an exception right here!
    pipeline = get_pipeline(options)

    # We do NOT invoke pipeline.run(), because we just wanted to validate the topological typing.
    assert pipeline is not None


def test_pipeline_topology_typehints_with_bypass_stitching() -> None:
    """Builds the DAG with bypass_stitching to trigger Apache Beam's static type checker."""
    options = TranscriptionOptions(
        flags=[
            "--project",
            "test-project",
            "--input_subscription",
            "projects/test-project/subscriptions/in",
            "--output_topic",
            "projects/test-project/topics/out",
            "--dlq_topic",
            "projects/test-project/topics/dlq",
            "--bypass_stitching",
            "true",
        ]
    )

    pipeline = get_pipeline(options)
    assert pipeline is not None


def test_pipeline_invalid_timeout_configuration() -> None:
    """Verifies ValueError raised when out_of_order_timeout_ms >= stale_timeout_ms."""
    options = TranscriptionOptions(
        flags=[
            "--project",
            "test-project",
            "--input_subscription",
            "projects/test-project/subscriptions/in",
            "--output_topic",
            "projects/test-project/topics/out",
            "--dlq_topic",
            "projects/test-project/topics/dlq",
            "--out_of_order_timeout_ms",
            "80000",
            "--stale_timeout_ms",
            "70000",
        ]
    )

    with pytest.raises(
        ValueError,
        match=r"stale_timeout_ms .* must be strictly greater than out_of_order_timeout_ms",
    ):
        get_pipeline(options)
