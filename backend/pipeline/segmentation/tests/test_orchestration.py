import argparse
import json
from pathlib import Path

import pytest

from backend.pipeline.segmentation.options import SegmentationOptions
from backend.pipeline.segmentation.orchestration import get_pipeline


def test_pipeline_topology_typehints() -> None:
    """Builds the DAG to trigger Apache Beam's static type checker instantaneously."""
    options = SegmentationOptions(
        flags=[
            "--project",
            "test-project",
            "--input_subscription",
            "projects/test-project/subscriptions/audio-in",
            "--output_topic",
            "projects/test-project/topics/out",
            "--dlq_topic",
            "projects/test-project/topics/dlq",
            "--staging_audio_bucket",
            "test-staging-bucket",
        ]
    )

    # Calling get_pipeline() dynamically maps the entire Apache Beam graph in memory.
    # If there are any TypeCheckErrors or missing links between transforms,
    # the Python SDK will immediately raise an exception right here!
    pipeline = get_pipeline(options)

    # We do NOT invoke pipeline.run(), because we just wanted to validate the topological typing.
    assert pipeline is not None


def test_pipeline_invalid_timeout() -> None:
    """Verifies failure when ooo timeout >= stale_timeout_ms."""
    options = SegmentationOptions(
        flags=[
            "--project",
            "test-project",
            "--input_subscription",
            "projects/test-project/subscriptions/audio-in",
            "--output_topic",
            "projects/test-project/topics/out",
            "--dlq_topic",
            "projects/test-project/topics/dlq",
            "--staging_audio_bucket",
            "test-staging-bucket",
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


def test_metadata_json_parameters_parity() -> None:
    """Enforces registration of new programmatic options in metadata.json."""
    parser = argparse.ArgumentParser()
    SegmentationOptions._add_argparse_args(parser)

    # Read arguments from pipeline definition, excluding general plumbing args
    declared_options = {
        action.dest
        for action in parser._actions
        if action.dest
        not in (
            "help",
            "id_label",
            "transcriber_type",
            "transcriber_config",
            "vad_config",
            "significant_gap_ms",
        )
    }

    # Parse template specification
    meta_path = Path(__file__).parent.parent / "metadata.json"
    with open(meta_path) as f:
        metadata = json.load(f)

    metadata_parameters = {param["name"] for param in metadata["parameters"]}

    missing = declared_options - metadata_parameters
    assert not missing, (
        f"Config options defined in options.py missing from metadata.json spec: {missing}"
    )

    stale = metadata_parameters - declared_options
    assert not stale, (
        f"Entries in metadata.json have no corresponding option in options.py (stale?): {stale}"
    )
