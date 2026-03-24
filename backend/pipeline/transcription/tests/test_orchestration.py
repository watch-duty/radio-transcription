from backend.pipeline.transcription.options import TranscriptionOptions
from backend.pipeline.transcription.orchestration import get_pipeline


def test_pipeline_topology_typehints() -> None:
    """Builds the DAG to trigger Apache Beam's static type checker instantaneously."""
    options = TranscriptionOptions(
        flags=[
            "--project_id",
            "test-project",
            "--input_topic",
            "projects/test-project/topics/in",
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
