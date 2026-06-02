"""Radio Transcription Normalization Pipeline Entry Point.

This is the CLI entry point for the normalization pipeline.
It handles argument parsing and environment configuration before launching the Beam orchestration.
"""

import sys

from apache_beam.options.pipeline_options import PipelineOptions

from backend.pipeline.common.log_helper import get_task_logger
from backend.pipeline.segmentation.options import TranscriptionOptions
from backend.pipeline.segmentation.orchestration import get_pipeline

logger = get_task_logger(
    __name__, {"system": "transcription", "component": "main"}
)


def main() -> None:
    """Parses CLI arguments and launches the pipeline orchestration."""
    # Parse all arguments via Beam's PipelineOptions to capture custom configs
    pipeline_options = PipelineOptions(sys.argv[1:])
    options = pipeline_options.view_as(TranscriptionOptions)

    logger.info(
        "Starting segmentation pipeline: continuous_input=%s, output=%s",
        options.continuous_input_subscription,
        options.output_topic,
    )

    try:
        pipeline = get_pipeline(
            pipeline_options=pipeline_options,
        )
        # Execute the pipeline and wait for completion
        result = pipeline.run()
        # For a streaming pipeline, this usually blocks indefinitely
        result.wait_until_finish()
    except Exception:
        logger.exception("Failed to run pipeline")
        sys.exit(1)


if __name__ == "__main__":
    # Note: Do *not* add backend.pipeline.common.log_helper.setup_logging() here. It is handled by the Dataflow runner.
    main()
