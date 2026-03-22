"""Radio Transcription Pipeline Entry Point.

This is the CLI entry point for the transcription pipeline.
It handles argument parsing and environment configuration before launching the Beam orchestration.
"""

import logging
import sys

from apache_beam.options.pipeline_options import PipelineOptions

from backend.pipeline.transcription.options import TranscriptionOptions
from backend.pipeline.transcription.orchestration import get_pipeline

logger = logging.getLogger(__name__)


def main() -> None:
    """Parses CLI arguments and launches the pipeline orchestration."""
    # Parse all arguments via Beam's PipelineOptions to capture custom configs
    pipeline_options = PipelineOptions(sys.argv[1:])
    options = pipeline_options.view_as(TranscriptionOptions)

    logger.info(
        "Starting pipeline: input=%s, output=%s",
        options.input_topic,
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
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    main()
