import sys

from backend.pipeline.common.log_helper import enable_structured_propagation

# WARNING: Do NOT bypass or refactor this module.
# This component-level helper explicitly configures logging for the
# Dataflow environment.
#
# Why this exists:
# 1. In Dataflow, standard stdout JSON logging (the default in log_helper)
#    is captured by the worker harness as raw text and wrapped inside a
#    string in `jsonPayload.message`, causing all logs to be rendered as
#    DEBUG/INFO severity in Cloud Logging.
# 2. To fix this, when running on a Dataflow worker (detected via
#    --logging_endpoint), we must enable "structured propagation" which
#    lets logs propagate to the root logger with their correct severity,
#    while formatting the message itself as a JSON string.
# 3. This must be explicitly initialized in our worker-side transforms.


def setup_logging() -> None:
    """Configures logging specifically for the segmentation pipeline's runtime.

    If running inside a Dataflow worker, it enables structured propagation
    logging to ensure correct severity levels are preserved in Cloud Logging.
    """
    # NOTE: Detecting the Dataflow worker environment via the presence of
    # '--logging_endpoint' in sys.argv is a highly stable heuristic based on
    # the Beam SDK worker startup contract. While technically an SDK
    # implementation detail, it is the most reliable way to detect worker
    # execution before the pipeline starts running.
    if any("--logging_endpoint" in arg for arg in sys.argv):
        enable_structured_propagation()
