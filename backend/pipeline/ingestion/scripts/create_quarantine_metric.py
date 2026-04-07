"""One-time setup: register the quarantine_events metric descriptor.

Usage::

    python -m backend.pipeline.ingestion.scripts.create_quarantine_metric \
        --project-id=my-gcp-project

Runs once per GCP project, not per deploy.  Cloud Monitoring
auto-creates descriptors on first ``create_time_series`` call, so this
script is optional — but it ensures labels and descriptions are
registered before the first quarantine event.
"""

from __future__ import annotations

import argparse

from google.api import label_pb2, metric_pb2
from google.api_core.exceptions import AlreadyExists
from google.cloud import monitoring_v3

_METRIC_TYPE = "custom.googleapis.com/feeds/quarantine_events"


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Create the quarantine_events metric descriptor.",
    )
    parser.add_argument(
        "--project-id",
        required=True,
        help="GCP project ID (e.g. my-gcp-project)",
    )
    args = parser.parse_args()

    descriptor = metric_pb2.MetricDescriptor(
        type=_METRIC_TYPE,
        metric_kind=metric_pb2.MetricDescriptor.MetricKind.GAUGE,
        value_type=metric_pb2.MetricDescriptor.ValueType.INT64,
        description="Emitted when a feed transitions to quarantined status.",
        labels=[
            label_pb2.LabelDescriptor(
                key="feed_id",
                value_type=label_pb2.LabelDescriptor.ValueType.STRING,
                description="UUID of the quarantined feed.",
            ),
            label_pb2.LabelDescriptor(
                key="feed_name",
                value_type=label_pb2.LabelDescriptor.ValueType.STRING,
                description="Human-readable name of the feed.",
            ),
            label_pb2.LabelDescriptor(
                key="source_type",
                value_type=label_pb2.LabelDescriptor.ValueType.STRING,
                description="Feed source type slug (e.g. bcfy_feeds).",
            ),
        ],
    )

    client = monitoring_v3.MetricServiceClient()
    project_name = f"projects/{args.project_id}"

    try:
        client.create_metric_descriptor(
            name=project_name,
            metric_descriptor=descriptor,
        )
        print(f"Created metric descriptor: {_METRIC_TYPE}")  # noqa: T201
    except AlreadyExists:
        print(f"Metric descriptor already exists: {_METRIC_TYPE}")  # noqa: T201


if __name__ == "__main__":
    main()
