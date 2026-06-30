"""Teardown script for the Echo pipeline integration test."""

import sys

from google.cloud import storage

from regression_tests.constants_util import PipelineConstants
from regression_tests.setup_echo_util import (
    _delete_existing_feed_by_channel_name,
    _delete_existing_rule_by_name,
)


def _delete_echo_audio(bucket: storage.Bucket, channel_name: str) -> None:
    """Delete all audio blobs under the channel prefix."""
    sys.stdout.write(
        f"Cleaning up blobs in gs://{bucket.name}/{channel_name}/...\n"
    )
    try:
        blobs = list(bucket.list_blobs(prefix=f"{channel_name}/"))
        if not blobs:
            sys.stdout.write("No blobs found to delete.\n")
            return

        sys.stdout.write(
            f"Deleting {len(blobs)} blobs under prefix '{channel_name}/':\n"
        )
        for blob in blobs:
            sys.stdout.write(f"  - gs://{bucket.name}/{blob.name}\n")
            blob.delete()
        sys.stdout.write("Blobs deleted successfully.\n")
    except Exception as e:
        sys.stdout.write(f"WARNING: failed to delete test audio blobs: {e}\n")


def test_teardown_backend_pipeline(
    id_token: str,
    fe_proxy_api_url: str,
    gcp_echo_bucket: str,
) -> None:
    """Clean Echo regression resources by name and prefix."""
    sys.stdout.write(
        "Starting teardown for Echo feed "
        f"'{PipelineConstants.ECHO_CHANNEL_NAME}'...\n"
    )

    # Initialize GCS storage client and bucket
    storage_client = storage.Client()
    bucket = storage_client.bucket(gcp_echo_bucket)

    _delete_existing_rule_by_name(
        fe_proxy_api_url, id_token, PipelineConstants.RULE_NAME
    )
    _delete_existing_feed_by_channel_name(
        fe_proxy_api_url, id_token, PipelineConstants.ECHO_CHANNEL_NAME
    )
    _delete_echo_audio(bucket, PipelineConstants.ECHO_CHANNEL_NAME)

    sys.stdout.write("Teardown completed successfully.\n")
