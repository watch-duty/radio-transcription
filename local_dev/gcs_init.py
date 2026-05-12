"""Initializes the Fake GCS server with required buckets and test files."""

import logging
import os
import sys
import time

from google.cloud import storage

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def wait_for_gcs() -> None:
    """Waits for the Fake GCS server to become ready."""
    logger.info("Waiting for Fake GCS server...")
    client = storage.Client()

    for _ in range(30):
        try:
            list(client.list_buckets())

        except Exception as e:
            # Failures expected if Fake GCS server isn't ready yet.
            logger.info(f"Fake GCS server not ready yet: {e}")
            time.sleep(1)
        else:
            logger.info("Fake GCS server is ready.")
            return

    logger.error("Timed out waiting for Fake GCS server.")
    sys.exit(1)


def create_bucket(client: storage.Client, bucket_name: str) -> None:
    """Creates a bucket if it does not exist."""
    try:
        client.create_bucket(bucket_name)
        logger.info(f"Bucket '{bucket_name}' created.")
    except Exception:
        logger.exception(f"Failed to check/create bucket '{bucket_name}'")


def upload_file(
    client: storage.Client, bucket_name: str, local_path: str, remote_name: str
) -> None:
    """Uploads a file to the specified bucket."""
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(remote_name)
    try:
        blob.upload_from_filename(local_path)
        logger.info(
            f"Uploaded '{local_path}' to '{remote_name}' in bucket '{bucket_name}'."
        )
    except Exception:
        logger.exception(f"Failed to upload '{local_path}'")


if __name__ == "__main__":
    wait_for_gcs()

    client = storage.Client()

    staging_bucket = os.environ["AUDIO_STAGING_BUCKET"]
    canonical_bucket = os.environ["AUDIO_CANONICAL_BUCKET"]

    create_bucket(client, staging_bucket)
    create_bucket(client, canonical_bucket)

    upload_file(
        client, staging_bucket, "/app/data/test_bcfy.flac", "test_bcfy.flac"
    )
    upload_file(
        client,
        staging_bucket,
        "/app/data/test_dispatch_amador.flac",
        "test_dispatch_amador.flac",
    )

    logger.info("GCS initialization complete.")
