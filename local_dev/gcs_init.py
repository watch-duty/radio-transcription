import logging
import os
import sys

from google.cloud import storage

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def init_gcs():
    # The storage.Client() automatically respects STORAGE_EMULATOR_HOST if set
    client = storage.Client()

    staging_bucket_name = os.environ["AUDIO_STAGING_BUCKET"]
    canonical_bucket_name = os.environ["AUDIO_CANONICAL_BUCKET"]

    # Create buckets if they don't exist
    for name in [staging_bucket_name, canonical_bucket_name]:
        bucket = client.bucket(name)
        try:
            if not bucket.exists():
                client.create_bucket(name)
                logger.info(f"Bucket '{name}' created.")
            else:
                logger.info(f"Bucket '{name}' already exists.")
        except Exception as e:
            logger.error(f"Failed to check/create bucket '{name}': {e}")
            sys.exit(1)

    # Upload test files
    staging_bucket = client.bucket(staging_bucket_name)

    files_to_upload = [
        ("test_bcfy.flac", "/app/data/test_bcfy.flac"),
        ("test_dispatch_amador.flac", "/app/data/test_dispatch_amador.flac"),
    ]

    for remote_name, local_path in files_to_upload:
        blob = staging_bucket.blob(remote_name)
        try:
            blob.upload_from_filename(local_path)
            logger.info(
                f"Uploaded '{local_path}' to '{remote_name}' in bucket '{staging_bucket_name}'."
            )
        except Exception as e:
            logger.error(f"Failed to upload '{local_path}': {e}")
            sys.exit(1)


if __name__ == "__main__":
    init_gcs()
