import logging
import os
import sys
import time

import requests

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

STORAGE_EMULATOR_HOST = os.environ["STORAGE_EMULATOR_HOST"]
PROJECT_ID = os.environ["GOOGLE_CLOUD_PROJECT"]


def wait_for_gcs() -> None:
    logger.info("Waiting for Fake GCS server...")
    for _ in range(30):
        try:
            response = requests.get(
                f"{STORAGE_EMULATOR_HOST}/storage/v1/b?project={PROJECT_ID}",
                timeout=10,
            )
            if response.status_code in (200, 404):
                logger.info("Fake GCS server is ready.")
                return
        except requests.exceptions.RequestException:
            pass
        time.sleep(1)
    logger.error("Timed out waiting for Fake GCS server.")
    sys.exit(1)


def create_bucket(bucket_name: str) -> None:
    url = f"{STORAGE_EMULATOR_HOST}/storage/v1/b?project={PROJECT_ID}"
    payload = {"name": bucket_name}
    response = requests.post(url, json=payload, timeout=10)
    if response.status_code == 200:
        logger.info("Bucket '%s' created.", bucket_name)
    elif response.status_code == 409:
        logger.info("Bucket '%s' already exists.", bucket_name)
    else:
        logger.error(
            "Failed to create bucket '%s': %s", bucket_name, response.text
        )


def upload_file(bucket_name: str, local_path: str, remote_path: str) -> None:
    url = f"{STORAGE_EMULATOR_HOST}/upload/storage/v1/b/{bucket_name}/o?uploadType=media&name={remote_path}"
    try:
        with open(local_path, "rb") as f:
            data = f.read()
        response = requests.post(
            url, data=data, headers={"Content-Type": "audio/flac"}, timeout=10
        )
        if response.status_code == 200:
            logger.info(
                "Uploaded '%s' to '%s' in bucket '%s'.",
                local_path,
                remote_path,
                bucket_name,
            )
        else:
            logger.error("Failed to upload '%s': %s", local_path, response.text)
    except Exception:
        logger.exception("Error uploading '%s'", local_path)


if __name__ == "__main__":
    wait_for_gcs()
    # Create audio buckets
    staging_bucket = os.environ["AUDIO_STAGING_BUCKET"]
    create_bucket(staging_bucket)
    create_bucket(os.environ["AUDIO_CANONICAL_BUCKET"])

    # Upload test files
    local_file = "/app/data/test_continuous.flac"
    upload_file(staging_bucket, local_file, "test_continuous.flac")

    local_segmented_file = "/app/data/test_segmented.flac"
    upload_file(staging_bucket, local_segmented_file, "test_segmented.flac")
