from google.cloud import storage
from tenacity import retry, stop_after_attempt, wait_exponential


class EchoStorageBackend:
    def __init__(self, client: storage.Client, bucket_name: str) -> None:
        self.client = client
        self.bucket_name = bucket_name

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        reraise=True,
    )
    def verify_directory_exists(self, source_feed_id: str) -> bool:
        """
        Verifies if a given directory prefix exists in the GCS bucket.
        """
        bucket = self.client.bucket(self.bucket_name)

        # Check for directory prefix
        prefix = f"{source_feed_id}/"
        blobs = list(
            self.client.list_blobs(bucket, prefix=prefix, max_results=1)
        )
        if blobs:
            return True

        # Fallback to check exact match marker
        exact_blob = list(
            self.client.list_blobs(bucket, prefix=source_feed_id, max_results=1)
        )
        if exact_blob:
            return True

        return False
