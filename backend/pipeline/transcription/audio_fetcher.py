from abc import ABC, abstractmethod

from google.cloud import storage


class AudioFetcher(ABC):
    """Abstract base class for audio fetchers."""

    @abstractmethod
    def fetch(self, uri: str) -> bytes:
        """Fetches audio data from a given URI."""


class GCSAudioFetcher(AudioFetcher):
    """Fetches audio data from Google Cloud Storage."""

    def __init__(self, storage_client: storage.Client | None = None) -> None:
        self.storage_client = storage_client or storage.Client()

    def fetch(self, uri: str) -> bytes:
        """
        Fetches audio data from a GCS URI.

        Args:
            uri: The GCS URI of the audio file (e.g., gs://bucket/object).

        Returns:
            The audio data as bytes.

        """
        blob = storage.Blob.from_string(uri, client=self.storage_client)
        return blob.download_as_bytes()
