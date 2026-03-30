from __future__ import annotations

import asyncio

from google.cloud import pubsub_v1


class PubSubClient:
    """Lazily initialized Google Cloud Pub/Sub publisher client."""

    def __init__(self) -> None:
        self._publisher: pubsub_v1.PublisherClient | None = None

    @staticmethod
    def get_topic_path(
        publisher: pubsub_v1.PublisherClient,
        project_id: str,
        topic_id_or_path: str,
    ) -> str:
        """Resolve a topic name or path to a full topic path."""
        if topic_id_or_path.startswith("projects/"):
            return topic_id_or_path
        return publisher.topic_path(project_id, topic_id_or_path)

    def get_publisher(self) -> pubsub_v1.PublisherClient:
        """Return a shared Pub/Sub publisher client, creating one lazily."""
        if self._publisher is None:
            publisher_options = pubsub_v1.types.PublisherOptions(
                enable_message_ordering=True,
            )
            self._publisher = pubsub_v1.PublisherClient(
                publisher_options=publisher_options,
            )
        return self._publisher

    async def close(self) -> None:
        """Close the shared Pub/Sub publisher client if initialized."""
        if self._publisher is not None:
            publisher = self._publisher
            self._publisher = None
            await asyncio.to_thread(publisher.stop)
