"""Common exceptions for the radio transcription pipeline."""


class AlreadyExistsError(Exception):
    """Raised when a resource already exists."""

    def __init__(self, segment_id: str) -> None:
        self.segment_id = segment_id
        super().__init__(f"Transcript for segment {segment_id} already exists")


class FeedAlreadyExistsError(Exception):
    """Raised when a feed with the same source feed ID and source type already exists."""

    def __init__(self, source_type: str, source_feed_id: str) -> None:
        self.source_type = source_type
        self.source_feed_id = source_feed_id
        super().__init__(
            f"Feed with source type '{source_type}' and source feed ID '{source_feed_id}' already exists"
        )


class FeedNameAlreadyExistsError(Exception):
    """Raised when a feed with the same name already exists."""

    def __init__(self, name: str) -> None:
        self.name = name
        super().__init__(f"Feed with name '{name}' already exists")


class NonRetryableError(Exception):
    """Exception raised for non-retryable errors that should not trigger a message retry."""
