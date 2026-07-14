"""Common exceptions for the radio transcription pipeline."""


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


class FeedStateConflictError(Exception):
    """Raised when a feed exists but cannot accept the requested state change."""

    def __init__(
        self, feed_id: str, operation: str, current_status: str
    ) -> None:
        self.feed_id = feed_id
        self.operation = operation
        self.current_status = current_status
        super().__init__(
            f"Feed {feed_id} is currently {current_status} and cannot be {operation}"
        )


class NonRetryableError(Exception):
    """Exception raised for non-retryable errors that should not trigger a message retry."""


class InvalidFinishReasonError(NonRetryableError):
    """Raised when a transcription model returns an invalid finish reason (e.g., safety filters)."""


class PartialTranscriptionError(Exception):
    """Raised when a transcription is partial/truncated but still contains valid text."""

    def __init__(self, partial_text: str, reason: str) -> None:
        super().__init__(f"Partial transcription due to: {reason}")
        self.partial_text = partial_text
        self.reason = reason
