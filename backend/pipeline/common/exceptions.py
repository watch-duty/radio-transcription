"""Common exceptions for the radio transcription pipeline."""


class AlreadyExistsError(Exception):
    """Raised when a resource already exists."""

    def __init__(self, transmission_id: str) -> None:
        self.transmission_id = transmission_id
        super().__init__(
            f"Transcript for transmission {transmission_id} already exists"
        )
