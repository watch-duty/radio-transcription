from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar

from backend.pipeline.detection.sound_event_signal_combiner import (
    SoundEventSignalCombiner,
)

if TYPE_CHECKING:
    from backend.pipeline.detection.protocol import SoundEventDetector


class DetectorFactory:
    """
    Registry and ensemble builder for SoundEventDetector implementations.

    ``_registry`` is a class-level dict shared across the container
    lifetime.  Detectors register once at startup; ``create_ensemble``
    reads the registry to build per-feed detector sets from JSON config.

    """

    _registry: ClassVar[dict[str, type[SoundEventDetector]]] = {}

    @classmethod
    def register(
        cls,
        detector_type: str,
        detector_class: type[SoundEventDetector],
    ) -> None:
        """
        Register a detector implementation under a unique type string.

        Raises ``ValueError`` if *detector_type* is already registered.
        """
        if detector_type in cls._registry:
            msg = f"Detector type {detector_type!r} is already registered"
            raise ValueError(msg)
        cls._registry[detector_type] = detector_class

    @classmethod
    def unregister(cls, detector_type: str) -> None:
        """Remove a registration.  Silently ignores unknown types."""
        cls._registry.pop(detector_type, None)

    @classmethod
    def create_ensemble(
        cls,
        config: dict[str, Any],
    ) -> tuple[list[SoundEventDetector], SoundEventSignalCombiner]:
        """
        Instantiate detectors and combiner from a feed's config dict.

        Config format::

            {
                "detectors": [
                    {"type": "silero_vad", "threshold": 0.4, ...},
                    {"type": "energy_squelch", "mode": "rms", ...}
                ]
            }

        For each entry in ``"detectors"``:

        1. Look up the class by the ``"type"`` field in the registry.
        2. Strip the ``"type"`` field from a **copy** of the entry.
        3. Pass all remaining fields as ``**kwargs`` to the constructor.

        Returns a ``(detectors, combiner)`` tuple.

        Raises:
            KeyError: if a detector type is not registered.
            TypeError: if kwargs don't match the constructor.

        """
        combiner = SoundEventSignalCombiner()

        detector_configs: list[dict[str, Any]] = config.get("detectors", [])
        detectors: list[SoundEventDetector] = []

        for entry in detector_configs:
            entry_copy = {**entry}
            dtype = entry_copy.pop("type", None)
            if dtype is None:
                msg = "Detector config entry missing required 'type' key"
                raise KeyError(msg)
            if dtype not in cls._registry:
                msg = f"Unknown detector type: {dtype!r}"
                raise KeyError(msg)
            detector_class = cls._registry[dtype]
            detectors.append(detector_class(**entry_copy))

        return detectors, combiner
