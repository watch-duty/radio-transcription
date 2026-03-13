from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    import numpy as np

    from backend.pipeline.detection.types import DetectionResult


@runtime_checkable
class SoundEventDetector(Protocol):
    """
    Structural interface for a streaming sound event detector.

    Every concrete detector (Silero VAD, energy squelch, CTCSS tone
    detector, etc.) must satisfy this protocol.  The protocol is
    structural — implementations do **not** inherit from this class;
    they simply provide the required properties and methods.

    Design contracts
    ----------------
    * **Self-buffering:** ``feed()`` accepts arbitrary-length sample
      arrays.  The detector accumulates samples internally and runs
      its evaluation logic once enough have been buffered (e.g. 512
      samples for Silero VAD, 320 for energy squelch).
    * **Error-isolated:** ``feed()`` **must not raise**.  On internal
      errors the detector sets ``is_healthy`` to ``False`` and
      becomes a no-op until ``reset()`` is called.
    * **Resettable:** ``reset()`` clears all internal state — sample
      buffers, LSTM hidden states, noise-floor estimates, hangover
      timers — so the detector can be reused across audio windows.
    * **Context-independent:** Implementations must not perform I/O,
      use ``asyncio``, or depend on external state.  They are pure
      computation over PCM samples.

    Properties
    ----------
    detector_type
        Unique string identifier for this detector kind
        (e.g. ``"silero_vad"``, ``"energy_squelch"``).  Serves triple
        duty as the factory registration key, the combiner tracking
        key, and the sidecar provenance field.
    sample_rate
        Expected input sample rate in Hz (16 000 or 8 000).
    is_healthy
        ``True`` while the detector is operational.  Set to ``False``
        internally on persistent errors.

    Methods
    -------
    feed(samples)
        Accept int16 PCM samples at ``sample_rate``.  The audio
        pipeline stores segments as FLAC (a lossless *integer* codec),
        so samples arrive as 16-bit signed integers — no float
        conversion is needed.
    pop_results()
        Drain and return all ``DetectionResult`` objects produced
        since the last call.  Returns an empty list when nothing is
        ready.
    reset()
        Clear all internal state between audio windows.

    """

    @property
    def detector_type(self) -> str: ...

    @property
    def sample_rate(self) -> int: ...

    @property
    def is_healthy(self) -> bool: ...

    def feed(self, samples: np.ndarray) -> None: ...

    def pop_results(self) -> list[DetectionResult]: ...

    def reset(self) -> None: ...
