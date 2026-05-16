"""Audio preprocessing utilities for ASR model input (behind [audio] extra).

Provides ``preprocess_audio_for_model`` (resample + mono downmix) and
``load_audio_segment`` (offset+duration frame-slice). Both require the ``[audio]``
extra (torchaudio + torch).

Importing this module WITHOUT the extra is safe — heavy deps are deferred so that
``import common.audio_utils`` never triggers torch/torchaudio when ``[audio]`` is not
installed (Pitfall 8).
"""

import logging
import os

logger = logging.getLogger(__name__)

try:
    import torch
    import torchaudio
    import torchaudio.transforms as T
except ImportError as _e:
    _AUDIO_MISSING = _e
else:
    _AUDIO_MISSING = None


def _require_audio() -> None:
    """Raise a clear error if the [audio] extra is not installed."""
    if _AUDIO_MISSING:
        raise ImportError(
            "audio_utils requires the [audio] extra: pip install 'common[audio]'"
        ) from _AUDIO_MISSING


def preprocess_audio_for_model(
    input_path: str, output_path: str, target_sr: int = 16000
) -> bool:
    """Preprocess audio file: resample to target_sr and downmix to mono.

    Behavior-preserved from the original audio_utils.py. The signature is kept
    exactly so all existing callers (notebooks, inference_hf.py) continue to work
    without modification (Pitfall 7).

    Args:
        input_path: Path to input audio file.
        output_path: Path to write preprocessed audio.
        target_sr: Target sample rate in Hz. Defaults to 16000.

    Returns:
        True on success, False on error.

    Raises:
        ImportError: If the ``[audio]`` extra is not installed.
    """
    _require_audio()
    try:
        waveform, sr = torchaudio.load(input_path)

        # Downmix to mono by averaging across channels
        if waveform.shape[0] > 1:
            waveform = torch.mean(waveform, dim=0, keepdim=True)

        # Resample if necessary
        if sr != target_sr:
            resampler = T.Resample(orig_freq=sr, new_freq=target_sr)
            waveform = resampler(waveform)

        torchaudio.save(output_path, waveform, target_sr)
        logger.info(f"Preprocessed audio saved to {output_path}")
        return True
    except Exception as e:
        logger.error(f"Failed to preprocess audio {input_path}: {e}")
        return False


def load_audio_segment(
    path: str,
    offset: float = 0.0,
    duration: "float | None" = None,
    target_sr: int = 16000,
) -> "tuple[torch.Tensor, int]":
    """Load an audio segment by offset + duration using torchaudio frame slicing.

    Needed by the Phase 3 ``hf_dataset`` adapter for per-row audio extraction from
    longer audio files. Uses ``torchaudio.info`` + ``torchaudio.load`` with
    ``frame_offset`` / ``num_frames`` to avoid loading the full file into memory.

    Args:
        path: Local filesystem path to the audio file.
        offset: Start time in seconds. Defaults to 0.0.
        duration: Duration in seconds to load. If None, loads to end of file.
        target_sr: Target sample rate in Hz. Defaults to 16000.

    Returns:
        Tuple of ``(waveform, target_sr)`` where waveform is a mono ``torch.Tensor``
        of shape ``(1, num_frames)``.

    Raises:
        ImportError: If the ``[audio]`` extra is not installed.
    """
    _require_audio()
    info = torchaudio.info(path)
    native_sr = info.sample_rate
    frame_offset = int(offset * native_sr)
    num_frames = int(duration * native_sr) if duration is not None else -1
    waveform, sr = torchaudio.load(
        path, frame_offset=frame_offset, num_frames=num_frames
    )
    if waveform.shape[0] > 1:
        waveform = torch.mean(waveform, dim=0, keepdim=True)
    if sr != target_sr:
        waveform = T.Resample(orig_freq=sr, new_freq=target_sr)(waveform)
    return waveform, target_sr
