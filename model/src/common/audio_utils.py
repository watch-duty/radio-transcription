"""Audio preprocessing utilities for ASR model input (behind [audio] extra).

Provides ``preprocess_audio_for_model`` (resample + mono downmix). Requires
the ``[audio]`` extra (torchaudio + torch).

Importing this module WITHOUT the extra is safe — heavy deps are deferred so that
``import common.audio_utils`` never triggers torch/torchaudio when ``[audio]`` is not
installed.
"""

import logging

logger = logging.getLogger(__name__)

try:
    import torch
    import torchaudio
    from torchaudio import transforms
except ImportError as _e:
    _AUDIO_MISSING = _e
else:
    _AUDIO_MISSING = None


def _require_audio() -> None:
    """Raise a clear error if the [audio] extra is not installed."""
    if _AUDIO_MISSING:
        msg = "audio_utils requires the [audio] extra: pip install 'common[audio]'"
        raise ImportError(msg) from _AUDIO_MISSING


def preprocess_audio_for_model(
    input_path: str, output_path: str, target_sr: int = 16000
) -> bool:
    """Preprocess audio file: resample to target_sr and downmix to mono.

    Behavior-preserved from the original audio_utils.py. The signature is kept
    exactly so all existing callers (notebooks, inference_hf.py) continue to work
    without modification.

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
            resampler = transforms.Resample(orig_freq=sr, new_freq=target_sr)
            waveform = resampler(waveform)

        torchaudio.save(output_path, waveform, target_sr)
        logger.info(f"Preprocessed audio saved to {output_path}")
    except Exception as e:
        logger.exception(f"Failed to preprocess audio {input_path}: {e}")
        return False
    else:
        return True
