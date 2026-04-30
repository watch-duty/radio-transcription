import librosa
import soundfile as sf
import os
import logging

logger = logging.getLogger(__name__)

# Gemma recommends using Fourier method for resampling (e.g. scipy.signal.resample or librosa.sample(res_type ='scipy')
# https://ai.google.dev/gemma/docs/capabilities/audio
def preprocess_audio_for_model(input_path: str, output_path: str, target_sr: int = 16000) -> bool:
    """
    Preprocesses audio file to meet typical ML model requirements:
    - Resamples to target_sr (default 16kHz) using the Fourier method.
    - Downmixes to mono.
    - Scales to float32 in range [-1, 1].
    """
    try:
        # Load, resample, mono, and scale all at once
        audio, sr = librosa.load(input_path, sr=target_sr, mono=True, res_type='scipy')
        
        # Save to output path
        sf.write(output_path, audio, target_sr, format='WAV')
        logger.info(f"Preprocessed audio saved to {output_path}")
        return True
    except Exception as e:
        logger.error(f"Failed to preprocess audio {input_path}: {e}")
        return False
