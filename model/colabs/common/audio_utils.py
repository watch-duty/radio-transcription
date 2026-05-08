import torchaudio
import torchaudio.transforms as T
import torch
import os
import logging

logger = logging.getLogger(__name__)

def preprocess_audio_for_model(input_path: str, output_path: str, target_sr: int = 16000) -> bool:
    """
    Preprocesses audio file to meet typical ML model requirements:
    - Resamples to target_sr (default 16kHz).
    - Downmixes to mono.
    """
    try:
        # Load audio using torchaudio
        waveform, sr = torchaudio.load(input_path)
        
        # Downmix to mono by averaging across channels
        if waveform.shape[0] > 1:
            waveform = torch.mean(waveform, dim=0, keepdim=True)
            
        # Resample if necessary
        if sr != target_sr:
            resampler = T.Resample(orig_freq=sr, new_freq=target_sr)
            waveform = resampler(waveform)
            
        # Save to output path
        torchaudio.save(output_path, waveform, target_sr)
        logger.info(f"Preprocessed audio saved to {output_path}")
        return True
    except Exception as e:
        logger.error(f"Failed to preprocess audio {input_path}: {e}")
        return False
