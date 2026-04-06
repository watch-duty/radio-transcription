import logging
import subprocess
from pathlib import Path

import numpy as np

from backend.pipeline.transcription.audio import silero_vad
from backend.pipeline.transcription.audio.audio_processor import AudioProcessor
from backend.pipeline.transcription.audio.signal_processing import (
    apply_sliding_agc,
)
from backend.pipeline.transcription.audio.silero_vad import create_sherpa_vad

logger = logging.getLogger(__name__)

def read_audio_ffmpeg(path: str) -> tuple[np.ndarray, int]:
    cmd = [
        "ffmpeg",
        "-i",
        path,
        "-f",
        "s16le",
        "-acodec",
        "pcm_s16le",
        "-ar",
        "16000",
        "-",
    ]
    process = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    stdout, stderr = process.communicate()
    if process.returncode != 0:
        msg = f"ffmpeg failed: {stderr.decode()}"
        raise RuntimeError(msg)
    audio = np.frombuffer(stdout, dtype=np.int16)
    return audio, 16000

def test_debug_audio() -> None:
    path = "backend/pipeline/transcription/tests/integration/fixtures/prior_sample.flac"
    assert Path(path).exists(), f"File not found: {path}"

    audio, sr = read_audio_ffmpeg(path)
    logger.info(f"[INIT] Audio read: len={len(audio)}, RMS={np.sqrt(np.mean(audio.astype(np.float32)**2)):.2f}")

    processor = AudioProcessor(vad_config="{}")
    denoiser = processor._create_sherpa_denoiser()

    # Simulate _detect_speech_candidates steps
    samples_float = audio.astype(np.float32) / 32768.0
    logger.info(f"[FLOAT] After scale: RMS={np.sqrt(np.mean(samples_float**2)):.5f}")

    # Normalize
    samples_float = samples_float / np.max(np.abs(samples_float))
    logger.info(f"[NORM] After normalization: RMS={np.sqrt(np.mean(samples_float**2)):.5f}")

    # Denoise
    denoised_audio_obj = denoiser.run(samples_float.tolist(), sr)
    denoised_float = np.array(denoised_audio_obj.samples, dtype=np.float32)
    logger.info(f"[DENOISE] After denoiser: RMS={np.sqrt(np.mean(denoised_float**2)):.5f}")

    # AGC
    leveled_float = apply_sliding_agc(denoised_float, sample_rate=sr)
    logger.info(f"[AGC] After AGC: RMS={np.sqrt(np.mean(leveled_float**2)):.5f}")

    # Pre-emphasis
    leveled_int16 = (leveled_float * 32767).astype(np.int16)
    samples_rec = processor._apply_pre_emphasis(leveled_int16)
    samples_rec_float = samples_rec.astype(np.float32) / 32768.0
    logger.info(f"[PRE-EMPH] After Pre-emphasis: RMS={np.sqrt(np.mean(samples_rec_float**2)):.5f}")

    # VAD
    vad_sensitive = create_sherpa_vad(threshold=0.01, min_silence_duration=0.6, min_speech_duration=0.05)
    vad_result = silero_vad.process_vad_streaming(samples_rec, 0, vad_sensitive)
    logger.info(f"[VAD] Returned {len(vad_result.speech_segments)} segments")
    for s in vad_result.speech_segments:
        logger.info(f"  -> Segment: {s.speech_start_ms} - {s.speech_end_ms}")
