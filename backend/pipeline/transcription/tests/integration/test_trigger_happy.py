import os
import sys
import numpy as np
from pathlib import Path
import subprocess

from backend.pipeline.transcription.audio.audio_processor import AudioProcessor
from backend.pipeline.transcription.audio.audio_processor import create_sherpa_vad

def read_audio_ffmpeg(path: str) -> tuple[np.ndarray, int]:
    """Reads audio using ffmpeg to handle non-zero start times and resample to 16kHz."""
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

def test_files():
    chunks_dir = "backend/pipeline/transcription/tests/integration/fixtures"
    flac_files = list(Path(chunks_dir).glob("chunk_*.flac"))
    
    print(f"Found {len(flac_files)} FLAC files.")
    
    for f in flac_files:
        print(f"\nProcessing {f.name}...")
        processor = AudioProcessor(vad_config="{}")
        processor.vad_sensitive = create_sherpa_vad(
            threshold=0.01, min_silence_duration=0.6, min_speech_duration=0.1
        )
        processor.denoiser = processor._create_sherpa_denoiser()
        
        try:
            audio, _ = read_audio_ffmpeg(str(f))
            padded_segments, _ = processor.detect_vad(audio, start_ms=0)
            print(f"{f.name}: Detected {len(padded_segments)} segments.")
            for seg in padded_segments:
                print(f"  -> {seg.speech_start_ms}ms - {seg.speech_end_ms}ms")
        except Exception as e:
            print(f"{f.name}: Error: {e}")

if __name__ == "__main__":
    test_files()
