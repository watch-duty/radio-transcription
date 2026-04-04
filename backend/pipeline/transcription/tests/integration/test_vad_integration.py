import os
import io
import pathlib
import numpy as np
import pytest
import soundfile as sf
import subprocess
import tempfile

from backend.pipeline.transcription.audio.audio_processor import AudioProcessor
from backend.pipeline.transcription.audio.silero_vad import create_sherpa_vad


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
        raise RuntimeError(f"ffmpeg failed: {stderr.decode()}")

    audio = np.frombuffer(stdout, dtype=np.int16)
    return audio, 16000


def test_vad_integration():
    """Integration test to verify VAD and Denoiser work without mocks."""
    # 1. Create a dummy audio chunk with a tone in the middle
    # 2 seconds of silence, 1 second of 440Hz tone, 2 seconds of silence
    # Total 5 seconds.
    # VAD expects 16kHz
    silence = np.zeros(2000 * 16, dtype=np.int16)

    t = np.linspace(0, 1, 16000, endpoint=False)
    sine = (np.sin(2 * np.pi * 440 * t) * 32767).astype(np.int16)

    audio = np.concatenate([silence, sine, silence])

    # 2. Initialize AudioProcessor
    processor = AudioProcessor(vad_config="{}")

    processor.vad_sensitive = create_sherpa_vad(
        threshold=0.15, min_silence_duration=0.6, min_speech_duration=0.25
    )
    processor.vad_strict = create_sherpa_vad(
        threshold=0.25, min_silence_duration=0.25, min_speech_duration=0.05
    )
    processor.denoiser = processor._create_sherpa_denoiser()

    # 4. Run detect_vad
    # Start time is 1000ms arbitrarily
    start_ms = 1000
    padded_segments = processor.detect_vad(audio, start_ms=start_ms)

    # 5. Assertions
    # We expect it to run without crashing.
    assert isinstance(padded_segments, list)

    # Print the result to see if VAD detected anything.
    print(f"\nDetected segments: {padded_segments}")
    for seg in padded_segments:
        print(f"Segment: {seg.start_ms}ms - {seg.speech_end_ms}ms")


def test_vad_with_real_audio():
    """Integration test to verify VAD works with real audio."""
    # 1. Load real audio from fixtures
    fixture_path1 = (
        "backend/pipeline/transcription/tests/fixtures/prior_sample.flac"
    )
    fixture_path2 = "backend/pipeline/transcription/tests/fixtures/sample.flac"
    assert os.path.exists(fixture_path1), f"Fixture not found: {fixture_path1}"
    assert os.path.exists(fixture_path2), f"Fixture not found: {fixture_path2}"

    audio1, sr1 = read_audio_ffmpeg(fixture_path1)
    audio2, sr2 = read_audio_ffmpeg(fixture_path2)

    print(f"\nAudio1 duration: {len(audio1) // 16}ms")
    print(f"Audio2 duration: {len(audio2) // 16}ms")

    # Concatenate them
    audio = np.concatenate([audio1, audio2])
    print(f"Combined audio duration: {len(audio) // 16}ms")

    # 2. Initialize AudioProcessor
    processor = AudioProcessor(vad_config="{}")

    processor.vad_sensitive = create_sherpa_vad(
        threshold=0.01, min_silence_duration=0.6, min_speech_duration=0.05
    )
    processor.vad_strict = create_sherpa_vad(
        threshold=0.01, min_silence_duration=0.25, min_speech_duration=0.05
    )
    processor.denoiser = processor._create_sherpa_denoiser()

    # 4. Run detect_vad in two steps to simulate streaming
    padded_segments1 = processor.detect_vad(audio1, start_ms=0)
    padded_segments2 = processor.detect_vad(audio2, start_ms=len(audio1) // 16)

    all_segments = padded_segments1 + padded_segments2

    # 5. Assertions
    assert isinstance(all_segments, list)
    assert len(all_segments) > 0, "Expected to find speech segments"
    print(f"Total speech segments found: {len(all_segments)}")

    print(f"\nReal audio detected segments: {all_segments}")
    for seg in all_segments:
        print(f"Segment: {seg.speech_start_ms}ms - {seg.speech_end_ms}ms")

    # 6. Integration Test for M4A round-trip
    if all_segments:
        seg = all_segments[0]
        # Extract audio from combined audio
        start_sample = seg.start_ms * 16
        end_sample = start_sample + len(seg.audio)
        seg_audio = audio[start_sample:end_sample]

        # Export to M4A
        m4a_bytes = processor.export_m4a(seg_audio)
        assert len(m4a_bytes) > 0, "Failed to export M4A"

        # Load back to verify
        with tempfile.NamedTemporaryFile(
            suffix=".m4a", delete=False
        ) as temp_file:
            temp_file.write(m4a_bytes)
            temp_path = temp_file.name

        try:
            # Use soundfile to read back if it supports it, or ffmpeg!
            # Since soundfile doesn't support M4A read on all platforms, we use ffmpeg to convert back to WAV!
            wav_path = temp_path.replace(".m4a", ".wav")
            subprocess.run(
                ["ffmpeg", "-y", "-i", temp_path, wav_path],
                check=True,
                capture_output=True,
            )

            loaded_audio, loaded_sr = sf.read(wav_path, dtype="int16")
            assert loaded_sr == 16000, f"Expected 16kHz, got {loaded_sr}"

            # Verify duration is roughly same
            orig_duration = len(seg_audio) / 16000
            loaded_duration = len(loaded_audio) / 16000
            assert abs(orig_duration - loaded_duration) < 0.2, (
                f"Duration mismatch: {orig_duration} != {loaded_duration}"
            )

            print(
                f"Successfully verified M4A round-trip for segment. Orig duration: {orig_duration:.2f}s, Loaded: {loaded_duration:.2f}s"
            )

            if os.path.exists(wav_path):
                os.remove(wav_path)
        finally:
            if os.path.exists(temp_path):
                os.remove(temp_path)
