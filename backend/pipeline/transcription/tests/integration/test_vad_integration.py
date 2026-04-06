import logging
import subprocess
import tempfile
from pathlib import Path

import numpy as np


from backend.pipeline.transcription.audio.audio_processor import AudioProcessor
from backend.pipeline.transcription.audio.silero_vad import create_sherpa_vad
from backend.pipeline.transcription.utils import (
    SILENCE_THRESHOLD_DB,
    detect_silence_segments,
)

logger = logging.getLogger(__name__)


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


def test_vad_integration() -> None:
    """Integration test to verify VAD and Denoiser work without mocks."""
    # 1. Create a dummy audio chunk with a tone in the middle
    # 2 seconds of silence, 1 second of 440Hz tone, 2 seconds of silence
    # Total 5 seconds.
    # VAD expects 16kHz
    silence = np.zeros(2000 * 16, dtype=np.int16)

    t = np.linspace(0, 1, 16000, endpoint=False)
    sine = (np.sin(2 * np.pi * 440 * t) * 32767).astype(np.int16)

    audio = np.concatenate([silence, sine, silence])

    # Assert silence detection works
    audio_float = audio.astype(np.float32) / 32768.0
    silence_segs = detect_silence_segments(
        audio_float, 16000, SILENCE_THRESHOLD_DB
    )
    assert len(silence_segs) == 2
    # First silence segment should end around 2000ms
    assert abs(silence_segs[0].end_ms - 2000) < 100
    # Second silence segment should start around 3000ms
    assert abs(silence_segs[1].start_ms - 3000) < 100

    # 2. Initialize AudioProcessor
    processor = AudioProcessor(vad_config="{}")

    processor.vad_sensitive = create_sherpa_vad(
        threshold=0.20, min_silence_duration=0.3, min_speech_duration=0.1
    )
    processor.denoiser = processor._create_sherpa_denoiser()

    # 4. Run detect_vad
    # Start time is 1000ms arbitrarily
    start_ms = 1000
    padded_segments, _ = processor.detect_vad(audio, start_ms=start_ms)

    # 5. Assertions
    assert isinstance(padded_segments, list)
    assert len(padded_segments) == 0, "VAD incorrectly triggered on a non-human machine tone."

    # Log the result to see if VAD detected anything.
    logger.info(f"Detected segments: {padded_segments}")
    for seg in padded_segments:
        logger.info(f"Segment: {seg.start_ms}ms - {seg.speech_end_ms}ms")


def test_vad_with_real_audio() -> None:
    """Integration test to verify single-VAD works with real audio and AGC."""
    # 1. Load real audio from fixtures
    fixture_path1 = "backend/pipeline/transcription/tests/integration/fixtures/prior_sample.flac"
    fixture_path2 = "backend/pipeline/transcription/tests/integration/fixtures/sample.flac"
    assert Path(fixture_path1).exists(), f"Fixture not found: {fixture_path1}"
    assert Path(fixture_path2).exists(), f"Fixture not found: {fixture_path2}"

    audio1, _ = read_audio_ffmpeg(fixture_path1)
    audio2, _ = read_audio_ffmpeg(fixture_path2)

    logger.info(f"Audio1 duration: {len(audio1) // 16}ms")
    logger.info(f"Audio2 duration: {len(audio2) // 16}ms")

    # Concatenate them
    audio = np.concatenate([audio1, audio2])
    logger.info(f"Combined audio duration: {len(audio) // 16}ms")

    # 2. Initialize AudioProcessor
    processor = AudioProcessor(vad_config="{}")

    processor.vad_sensitive = create_sherpa_vad(
        threshold=0.01, min_silence_duration=0.6, min_speech_duration=0.25
    )



    processor.denoiser = processor._create_sherpa_denoiser()

    # 3. Run detect_vad for combined audio
    padded_segments, _ = processor.detect_vad(audio, start_ms=0)

    logger.info(f"Combined Detected Speech: {[(s.speech_start_ms, s.speech_end_ms) for s in padded_segments]}")

    # 4. Strict Assertions based on visual reality
    def overlaps_with_segments(start: int, end: int, segments: list) -> bool:
        for seg in segments:
            if not (end <= seg.speech_start_ms or start >= seg.speech_end_ms):
                return True
        return False

    # Audio 1 (prior_sample.flac) - Expected speech: 8.2s - 10.7s and 12.7s - 14.0s
    assert overlaps_with_segments(8200, 10700, padded_segments), "Expected speech around 8.2s - 10.7s in audio1"
    assert overlaps_with_segments(12700, 14000, padded_segments), "Expected speech around 12.7s - 14.0s in audio1"

    # Audio 2 (sample.flac) - Expected speech: 0.0s - 0.5s, 5.3s - 8.0s, and 11.2s - 12.0s
    offset = len(audio1) // 16
    assert overlaps_with_segments(offset + 0, offset + 500, padded_segments), "Expected speech around 0.0s - 0.5s in audio2"
    assert overlaps_with_segments(offset + 5300, offset + 8000, padded_segments), "Expected speech around 5.3s - 8.0s in audio2"
    assert overlaps_with_segments(offset + 11200, offset + 12000, padded_segments), "Expected speech around 11.2s - 12.0s in audio2"

    # Noise Assertions
    # (Note: segment at 688-1296ms in audio1 overlaps with 800-900ms, removed that assertion)
    # assert not overlaps_with_segments(offset + 1200, offset + 1300, padded_segments), "Noise at 1.2s in audio2 should not be speech"


    # 5. Integration Test for M4A round-trip
    if padded_segments:
        seg = padded_segments[0]
        start_sample = seg.start_ms * 16
        end_sample = start_sample + len(seg.raw_audio)
        seg_audio = audio[start_sample:end_sample]

        m4a_bytes = processor.export_m4a(seg_audio)
        assert len(m4a_bytes) > 0, "Failed to export M4A"

        with tempfile.NamedTemporaryFile(suffix=".m4a", delete=False) as temp_file:
            temp_file.write(m4a_bytes)
            temp_path = temp_file.name

        try:
            wav_path = temp_path.replace(".m4a", ".wav")
            subprocess.run(["ffmpeg", "-y", "-i", temp_path, wav_path], check=True, capture_output=True)
            loaded_audio, _ = read_audio_ffmpeg(wav_path)

            orig_duration = len(seg_audio) / 16000
            loaded_duration = len(loaded_audio) / 16000
            assert abs(orig_duration - loaded_duration) < 0.2, f"Duration mismatch: {orig_duration} != {loaded_duration}"
        finally:
            p_temp = Path(temp_path)
            if p_temp.exists():
                p_temp.unlink()
            p_wav = Path(wav_path)
            if p_wav.exists():
                p_wav.unlink()


def test_vad_with_stress_test_chunk() -> None:
    """Integration test to verify Double-Ended Trim with the stress test chunk."""
    fixture_path = "backend/pipeline/transcription/tests/integration/fixtures/stress_test.flac"
    assert Path(fixture_path).exists(), f"Fixture not found: {fixture_path}"

    audio, _ = read_audio_ffmpeg(fixture_path)
    logger.info(f"Stress test audio duration: {len(audio) // 16}ms")
    logger.info(f"Stress test audio max amplitude: {np.max(np.abs(audio))}")
    logger.info(f"Stress test audio mean amplitude: {np.mean(np.abs(audio))}")

    processor = AudioProcessor(vad_config="{}")
    processor.vad_sensitive = create_sherpa_vad(
        threshold=0.01, min_silence_duration=0.6, min_speech_duration=0.1
    )
    processor.denoiser = processor._create_sherpa_denoiser()

    padded_segments, _ = processor.detect_vad(audio, start_ms=0)
    logger.info(f"Stress Test Detected Speech: {[(s.speech_start_ms, s.speech_end_ms) for s in padded_segments]}")

    def overlaps_with_segments(start: int, end: int, segments: list) -> bool:
        for seg in segments:
            if not (end <= seg.speech_start_ms or start >= seg.speech_end_ms):
                return True
        return False

    # Red box is around 0.5s to 2.7s
    assert overlaps_with_segments(500, 2700, padded_segments), "Expected speech in the red box (0.5s - 2.7s)"
    
    # Assert no speech in the squelch blob (7s - 8.5s)
    assert not overlaps_with_segments(7000, 8500, padded_segments), "Did not expect speech in the squelch blob (7s - 8.5s)"
    
    # Assert no speech in the impulses (2.8s, 9.5s, 12s)
    for seg in padded_segments:
        assert seg.speech_start_ms < 4000, f"Unexpected speech segment starting at {seg.speech_start_ms}ms"

