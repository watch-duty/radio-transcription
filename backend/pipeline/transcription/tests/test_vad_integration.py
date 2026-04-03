import os
import numpy as np
import pytest
from pydub import AudioSegment
from pydub.generators import Sine

from backend.pipeline.transcription.audio.audio_processor import AudioProcessor
from backend.pipeline.transcription.audio.silero_vad import create_sherpa_vad


def test_vad_integration():
    """Integration test to verify VAD and Denoiser work without mocks."""
    # Ensure environment variables are set for tests to point to scratch
    os.environ["SHERPA_VAD_MODEL_PATH"] = "scratch/silero_vad.onnx"
    os.environ["GTCRN_MODEL_PATH"] = "scratch/gtcrn_simple.onnx"

    # 1. Create a dummy audio chunk with a tone in the middle
    # 2 seconds of silence, 1 second of 440Hz tone, 2 seconds of silence
    # Total 5 seconds.
    # VAD expects 16kHz
    silence = AudioSegment.silent(duration=2000, frame_rate=16000)
    sine = Sine(440).to_audio_segment(duration=1000).set_frame_rate(16000)
    
    audio = silence + sine + silence
    
    # 2. Initialize AudioProcessor
    processor = AudioProcessor(vad_config="{}")
    
    processor.vad_sensitive = create_sherpa_vad(threshold=0.15, min_silence_duration=0.6, min_speech_duration=0.25)
    processor.vad_strict = create_sherpa_vad(threshold=0.25, min_silence_duration=0.25, min_speech_duration=0.05)
    processor.denoiser = processor._create_sherpa_denoiser()
    
    # 4. Run detect_vad
    # Start time is 1000ms arbitrarily
    start_ms = 1000
    padded_segments = processor.detect_vad(audio, start_ms=start_ms)
    
    # 5. Assertions
    # We expect it to run without crashing.
    assert isinstance(padded_segments, list)
    
    # Print the result to see if VAD detected anything.
    # Since it's a pure tone, VAD might or might not detect it as speech.
    print(f"\nDetected segments: {padded_segments}")
    for seg in padded_segments:
        print(f"Segment: {seg.start_ms}ms - {seg.end_ms}ms")


def test_vad_with_real_audio():
    """Integration test to verify VAD works with real audio."""
    os.environ["SHERPA_VAD_MODEL_PATH"] = "scratch/silero_vad.onnx"
    os.environ["GTCRN_MODEL_PATH"] = "scratch/gtcrn_simple.onnx"

    # 1. Load real audio from fixtures
    fixture_path = "backend/pipeline/transcription/tests/fixtures/sample.flac"
    assert os.path.exists(fixture_path), f"Fixture not found: {fixture_path}"
    
    audio = AudioSegment.from_file(fixture_path)
    
    # VAD expects 16kHz, make sure it is 16kHz
    audio = audio.set_frame_rate(16000)
    
    # 2. Initialize AudioProcessor
    processor = AudioProcessor(vad_config="{}")
    
    processor.vad_sensitive = create_sherpa_vad(threshold=0.15, min_silence_duration=0.6, min_speech_duration=0.25)
    processor.vad_strict = create_sherpa_vad(threshold=0.25, min_silence_duration=0.25, min_speech_duration=0.05)
    processor.denoiser = processor._create_sherpa_denoiser()
    
    # 4. Run detect_vad
    start_ms = 0
    padded_segments = processor.detect_vad(audio, start_ms=start_ms)
    
    # 5. Assertions
    assert isinstance(padded_segments, list)
    
    print(f"\nReal audio detected segments: {padded_segments}")
    for seg in padded_segments:
        print(f"Segment: {seg.start_ms}ms - {seg.speech_end_ms}ms")

    # 6. Integration Test for pydub/ffmpeg (Round-trip MP3)
    if padded_segments:
        seg = padded_segments[0]
        temp_file = "scratch/temp_test.mp3"
        try:
            # Export to MP3 (exercises ffmpeg encoder)
            seg.audio.export(temp_file, format="mp3")
            assert os.path.exists(temp_file), "Failed to export MP3"
            assert os.path.getsize(temp_file) > 0, "Exported MP3 is empty"
            
            # Load back (exercises ffmpeg decoder)
            loaded_audio = AudioSegment.from_file(temp_file)
            assert isinstance(loaded_audio, AudioSegment), "Failed to load MP3 back"
            
            # Verify duration is roughly same (MP3 might add slight padding)
            orig_duration = seg.audio.duration_seconds
            loaded_duration = loaded_audio.duration_seconds
            assert abs(orig_duration - loaded_duration) < 0.1, f"Duration mismatch: {orig_duration} != {loaded_duration}"
            
            print(f"Successfully verified MP3 round-trip for segment. Orig duration: {orig_duration:.2f}s, Loaded: {loaded_duration:.2f}s")
            
        finally:
            if os.path.exists(temp_file):
                os.remove(temp_file)
