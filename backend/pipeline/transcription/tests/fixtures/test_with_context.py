import os
import numpy as np
from pydub import AudioSegment
import sherpa_onnx

from backend.pipeline.common.constants import SAMPLE_RATE_HZ
from backend.pipeline.transcription.constants import MS_PER_SEC
from backend.pipeline.transcription.audio.silero_vad import create_sherpa_vad, process_vad_streaming

def main():
    print("Testing with context...")
    prior_path = "backend/pipeline/transcription/tests/fixtures/prior_sample.flac"
    current_path = "backend/pipeline/transcription/tests/fixtures/sample.flac"
    
    if not os.path.exists(prior_path) or not os.path.exists(current_path):
        print("Fixtures not found!")
        return
        
    prior_audio = AudioSegment.from_file(prior_path).set_frame_rate(SAMPLE_RATE_HZ)
    current_audio = AudioSegment.from_file(current_path).set_frame_rate(SAMPLE_RATE_HZ)
    
    threshold = 0.15
    print(f"Using threshold: {threshold}")
    
    os.environ["SHERPA_VAD_MODEL_PATH"] = "scratch/silero_vad.onnx"
    vad = create_sherpa_vad(threshold=threshold)
    
    # 1. Process prior chunk
    print("\nProcessing prior chunk...")
    prior_samples = np.array(prior_audio.get_array_of_samples(), dtype=np.int16)
    prior_res = process_vad_streaming(prior_samples, 0, vad)
    print(f"Prior chunk detected segments: {prior_res.speech_segments}")
    
    # 2. Process current chunk (with context from prior chunk)
    print("\nProcessing current chunk...")
    current_samples = np.array(current_audio.get_array_of_samples(), dtype=np.int16)
    
    # We pass the start_ms as 15000 (since the prior chunk is 15 seconds long)
    # to maintain the absolute timeline if needed, or we can use 0 to see relative to this chunk.
    # Let's use 0 to compare directly with previous non-context run!
    current_res = process_vad_streaming(current_samples, 0, vad)
    print(f"Current chunk detected segments: {current_res.speech_segments}")

if __name__ == "__main__":
    main()
