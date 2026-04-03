import os
import numpy as np
from pydub import AudioSegment
import sherpa_onnx

from backend.pipeline.transcription.audio.silero_vad import create_sherpa_vad, process_vad_streaming

def main():
    print("Analyzing audio...")
    fixture_path = "backend/pipeline/transcription/tests/fixtures/sample.flac"
    if not os.path.exists(fixture_path):
        print(f"Fixture not found at {fixture_path}")
        return
        
    audio = AudioSegment.from_file(fixture_path)
    print(f"Duration: {len(audio)}ms")
    print(f"Channels: {audio.channels}")
    print(f"Frame rate: {audio.frame_rate}")
    
    audio = audio.set_frame_rate(16000)
    
    # Try different thresholds
    for thresh in [0.5, 0.3, 0.15, 0.1]:
        print(f"\nTrying threshold: {thresh}")
        # We need to set the environment variables again in case they are not inherited
        os.environ["SHERPA_VAD_MODEL_PATH"] = "scratch/silero_vad.onnx"
        
        vad = create_sherpa_vad(threshold=thresh)
        
        samples = np.array(audio.get_array_of_samples(), dtype=np.int16)
        
        res = process_vad_streaming(samples, 0, vad)
        print(f"Detected speech segments: {res.speech_segments}")

if __name__ == "__main__":
    main()
