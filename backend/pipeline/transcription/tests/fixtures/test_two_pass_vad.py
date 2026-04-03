import os
import numpy as np
from pydub import AudioSegment
import pydub.effects as effects
import sherpa_onnx

from backend.pipeline.common.constants import SAMPLE_RATE_HZ
from backend.pipeline.transcription.constants import MS_PER_SEC, PRE_EMPHASIS_COEFF
from backend.pipeline.transcription.audio.silero_vad import create_sherpa_vad, process_vad_streaming

def apply_pre_emphasis(samples: np.ndarray, alpha: float = PRE_EMPHASIS_COEFF) -> np.ndarray:
    emphasized = np.append(
        samples[0], samples[1:] - alpha * samples[:-1]
    )
    return emphasized.astype(np.int16)

def main():
    prior_path = "backend/pipeline/transcription/tests/fixtures/prior_sample.flac"
    current_path = "backend/pipeline/transcription/tests/fixtures/sample.flac"
    
    prior_audio = AudioSegment.from_file(prior_path).set_frame_rate(SAMPLE_RATE_HZ).set_channels(1)
    current_audio = AudioSegment.from_file(current_path).set_frame_rate(SAMPLE_RATE_HZ).set_channels(1)
    
    prior_samples = np.array(prior_audio.get_array_of_samples(), dtype=np.int16)
    current_samples = np.array(current_audio.get_array_of_samples(), dtype=np.int16)
    
    os.environ["SHERPA_VAD_MODEL_PATH"] = "scratch/silero_vad.onnx"
    denoiser_model_path = "scratch/gtcrn.onnx"
    
    # helper to process through denoiser and DRC
    def preprocess(samples, denoiser_instance):
        samples_float = samples.astype(np.float32) / 32768.0
        denoised_audio_obj = denoiser_instance.run(samples_float.tolist(), SAMPLE_RATE_HZ)
        denoised_audio = np.array(denoised_audio_obj.samples, dtype=np.float32)
        denoised_int16 = (denoised_audio * 32768.0).astype(np.int16)
        
        seg = AudioSegment(
            denoised_int16.tobytes(),
            frame_rate=SAMPLE_RATE_HZ,
            sample_width=2,
            channels=1
        )
        compressed = effects.compress_dynamic_range(seg, threshold=-30.0, ratio=5.0)
        samples_rec = np.array(compressed.get_array_of_samples(), dtype=np.int16)
        samples_rec = apply_pre_emphasis(samples_rec)
        return samples_rec

    print("Initializing models...")
    # Create denoiser
    gtcrn_config = sherpa_onnx.OfflineSpeechDenoiserGtcrnModelConfig(model=denoiser_model_path)
    model_config = sherpa_onnx.OfflineSpeechDenoiserModelConfig(gtcrn=gtcrn_config, num_threads=1, provider="cpu")
    config = sherpa_onnx.OnlineSpeechDenoiserConfig(model=model_config)
    denoiser = sherpa_onnx.OnlineSpeechDenoiser(config)
    
    # Pass 1: Sensitive VAD
    print("Running Pass 1 (Sensitive)...")
    vad_pass1 = create_sherpa_vad(threshold=0.15, min_silence_duration=0.6, min_speech_duration=0.25)
    
    processed_prior = preprocess(prior_samples, denoiser)
    prior_len_samples = len(processed_prior)
    process_vad_streaming(processed_prior, 0, vad_pass1) # ignore results, just priming
    
    processed_current = preprocess(current_samples, denoiser)
    res_pass1 = process_vad_streaming(processed_current, 0, vad_pass1)
    coarse_segments = res_pass1.speech_segments
    
    print(f"Pass 1 (Coarse) segments: {coarse_segments}")
    
    # Pass 2: Refinement
    print("\nRunning Pass 2 (Refinement)...")
    
    for seg in coarse_segments:
        print(f"\nRefining segment: [{seg.start_ms}, {seg.end_ms}]")
        # Extract audio for this segment
        start_idx = int((seg.start_ms / 1000.0) * SAMPLE_RATE_HZ) - prior_len_samples
        end_idx = int((seg.end_ms / 1000.0) * SAMPLE_RATE_HZ) - prior_len_samples
        
        print(f"  Extracting samples: [{start_idx}, {end_idx}] from processed_current of length {len(processed_current)}")
        segment_samples = processed_current[start_idx:end_idx]
        
        if len(segment_samples) == 0:
            print("  Empty segment samples, skipping.")
            continue
            
        # Variation A: VAD 0.25, Silence 0.6
        vad_pass2_a = create_sherpa_vad(threshold=0.25, min_silence_duration=0.6, min_speech_duration=0.25)
        res_a = process_vad_streaming(segment_samples, seg.start_ms, vad_pass2_a)
        print(f"  Var A (VAD 0.25, Sil 0.6) split: {res_a.speech_segments}")
        
        # Variation B: VAD 0.15, Silence 0.25
        vad_pass2_b = create_sherpa_vad(threshold=0.15, min_silence_duration=0.25, min_speech_duration=0.25)
        res_b = process_vad_streaming(segment_samples, seg.start_ms, vad_pass2_b)
        print(f"  Var B (VAD 0.15, Sil 0.25) split: {res_b.speech_segments}")
        
        # Variation C: VAD 0.25, Silence 0.25
        vad_pass2_c = create_sherpa_vad(threshold=0.25, min_silence_duration=0.25, min_speech_duration=0.25)
        res_c = process_vad_streaming(segment_samples, seg.start_ms, vad_pass2_c)
        print(f"  Var C (VAD 0.25, Sil 0.25) split: {res_c.speech_segments}")

        # Variation D: VAD 0.15, Silence 0.25, Speech 0.05
        vad_pass2_d = create_sherpa_vad(threshold=0.15, min_silence_duration=0.25, min_speech_duration=0.05)
        res_d = process_vad_streaming(segment_samples, seg.start_ms, vad_pass2_d)
        print(f"  Var D (VAD 0.15, Sil 0.25, Sp 0.05) split: {res_d.speech_segments}")
        
        # Variation E: VAD 0.25, Silence 0.25, Speech 0.05
        vad_pass2_e = create_sherpa_vad(threshold=0.25, min_silence_duration=0.25, min_speech_duration=0.05)
        res_e = process_vad_streaming(segment_samples, seg.start_ms, vad_pass2_e)
        print(f"  Var E (VAD 0.25, Sil 0.25, Sp 0.05) split: {res_e.speech_segments}")

if __name__ == "__main__":
    main()
