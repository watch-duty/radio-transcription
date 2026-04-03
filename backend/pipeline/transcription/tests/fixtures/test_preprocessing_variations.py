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

def run_test(config_name, use_bandpass, use_drc, use_pre_emphasis, drc_threshold, drc_ratio, prior_samples, current_samples, vad_model_path, vad_threshold=0.15, denoiser_model_path="scratch/gtcrn_simple.onnx", min_silence_duration=0.6, min_speech_duration=0.25):
    print(f"\n--- Testing Config: {config_name} ---")
    
    # Set model path explicitly
    os.environ["SHERPA_VAD_MODEL_PATH"] = "/app/scratch/silero_vad.onnx"
    
    # Create fresh VAD and Denoiser for each config to avoid state pollution
    vad = create_sherpa_vad(threshold=vad_threshold, min_silence_duration=min_silence_duration, min_speech_duration=min_speech_duration)
    
    gtcrn_config = sherpa_onnx.OfflineSpeechDenoiserGtcrnModelConfig(
        model=denoiser_model_path
    )
    model_config = sherpa_onnx.OfflineSpeechDenoiserModelConfig(
        gtcrn=gtcrn_config, num_threads=1, provider="cpu"
    )
    config = sherpa_onnx.OnlineSpeechDenoiserConfig(model=model_config)
    denoiser = sherpa_onnx.OnlineSpeechDenoiser(config)
    
    # Helper to process a chunk through full pipeline
    def process_chunk(chunk_samples, start_ms_val):
        # 1. Denoise
        samples_float = chunk_samples.astype(np.float32) / 32768.0
        denoised_audio_obj = denoiser.run(samples_float.tolist(), SAMPLE_RATE_HZ)
        denoised_audio = np.array(denoised_audio_obj.samples, dtype=np.float32)
        
        if len(denoised_audio) == 0:
            return []
            
        denoised_int16 = (denoised_audio * 32768.0).astype(np.int16)
        
        # 2. Band-pass (optional)
        if use_bandpass:
            seg = AudioSegment(
                denoised_int16.tobytes(),
                frame_rate=SAMPLE_RATE_HZ,
                sample_width=2,
                channels=1
            )
            seg = seg.high_pass_filter(300).low_pass_filter(3500)
            denoised_int16 = np.array(seg.get_array_of_samples(), dtype=np.int16)
            
        # 3. DRC (optional)
        if use_drc:
            seg = AudioSegment(
                denoised_int16.tobytes(),
                frame_rate=SAMPLE_RATE_HZ,
                sample_width=2,
                channels=1
            )
            compressed = effects.compress_dynamic_range(seg, threshold=drc_threshold, ratio=drc_ratio)
            samples_rec = np.array(compressed.get_array_of_samples(), dtype=np.int16)
        else:
            samples_rec = denoised_int16
            
        # 4. Pre-emphasis (optional)
        if use_pre_emphasis:
            samples_rec = apply_pre_emphasis(samples_rec)
        
        # 5. VAD
        res = process_vad_streaming(samples_rec, start_ms_val, vad)
        return res.speech_segments

    # Process prior chunk for context
    if len(prior_samples) > 0:
        process_chunk(prior_samples, 0)
        
    # Process current chunk
    segments = process_chunk(current_samples, 0)
    print(f"Detected segments: {segments}")
    return segments

def main():
    prior_path = "backend/pipeline/transcription/tests/fixtures/prior_sample.flac"
    current_path = "backend/pipeline/transcription/tests/fixtures/sample.flac"
    
    prior_audio = AudioSegment.from_file(prior_path).set_frame_rate(SAMPLE_RATE_HZ).set_channels(1)
    current_audio = AudioSegment.from_file(current_path).set_frame_rate(SAMPLE_RATE_HZ).set_channels(1)
    
    prior_samples = np.array(prior_audio.get_array_of_samples(), dtype=np.int16)
    current_samples = np.array(current_audio.get_array_of_samples(), dtype=np.int16)
    
    # Run variations
    # 1. Baseline (Simple Model, Old VAD Threshold 0.15)
    run_test("Baseline (Simple Model, VAD 0.15)", 
             use_bandpass=False, use_drc=True, use_pre_emphasis=True,
             drc_threshold=-30.0, drc_ratio=5.0, 
             prior_samples=prior_samples, current_samples=current_samples, 
             vad_model_path="scratch/silero_vad.onnx",
             vad_threshold=0.15,
             denoiser_model_path="scratch/gtcrn_simple.onnx")
             
    # 2. Denoiser Only (Simple Model, VAD 0.15)
    run_test("Denoiser Only (Simple Model, VAD 0.15)", 
             use_bandpass=False, use_drc=False, use_pre_emphasis=False,
             drc_threshold=0, drc_ratio=0, 
             prior_samples=prior_samples, current_samples=current_samples, 
             vad_model_path="scratch/silero_vad.onnx",
             vad_threshold=0.15,
             denoiser_model_path="scratch/gtcrn_simple.onnx")
             
    # 3. Denoiser Only (Robust Model, VAD 0.15)
    run_test("Denoiser Only (Robust Model, VAD 0.15)", 
             use_bandpass=False, use_drc=False, use_pre_emphasis=False,
             drc_threshold=0, drc_ratio=0, 
             prior_samples=prior_samples, current_samples=current_samples, 
             vad_model_path="scratch/silero_vad.onnx",
             vad_threshold=0.15,
             denoiser_model_path="scratch/gtcrn.onnx")
             
    # 4. Gemini Recommended (Robust Model, VAD 0.4)
    run_test("Gemini Recommended (Robust Model, VAD 0.4)", 
             use_bandpass=False, use_drc=False, use_pre_emphasis=False,
             drc_threshold=0, drc_ratio=0, 
             prior_samples=prior_samples, current_samples=current_samples, 
             vad_model_path="scratch/silero_vad.onnx",
             vad_threshold=0.4,
             denoiser_model_path="scratch/gtcrn.onnx",
             min_speech_duration=0.25)
             
    # 5. Gemini Recommended (Robust Model, VAD 0.15) - for direct comparison
    run_test("Gemini Recommended (Robust Model, VAD 0.15)", 
             use_bandpass=False, use_drc=False, use_pre_emphasis=False,
             drc_threshold=0, drc_ratio=0, 
             prior_samples=prior_samples, current_samples=current_samples, 
             vad_model_path="scratch/silero_vad.onnx",
             vad_threshold=0.15,
             denoiser_model_path="scratch/gtcrn.onnx",
             min_speech_duration=0.25)
             
    # 6. Baseline Preprocessing + Robust Model + VAD 0.4
    run_test("Baseline Preprocessing + Robust Model + VAD 0.4", 
             use_bandpass=False, use_drc=True, use_pre_emphasis=True,
             drc_threshold=-30.0, drc_ratio=5.0, 
             prior_samples=prior_samples, current_samples=current_samples, 
             vad_model_path="scratch/silero_vad.onnx",
             vad_threshold=0.4,
             denoiser_model_path="scratch/gtcrn.onnx",
             min_speech_duration=0.25)
             
    # 7. Baseline Preprocessing + Robust Model + VAD 0.15 + Short Silence (0.25)
    run_test("Baseline Preprocessing + Robust Model + VAD 0.15 + Short Silence (0.25)", 
             use_bandpass=False, use_drc=True, use_pre_emphasis=True,
             drc_threshold=-30.0, drc_ratio=5.0, 
             prior_samples=prior_samples, current_samples=current_samples, 
             vad_model_path="scratch/silero_vad.onnx",
             vad_threshold=0.15,
             denoiser_model_path="scratch/gtcrn.onnx",
             min_speech_duration=0.25,
             min_silence_duration=0.25)
             
    # 8. Baseline Preprocessing + Robust Model + VAD 0.25 + Short Silence (0.25)
    run_test("Baseline Preprocessing + Robust Model + VAD 0.25 + Short Silence (0.25)", 
             use_bandpass=False, use_drc=True, use_pre_emphasis=True,
             drc_threshold=-30.0, drc_ratio=5.0, 
             prior_samples=prior_samples, current_samples=current_samples, 
             vad_model_path="scratch/silero_vad.onnx",
             vad_threshold=0.25,
             denoiser_model_path="scratch/gtcrn.onnx",
             min_speech_duration=0.25,
             min_silence_duration=0.25)
             
    # 9. Baseline Preprocessing + Robust Model + VAD 0.15 + Long Silence (0.6)
    run_test("Baseline Preprocessing + Robust Model + VAD 0.15 + Long Silence (0.6)", 
             use_bandpass=False, use_drc=True, use_pre_emphasis=True,
             drc_threshold=-30.0, drc_ratio=5.0, 
             prior_samples=prior_samples, current_samples=current_samples, 
             vad_model_path="scratch/silero_vad.onnx",
             vad_threshold=0.15,
             denoiser_model_path="scratch/gtcrn.onnx",
             min_speech_duration=0.25,
             min_silence_duration=0.6)
             
    # 10. Baseline Preprocessing + Robust Model + VAD 0.15 + Short Silence (0.25) + Short Speech (0.1)
    run_test("Baseline Preprocessing + Robust Model + VAD 0.15 + Short Silence (0.25) + Short Speech (0.1)", 
             use_bandpass=False, use_drc=True, use_pre_emphasis=True,
             drc_threshold=-30.0, drc_ratio=5.0, 
             prior_samples=prior_samples, current_samples=current_samples, 
             vad_model_path="scratch/silero_vad.onnx",
             vad_threshold=0.15,
             denoiser_model_path="scratch/gtcrn.onnx",
             min_speech_duration=0.1,
             min_silence_duration=0.25)
             
    # 11. Baseline + Bandpass (300-3500) + VAD 0.15 + Long Silence (0.6)
    run_test("Baseline + Bandpass (300-3500) + VAD 0.15 + Long Silence (0.6)", 
             use_bandpass=True, use_drc=True, use_pre_emphasis=True,
             drc_threshold=-30.0, drc_ratio=5.0, 
             prior_samples=prior_samples, current_samples=current_samples, 
             vad_model_path="scratch/silero_vad.onnx",
             vad_threshold=0.15,
             denoiser_model_path="scratch/gtcrn.onnx",
             min_speech_duration=0.25,
             min_silence_duration=0.6)
             
    # 12. Baseline Preprocessing + Robust Model + VAD 0.25 + Long Silence (0.6)
    run_test("Baseline Preprocessing + Robust Model + VAD 0.25 + Long Silence (0.6)", 
             use_bandpass=False, use_drc=True, use_pre_emphasis=True,
             drc_threshold=-30.0, drc_ratio=5.0, 
             prior_samples=prior_samples, current_samples=current_samples, 
             vad_model_path="scratch/silero_vad.onnx",
             vad_threshold=0.25,
             denoiser_model_path="scratch/gtcrn.onnx",
             min_speech_duration=0.25,
             min_silence_duration=0.6)
             
    # 13. Baseline (Less DRC: -20dB) + Robust Model + VAD 0.15 + Long Silence (0.6)
    run_test("Baseline (Less DRC: -20dB) + Robust Model + VAD 0.15 + Long Silence (0.6)", 
             use_bandpass=False, use_drc=True, use_pre_emphasis=True,
             drc_threshold=-20.0, drc_ratio=5.0, 
             prior_samples=prior_samples, current_samples=current_samples, 
             vad_model_path="scratch/silero_vad.onnx",
             vad_threshold=0.15,
             denoiser_model_path="scratch/gtcrn.onnx",
             min_speech_duration=0.25,
             min_silence_duration=0.6)

if __name__ == "__main__":
    main()
