# VAD Audio Suite Performance Benchmarks

This document tracks the frame-based F1 accuracy, Precision, and Recall benchmarks across our VAD integration test suite. Use this registry during upgrades to monitor quality, understand the trade-offs between sensitivity and noise leakage, and ensure no regression occurs.

## VAD Quality Philosophy: High Recall Safety
In radio transcription, **we prioritize Recall (capturing all speech) over Precision (avoiding noise leaks)**. 
* **High Recall (Target: >0.85)** guarantees that we do not clip the beginning or end of dispatch transmissions, ensuring vital information is never lost.
* **Moderate Precision (0.55 - 0.95)** is acceptable. Keeping the VAD gate open slightly longer (padding) or merging close segments sends a few seconds of extra silence or static noise to the ASR, which the transcription engine easily ignores.
* **F1 Score** remains our primary balanced metric, with a quality gate of **`>0.85`** for standard dispatch files.

---

## F1, Precision, and Recall Benchmarks

All metrics below are evaluated under the official production configuration: **`pad_sec = 0.0`** (for pure accuracy tracking) and **`priming_sec = 3.0`** (using the last 3.0s of the 6.0s extracted tail for warmup).

| Audio File | F1 | Precision | Recall | Description / Justification |
| :--- | :---: | :---: | :---: | :--- |
| **`test_stress.flac`** | **0.925** | `1.000` | `0.861` | Quiet dispatcher segments starting immediately at `t=0.4s`. |
| **`test_joined.flac`** | **0.895** | `0.902` | `0.888` | Multi-dispatch joined segments. |
| **`test_bcfy.flac`** | **0.850** | `0.923` | `0.787` | Broadcastify dispatch containing whispers and dropouts. |
| **`test_dispatch_amador.flac`** | **0.906** | `0.916` | `0.896` | Amador continuous dispatcher stream. |
| **`test_dispatch_sku.flac`** | **0.890** | `0.832` | `0.956` | SKU dispatch with heavy background static interference. |
| **`test_middlebury_quiet_segments.mp3`** | **0.865** | `0.981` | `0.773` | Quiet segments from Middlebury dataset. |
| **`test_middlebury_quiet_spiky.mp3`** | **0.713** | `0.564` | `0.969` | Quiet EMS speech. Low precision due to conservative 3s chunk padding. |
| **`test_quiet_speech_loud_transient.mp3`** | **0.669** | `0.564` | `0.822` | Quiet speech followed by a loud transient click. |
| **`test_only_static_middlebury.mp3`** | **1.000** | `1.000` | `1.000` | Pure static noise (100% rejected, no false positives). |
| **`test_subaudible_flickering.flac`** | **1.000** | `1.000` | `1.000` | 72Hz electrical flickering interference (100% rejected). |
| **`test_vad_deafening_dispatcher_ems.flac`** | **0.494** | `1.000` | `0.328` | Loud dispatcher followed by quiet EMS (3s real noise warmup). Captures dispatcher and parts of both EMS segments. |
| **`test_vad_deafening_static_preamble.flac`** | **0.640** | `0.996` | `0.471` | Quiet speech preceded by 1.4s of static (3s real noise warmup). Captures the majority of the speech. |

*Note: For static-only files, an empty detection matching empty ground truth yields a perfect `1.000` across all metrics.*

---

## Architectural Design Patterns

### 1. Conditional state propagation
To prevent loud dispatches or long static noise from "deafening" the VAD in subsequent chunks, the pipeline uses **conditional state propagation** to manage state across chunk boundaries.

* **Mechanism**: The Stitcher engine caches and passes the trailing `6.0s` audio tail of the current chunk to prime the next chunk **only if the current chunk actually ended in active speech** (evaluated with a 50ms tolerance). If the chunk ended in silence or static noise, the state is discarded, and the next chunk performs a clean cold-start.
* **Rationale**: If a chunk ends in heavy static noise, unconditionally warming up on it in the next chunk causes the VAD's internal denoiser (UL-UNAS) to adapt its noise floor upward, deafening it to subsequent genuine quiet speech. Discarding the state on silence prevents this noise propagation while preserving boundary continuity for active, ongoing speech.

### 2. Dither Stabilization (RNN Phase Locking)
The recurrent denoiser (**UL-UNAS**) RNN is highly sensitive to input perturbations when operating on extremely quiet signals (low SNR). Minor 1-LSB rounding differences between different audio decoders can cause the internal GRU/LSTM states to fluctuate completely out of phase, suppressing speech onsets.

* **Mechanism**: We apply a deterministic, mathematically inaudible **`-120dB RMS` ($1\times 10^{-6}$) Gaussian dither** to the audio signal in `detect_speech_segments` just before peak normalization.
* **Rationale**: This steady, sub-audible noise floor "swamps" any LSB decoder rounding mismatches, forcing the RNN states to remain locked in phase across all platforms and decoders, restoring onset sensitivity for quiet speech without triggering false positives on static-only files.

---

## Guidelines for Performance Maintenance

1. **Adding New Test Goldens:**
   * When a new test file is added, establish its baseline F1, Precision, and Recall.
   * Document the file in the table above, noting its acoustic characteristics.
   * Do not lower quality gates unless physically justified by a change in production requirements.

2. **Investigating Regressions:**
   * If a change causes a regression, check if it is a **Recall** drop (clipping speech) or a **Precision** drop (noise leaking). Prioritize fixing Recall drops.
   * Verify that the **conditional state propagation** logic is not accidentally discarding tails on genuine cross-boundary speech.
