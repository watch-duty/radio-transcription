# VAD Audio Suite Performance Benchmarks

This document tracks the frame-based F1 accuracy, Precision, and Recall benchmarks across our VAD integration test suite. Use this registry during upgrades to monitor quality, understand the trade-offs between sensitivity and noise leakage, and ensure no regression occurs.

## VAD Quality Philosophy: High Recall Safety
In radio transcription, **we prioritize Recall (capturing all speech) over Precision (avoiding noise leaks)**. 
* **High Recall (Target: >0.85)** guarantees that we do not clip the beginning or end of dispatch transmissions, ensuring vital information is never lost.
* **Moderate Precision (0.55 - 0.95)** is acceptable. Keeping the VAD gate open slightly longer (padding) or merging close segments sends a few seconds of extra silence or static noise to the ASR, which the transcription engine easily ignores.
* **F1 Score** remains our primary balanced metric, with a quality gate of **`>0.85`** for standard dispatch files.

---

## F1, Precision, and Recall Benchmarks

All metrics below are evaluated under the official production configuration: **`pad_sec = 0.0`** (for pure accuracy tracking) and **`priming_sec = 6.0`** (matching the default 6.0s lookback priming in streaming mode).

| Audio File | F1 | Precision | Recall | Description / Justification |
| :--- | :---: | :---: | :---: | :--- |
| **`test_stress.flac`** | **0.841** | `0.725` | `1.000` | Quiet dispatcher segments starting immediately at `t=0.4s`. |
| **`test_joined.flac`** | **0.777** | `0.636` | `1.000` | Multi-dispatch joined segments. |
| **`test_bcfy.flac`** | **0.860** | `0.830` | `0.893` | Broadcastify dispatch containing whispers and dropouts. |
| **`test_dispatch_amador.flac`** | **0.811** | `0.724` | `0.923` | Amador continuous dispatcher stream. |
| **`test_dispatch_sku.flac`** | **0.858** | `0.751` | `1.000` | SKU dispatch with heavy background static interference. |
| **`test_middlebury_quiet_segments.mp3`** | **0.824** | `0.705` | `0.990` | Quiet segments from Middlebury dataset. |
| **`test_middlebury_quiet_spiky.mp3`** | **0.596** | `0.425` | `1.000` | Quiet EMS speech. Low precision due to conservative chunk padding. |
| **`test_quiet_speech_loud_transient.mp3`** | **0.734** | `0.580` | `1.000` | Quiet speech followed by a loud transient click. |
| **`test_muffled_mason_co_fire.flac`** | **0.507** | `0.341` | `0.987` | Quiet, muffled dispatch speech (Mason County Fire). |
| **`test_only_static_middlebury.mp3`** | **1.000** | `1.000` | `1.000` | Pure static noise (100% rejected, zero false positives). |
| **`test_subaudible_flickering.flac`** | **1.000** | `1.000` | `1.000` | 72Hz electrical flickering interference (100% rejected). |
| **`test_vad_deafening_dispatcher_ems.flac`** | **0.785** | `0.720` | `0.863` | Loud dispatcher followed by quiet EMS. High recall maintained via state continuity. |
| **`test_vad_deafening_static_preamble.flac`** | **0.682** | `0.997` | `0.519` | Quiet speech preceded by 1.4s of static noise. |
| **`test_cajon_pass_trailing.flac`** | **0.190** | `0.713` | `0.110` | Quiet, muffled scanner speech preceded by open-squelch static (Cajon Pass feed). |
| **`bcfy_feed_or_hood_river_missed_speech.flac`** | **0.556** | `0.385` | `1.000` | Oregon Hood River feed segment (`c1416cf1`): Short dual speech bursts separated by a 1.08s pause; 100% recall with chunk padding. |

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

### 3. Removed: pre-denoiser AGC Compressor (history, for future reference)
An earlier revision of `preprocess()` conditionally ran a `pedalboard.Compressor` on the bandpassed signal before the UL-UNAS denoiser, intended to lift quiet passages relative to a loud peak within the same chunk. It was removed after investigation showed the gate (`peak < comp_peak_threshold`) was comparing against the *post*-normalization peak, which `_peak_normalize` always rescales to a fixed target (~0.95) — so the branch was provably dead in every production chunk, regardless of how quiet the source audio actually was.

* **It did work once, standalone**: the original prototype (`model/colabs/segmentation/silero_and_ul_unas_wet_dry_VAD_mixture.ipynb`) gates correctly on the *pre*-normalization peak and validated real benefit there, using gentler settings (`threshold_db=-15`, `ratio=3.0`) than what ended up in production (`-30.0`/`6.0`). The gate broke silently during the port to the streaming pipeline.
* **Why it wasn't simply re-fixed**: correcting the gate to use the pre-normalization peak (with either the production or the original colab's settings) reliably reintroduces a false-positive speech segment on `test_only_static_middlebury.mp3`, which must return zero segments. This isn't a tuning problem — the colab predates the click/burst noise-rejection heuristics now in `_is_speech_segment` (spikiness-ratio check), and compression's job (flattening transient dynamics) directly undermines that heuristic's signal (spiky RMS = click/static, flat RMS = sustained speech). Confirmed across 5 parameter configs spanning the colab's original settings through production's.
* **If revisiting this**: any reintroduction needs `_is_speech_segment`'s noise/click rejection reworked to classify on a pre-compression signal (or an equivalent fix), validated against the full benchmark suite in this document, before the compressor can be safely re-gated on. Don't just restore the old gate condition or the old parameters — both were tried during this investigation and both reopen the static false-positive.

---

## Guidelines for Performance Maintenance

1. **Adding New Test Goldens:**
   * When a new test file is added, establish its baseline F1, Precision, and Recall.
   * Document the file in the table above, noting its acoustic characteristics.
   * Do not lower quality gates unless physically justified by a change in production requirements.

2. **Investigating Regressions:**
   * If a change causes a regression, check if it is a **Recall** drop (clipping speech) or a **Precision** drop (noise leaking). Prioritize fixing Recall drops.
   * Verify that the **conditional state propagation** logic is not accidentally discarding tails on genuine cross-boundary speech.
