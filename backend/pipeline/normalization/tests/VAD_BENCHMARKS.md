# VAD Audio Suite Performance Benchmarks

This document tracks the frame-based F1 accuracy benchmarks across our VAD integration test suite over time. Use this registry during upgrades to monitor quality regressions and ensure no negative drift.

## F1 Performance Benchmarks (As of May 2026)

| Audio File | Target F1 | Baseline F1 (Main) | Current F1 (Peak-Normalized) | Status | Description |
| :--- | :---: | :---: | :---: | :---: | :--- |
| **`test_stress.flac`** | 0.70 | 0.781 | **0.925** | **PASSED** | Quiet dispatcher segments starting immediately at `t=0.4s`. |
| **`test_joined.flac`** | 0.85 | 0.884 | **0.907** | **PASSED** | Multi-dispatch joined segments. |
| **`test_bcfy.flac`** | 0.80 | 0.869 | **0.895** | **PASSED** | Broadcastify dispatch containing whispers and dropouts. |
| **`test_dispatch_amador.flac`** | 0.85 | 0.937 | **0.900** | **PASSED** | Amador continuous dispatcher stream. |
| **`test_dispatch_sku.flac`** | 0.85 | 0.888 | **0.888** | **PASSED** | SKU dispatch with heavy background static interference. |
| **`test_middlebury.mp3`** | 0.85 | 0.947 | **0.889** | **PASSED** | Middlebury baseline audio (quiet segments). |
| **`test_quiet_speech_loud_transient.mp3`** | 0.60 | 0.000 | **0.662** | **PASSED** | Quiet speech followed by a loud transient click. |

---

## Guidelines for Performance Maintenance

1. **Adding New Test Goldens:**
   - When a new test file is added, establish its baseline F1 score using replayed-speech priming or un-primed execution.
   - Add the file name, target F1, and baseline F1 to this log.
   - Do not lower target F1 benchmarks unless physically justified by a change in production requirements.

2. **Investigating Regressions:**
   - If any test transitions from `PASSED` to `FAILED` during an update, check if the regression is caused by:
     - Unintentional changes to VAD parameters (`threshold_onset`, `threshold_offset`, `min_silence_duration_ms`).
     - Changes to denoiser blend ratios or dynamic range compression threshold peak triggers.
     - Preamble resampling artifacts (such as our native-rate generator fix).


