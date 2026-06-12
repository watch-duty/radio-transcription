# VAD Audio Suite Performance Benchmarks

This document tracks the frame-based F1 accuracy benchmarks across our VAD integration test suite over time. Use this registry during upgrades to monitor quality regressions and ensure no negative drift.

## F1 Performance Benchmarks (As of June 2026 / PR #709)

| Audio File | Target F1 | Baseline F1 (Main) | Current F1 (PR #709) | Status | Description |
| :--- | :---: | :---: | :---: | :---: | :--- |
| **`test_stress.flac`** | 0.70 | 0.925 | **0.925** | **PASSED** | Quiet dispatcher segments starting immediately at `t=0.4s`. |
| **`test_joined.flac`** | 0.85 | 0.895 | **0.895** | **PASSED** | Multi-dispatch joined segments. |
| **`test_bcfy.flac`** | 0.80 | 0.895 | **0.895** | **PASSED** | Broadcastify dispatch containing whispers and dropouts. |
| **`test_dispatch_amador.flac`** | 0.85 | 0.869 | **0.872** | **PASSED** | Amador continuous dispatcher stream. |
| **`test_dispatch_sku.flac`** | 0.85 | 0.869 | **0.869** | **PASSED** | SKU dispatch with heavy background static interference. |
| **`test_middlebury.mp3`** | 0.85 | 0.889 | **0.889** | **PASSED** | Middlebury baseline audio (quiet segments). |
| **`test_quiet_speech_loud_transient.mp3`** | 0.60 | 0.662 | **0.662** | **PASSED** | Quiet speech followed by a loud transient click. |
| **`test_tone_only.flac`** | 1.00 | 0.000 | **1.000** | **PASSED** | Broadcastify two-tone paging and EAS alert audio (100% rejected as non-speech). |
| **`test_subaudible_flickering.flac`** | 1.00 | 0.000 | **1.000** | **PASSED** | Open-squelch static ticks and 72Hz flickering interference (100% rejected as non-speech). |

* **Note on Empirical Baseline Shifts (`test_dispatch_amador`)**: The Baseline F1 on `main` shifted from an earlier $0.937 \rightarrow 0.869$ when software Peak Normalization was enabled across the streaming pipeline. Peak Normalization amplifies low-level background dispatcher static, expanding trailing word boundaries. The PR #709 F1 of $0.872$ comfortably surpasses our official quality gate of $0.85$.

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


