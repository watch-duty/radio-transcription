# VAD Audio Suite Performance Benchmarks

This document tracks the frame-based F1 accuracy benchmarks across our VAD integration test suite over time. Use this registry during upgrades to monitor quality regressions and ensure no negative drift.

## F1 Performance Benchmarks (As of May 2026)

| Audio File | Target F1 | Baseline F1 (Main) | Current F1 (Clean Offline Priming) | Status | Description |
| :--- | :---: | :---: | :---: | :---: | :--- |
| **`test_stress.flac`** | 0.70 | 0.781 | **0.782** | **PASSED** | Stress file (quiet/loud segments starting immediately at `t=0.4s`). Fully passing under offline replayed speech priming. |
| **`test_joined.flac`** | 0.85 | 0.884 | **0.884** | **PASSED** | Multi-dispatch joined segments. |
| **`test_bcfy.flac`** | 0.80 | 0.869 | **0.869** | **PASSED** | Broadcastify dispatch containing whispers and dropouts. |
| **`test_dispatch_amador.flac`** | 0.85 | 0.937 | **0.937** | **PASSED** | Amador continuous dispatcher stream. |
| **`test_dispatch_sku.flac`** | 0.85 | 0.888 | **0.888** | **PASSED** | SKU dispatch with heavy background static interference. |
| **`test_middlebury.mp3`** | 0.85 | 0.947 | **0.947** | **PASSED** | Middlebury baseline audio (quiet segments). |
| **`test_quiet_speech_loud_transient.mp3`** | 0.75 | 0.000 | **0.000** | *XFAIL* | Quiet speech followed by a loud transient click. |

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

3. **Expected Failure (`XFAIL`) Policies:**
   - Do not add new `XFAIL` tests to bypass code-quality failures.
   - Only `test_quiet_speech_loud_transient.mp3` is flagged as `XFAIL` due to fundamental physical trade-offs in recurrent noise subtraction. It remains in the suite with a high target F1 (e.g., `0.75+`) so that future denoiser/VAD core model upgrades will automatically test against it as a quality target.
