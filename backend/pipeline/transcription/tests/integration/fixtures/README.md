# Test Fixtures

This directory contains audio fixtures used for testing the transcription pipeline.

## Important Note on Audio Files

The files `sample.flac` and `prior_sample.flac` are **purposely raw** and reflect what we can expect from the live ingestion process:
- They have a sample rate of **22050 Hz** (not 16000 Hz).
- They have **non-zero start timestamps** (they do not start at time 0).

**DO NOT** modify, truncate, or resample these files! They are used to test that the pipeline can handle realistic inputs from the ingestion process.

If you need to read them in tests, note that `soundfile` might fail with `Internal psf_fseek() failed` due to the non-zero start times. Use `ffmpeg` to read them instead, as demonstrated in `test_vad_integration.py`.

## Ground Truth Speech Segments

These segments are used in `test_vad_integration.py` to verify VAD performance:

### `prior_sample.flac` (Audio 1)
- **Speech**: 8.2s - 10.7s
- **Speech**: 12.7s - 14.0s

### `sample.flac` (Audio 2)
- **Speech**: 0.0s - 0.5s
- **Speech**: 5.3s - 8.0s
- **Speech**: 11.2s - 12.0s

### `stress_test.flac` (Stress Test)
- **Speech**: Starts at around ~0.5 seconds and ends ~2.8 seconds.
- **Squelch Blob**: Noise artifact at ~7.0 seconds to 8.5 seconds.