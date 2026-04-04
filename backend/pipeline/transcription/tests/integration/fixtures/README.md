# Test Fixtures

This directory contains audio fixtures used for testing the transcription pipeline.

## Important Note on Audio Files

The files `sample.flac` and `prior_sample.flac` are **purposely raw** and reflect what we can expect from the live ingestion process:
- They have a sample rate of **22050 Hz** (not 16000 Hz).
- They have **non-zero start timestamps** (they do not start at time 0).

**DO NOT** modify, truncate, or resample these files! They are used to test that the pipeline can handle realistic inputs from the ingestion process.

If you need to read them in tests, note that `soundfile` might fail with `Internal psf_fseek() failed` due to the non-zero start times. Use `ffmpeg` to read them instead, as demonstrated in `test_vad_integration.py`.
