"""Pytest suite-wide configuration and un-authenticated GCS testing environments for segmentation."""

import pytest


@pytest.fixture(autouse=True)
def _suppress_gcs_authentication(monkeypatch: pytest.MonkeyPatch) -> None:
    """Automatically forces un-authenticated emulator testing environments across all unit tests.

    Entirely liberates core production domain models from housing dummy ADC or exception fallbacks.
    """
    monkeypatch.setenv("STORAGE_EMULATOR_HOST", "http://localhost:8080")
