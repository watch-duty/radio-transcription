"""HuggingFace dataset adapter — loads HF dataset, re-encodes to 16kHz FLAC, uploads to GCS,
yields CanonicalRows.

Satisfies the DatasetAdapter Protocol from common.manifest.
Requires common[hf,audio] extras (datasets + torchaudio/soundfile).
"""

from __future__ import annotations

import logging
import tempfile
from collections.abc import Iterator
from pathlib import Path

from common.auth_utils import login_to_huggingface
from common.gcs_utils import blob_exists, parse_gcs_uri, upload_file_to_blob
from google.cloud import storage

logger = logging.getLogger(__name__)

try:
    from datasets import load_dataset
except ImportError as _e:
    _HF_MISSING: Exception | None = _e
else:
    _HF_MISSING = None

try:
    from common.audio_utils import preprocess_audio_for_model
except ImportError as _ae:
    _AUDIO_MISSING: Exception | None = _ae
else:
    _AUDIO_MISSING = None


def _require_hf() -> None:
    if _HF_MISSING:
        raise ImportError(
            "hf_dataset adapter requires common[hf,audio]: "
            "pip install 'common[hf,audio]'"
        ) from _HF_MISSING


def _require_audio() -> None:
    if _AUDIO_MISSING:
        raise ImportError(
            "hf_dataset adapter requires common[audio]: "
            "pip install 'common[hf,audio]'"
        ) from _AUDIO_MISSING


class HfDatasetAdapter:
    """HuggingFace dataset -> 16kHz FLAC upload -> CanonicalRow iterator.

    For each example:
      1. Extract audio array + sampling_rate from the HF Audio column.
      2. Write to a temp WAV, call preprocess_audio_for_model (resample+mono -> FLAC).
      3. Compute GCS target URI: {gcs_audio_prefix}/{split}/{example_id}.flac.
      4. blob_exists check -- skip upload if already present (idempotent re-runs).
      5. upload_file_to_blob.
      6. Yield CanonicalRow.

    Args:
        hf_repo: HuggingFace dataset repository ID (e.g. "jlvdoorn/atcosim").
        split: Dataset split name (e.g. "train", "validation", "test").
        audio_column: Column name containing the HF Audio dict (default: "audio").
        text_column: Column name containing the ground-truth text (default: "text").
        gcs_audio_prefix: GCS prefix for uploaded FLACs (e.g. "gs://bucket/sft/datasets/atcosim/").
        storage_client: Authenticated google.cloud.storage.Client.
        normalize: If True, caller should apply text normalization via common.scoring.build_normalizer
            after iterating (not done inside the adapter -- keeps adapter simple).
    """

    def __init__(
        self,
        hf_repo: str,
        split: str,
        storage_client: storage.Client,
        audio_column: str = "audio",
        text_column: str = "text",
        gcs_audio_prefix: str = "",
        normalize: bool = False,
    ) -> None:
        _require_hf()
        _require_audio()
        self._hf_repo = hf_repo
        self._split = split
        self._audio_column = audio_column
        self._text_column = text_column
        self._gcs_audio_prefix = gcs_audio_prefix.rstrip("/")
        self._storage_client = storage_client
        self._normalize = normalize

    def iter_rows(self) -> Iterator[CanonicalRow]:
        from common.manifest import CanonicalRow

        login_to_huggingface()
        logger.info(f"Loading HF dataset {self._hf_repo} split={self._split}")
        ds = load_dataset(
            self._hf_repo, split=self._split, trust_remote_code=False
        )

        for idx, example in enumerate(ds):
            audio_data = example[self._audio_column]
            gt_text = str(example[self._text_column]).strip()
            if not gt_text:
                logger.warning(
                    f"[{self._hf_repo} idx={idx}] empty text -- skipping"
                )
                continue

            example_id = f"{self._split}_{idx:06d}"
            segment_id = f"{idx:06d}"
            gcs_uri = (
                f"{self._gcs_audio_prefix}/{self._split}/{example_id}.flac"
            )

            # Idempotency: skip if already uploaded
            if not blob_exists(self._storage_client, gcs_uri):
                self._encode_and_upload(audio_data, gcs_uri, example_id)

            # duration from the HF audio sampling_rate + array length
            sr = audio_data.get("sampling_rate", 16000)
            array = audio_data.get("array", [])
            duration = len(array) / sr if sr else 0.0

            yield CanonicalRow(
                audio_filepath=gcs_uri,
                example_id=example_id,
                segment_id=segment_id,
                offset=0.0,
                duration=duration,
                text=gt_text,
            )

    def _encode_and_upload(
        self, audio_data: dict, gcs_uri: str, label: str
    ) -> None:
        """Write audio to temp FLAC at 16kHz and upload to GCS."""
        import numpy as np
        import soundfile as sf

        array = audio_data.get("array")
        sr = audio_data.get("sampling_rate", 16000)

        with tempfile.TemporaryDirectory() as tmp:
            tmp_wav = Path(tmp) / f"{label}.wav"
            tmp_flac = Path(tmp) / f"{label}.flac"

            # Write source audio (any sample rate)
            sf.write(str(tmp_wav), np.array(array, dtype="float32"), sr)

            # Resample + mono + encode FLAC at 16kHz
            preprocess_audio_for_model(
                str(tmp_wav), str(tmp_flac), target_sr=16000
            )

            # Upload to GCS: parse URI into bucket + blob_path
            bucket_name, blob_path = parse_gcs_uri(gcs_uri)
            upload_file_to_blob(
                self._storage_client, bucket_name, blob_path, str(tmp_flac)
            )
            logger.info(f"Uploaded: {gcs_uri}")
