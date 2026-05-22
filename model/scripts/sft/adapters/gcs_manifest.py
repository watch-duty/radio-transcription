"""GCS manifest dataset adapter — reads a pre-split GCS JSONL manifest and yields CanonicalRows.

Satisfies the DatasetAdapter Protocol from common.manifest.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator

from common.gcs_utils import download_jsonl_manifest
from common.manifest import CanonicalRow, rows_from_manifest
from google.cloud import storage

logger = logging.getLogger(__name__)


class GcsManifestAdapter:
    """Read a pre-split GCS JSONL manifest -> CanonicalRow iterator.

    Args:
        manifest_uri: GCS URI to the manifest JSONL (gs://bucket/path/manifest.jsonl).
        storage_client: Authenticated google.cloud.storage.Client.
        normalize: If True, apply text normalization (unused by Echo; used for ATC).
            The normalization itself is applied by the caller via common.scoring.build_normalizer
            if normalize=True is set in datasets.toml. The adapter just yields the raw text.
    """

    def __init__(
        self,
        manifest_uri: str,
        storage_client: storage.Client,
        normalize: bool = False,
    ) -> None:
        self._manifest_uri = manifest_uri
        self._storage_client = storage_client
        self._normalize = normalize

    def iter_rows(self) -> Iterator[CanonicalRow]:
        """Download the manifest from GCS and yield typed CanonicalRows."""
        logger.info(f"Loading GCS manifest: {self._manifest_uri}")
        entries = download_jsonl_manifest(
            self._storage_client, self._manifest_uri
        )
        logger.info(f"Loaded {len(entries)} rows from {self._manifest_uri}")
        for row in rows_from_manifest(entries):
            yield row
