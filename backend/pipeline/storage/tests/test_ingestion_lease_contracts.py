"""Contract tests for the public fenced ingestion Lease values."""

from backend.pipeline.storage import (
    ingestion_lease_contracts,
    ingestion_lease_store,
)


def test_store_facade_reexports_canonical_contracts() -> None:
    """The established store import path remains source compatible."""
    for name in ingestion_lease_contracts.__all__:
        assert getattr(ingestion_lease_store, name) is getattr(
            ingestion_lease_contracts,
            name,
        )


def test_store_facade_exports_only_contracts_and_store() -> None:
    assert set(ingestion_lease_store.__all__) == {
        *ingestion_lease_contracts.__all__,
        "IngestionLeaseStore",
    }
