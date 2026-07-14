"""Pure contracts for fenced ingestion Lease queries and value types."""

from __future__ import annotations

import dataclasses
import inspect
import pathlib
import unittest
import uuid

from backend.pipeline.storage import (
    feed_store,
    ingestion_lease_queries,
    ingestion_lease_store,
)


def _normalized_sql(text: str) -> str:
    """Return SQL without comments or formatting-only whitespace."""
    uncommented = "\n".join(
        line for line in text.splitlines() if not line.lstrip().startswith("--")
    )
    return " ".join(uncommented.split())


class TestLeaseGrantContract(unittest.TestCase):
    """Tests for immutable complete-grant values."""

    def setUp(self) -> None:
        self.owner_id = uuid.UUID("11111111-2222-3333-4444-555555555555")

    def test_grant_contains_exact_complete_identity(self) -> None:
        fields = dataclasses.fields(ingestion_lease_store.LeaseGrant)

        self.assertEqual(
            [field.name for field in fields],
            [
                "source_type",
                "lease_key",
                "owner_worker_id",
                "fencing_token",
            ],
        )
        self.assertNotIn("status", {field.name for field in fields})
        self.assertNotIn(
            "membership_revision",
            {field.name for field in fields},
        )

    def test_grant_and_snapshot_values_are_frozen_and_slotted(self) -> None:
        grant = ingestion_lease_store.LeaseGrant(
            feed_store.SourceType.BCFY_CALLS,
            "00123",
            self.owner_id,
            7,
        )

        with self.assertRaises(dataclasses.FrozenInstanceError):
            grant.fencing_token = 8  # type: ignore[misc]  # ty: ignore[invalid-assignment]

        for value_type in (
            ingestion_lease_store.LeaseGrant,
            ingestion_lease_store.LeaseSnapshot,
            ingestion_lease_store.LeaseClaim,
        ):
            self.assertTrue(dataclasses.is_dataclass(value_type))
            self.assertTrue(hasattr(value_type, "__slots__"))

    def test_snapshot_contains_only_failure_policy_state(self) -> None:
        fields = dataclasses.fields(ingestion_lease_store.LeaseSnapshot)
        heartbeat_fields = dataclasses.fields(
            ingestion_lease_store.LeaseHeartbeatResult
        )

        self.assertEqual(
            [field.name for field in fields],
            ["status", "failure_count", "status_reason"],
        )
        self.assertEqual(
            [field.name for field in heartbeat_fields],
            ["grant", "disposition"],
        )

    def test_grant_rejects_incomplete_or_malformed_identity(self) -> None:
        cases = (
            ("bcfy_calls", "123", self.owner_id, 1),
            (feed_store.SourceType.BCFY_CALLS, "", self.owner_id, 1),
            (feed_store.SourceType.BCFY_CALLS, "123", "worker", 1),
            (feed_store.SourceType.BCFY_CALLS, "123", self.owner_id, True),
            (feed_store.SourceType.BCFY_CALLS, "123", self.owner_id, -1),
        )

        for case_index, case in enumerate(cases):
            with (
                self.subTest(case_index=case_index),
                self.assertRaises((TypeError, ValueError)),
            ):
                ingestion_lease_store.LeaseGrant(
                    *case,  # ty: ignore[invalid-argument-type]
                )

    def test_grant_equality_excludes_mutable_snapshot_state(self) -> None:
        grant = ingestion_lease_store.LeaseGrant(
            feed_store.SourceType.BCFY_CALLS,
            "123",
            self.owner_id,
            4,
        )
        first = ingestion_lease_store.LeaseClaim(
            grant,
            ingestion_lease_store.LeaseSnapshot(
                status=feed_store.FeedStatus.ACTIVE,
                failure_count=0,
                status_reason=None,
            ),
        )
        second = dataclasses.replace(
            first,
            snapshot=dataclasses.replace(
                first.snapshot,
                failure_count=3,
            ),
        )

        self.assertEqual(first.grant, second.grant)
        self.assertNotEqual(first.snapshot, second.snapshot)


class TestLeaseClaimQueryContract(unittest.TestCase):
    """Tests for deterministic Lease-only ownership generations."""

    def test_primary_and_recovery_are_distinct_bounded_claims(self) -> None:
        primary = _normalized_sql(
            ingestion_lease_queries.CLAIM_UNCLAIMED_LEASES_SQL
        )
        recovery = _normalized_sql(
            ingestion_lease_queries.CLAIM_RECOVERABLE_LEASES_SQL
        )

        self.assertNotEqual(primary, recovery)
        for sql in (primary, recovery):
            self.assertIn("AS MATERIALIZED", sql)
            self.assertIn("FOR NO KEY UPDATE SKIP LOCKED", sql)
            self.assertIn("LIMIT $3", sql)
            self.assertIn("ORDER BY source_type, lease_key", sql)
            self.assertEqual(sql.count("fencing_token + 1"), 1)
            self.assertIn("UPDATE public.ingestion_leases", sql)

    def test_claim_hot_paths_touch_only_lease_authority(self) -> None:
        forbidden = (
            " from feeds",
            " join feeds",
            "feed_properties",
            " exists ",
            " count(",
        )

        for query in (
            ingestion_lease_queries.CLAIM_UNCLAIMED_LEASES_SQL,
            ingestion_lease_queries.CLAIM_RECOVERABLE_LEASES_SQL,
        ):
            sql = f" {_normalized_sql(query).lower()} "
            self.assertIn("ingestion_leases", sql)
            for token in forbidden:
                self.assertNotIn(token, sql)

    def test_primary_claim_is_unclaimed_only(self) -> None:
        sql = _normalized_sql(
            ingestion_lease_queries.CLAIM_UNCLAIMED_LEASES_SQL
        )

        self.assertIn("status = 'unclaimed'::public.feed_status", sql)
        self.assertNotIn("status = 'failing'", sql)
        self.assertNotIn("last_heartbeat <", sql)

    def test_recovery_prioritizes_due_ownerless_failure(self) -> None:
        sql = _normalized_sql(
            ingestion_lease_queries.CLAIM_RECOVERABLE_LEASES_SQL
        )

        self.assertIn("status = 'failing'::public.feed_status", sql)
        self.assertIn("worker_id IS NULL", sql)
        self.assertIn("last_heartbeat IS NULL", sql)
        self.assertIn("retry_after IS NULL OR retry_after <= NOW()", sql)
        self.assertIn("status = 'active'::public.feed_status", sql)
        self.assertIn("last_heartbeat < NOW() - $4::interval", sql)
        self.assertIn("END AS recovery_priority", sql)
        self.assertIn(
            "ORDER BY recovery_priority, source_type, lease_key",
            sql,
        )

    def test_claim_updates_only_generation_fields(self) -> None:
        required = (
            "status = 'active'::public.feed_status",
            "worker_id = $2",
            "fencing_token = leases.fencing_token + 1",
            "last_heartbeat = NOW()",
            "retry_after = NULL",
            "updated_at = NOW()",
        )
        retained = (
            "failure_count =",
            "status_reason =",
            "status_reason_detail =",
            "membership_revision =",
        )

        for query in (
            ingestion_lease_queries.CLAIM_UNCLAIMED_LEASES_SQL,
            ingestion_lease_queries.CLAIM_RECOVERABLE_LEASES_SQL,
        ):
            sql = _normalized_sql(query)
            for fragment in required:
                self.assertIn(fragment, sql)
            for fragment in retained:
                self.assertNotIn(fragment, sql)
            for projected in ("failure_count", "status_reason"):
                self.assertIn(projected, sql)
            for omitted in (
                "last_heartbeat",
                "retry_after",
                "status_reason_detail",
                "membership_revision",
                "updated_at",
            ):
                self.assertNotIn(f"leases.{omitted}", sql)

    def test_claims_project_no_child_owner_or_durable_cursor(self) -> None:
        for query in (
            ingestion_lease_queries.CLAIM_UNCLAIMED_LEASES_SQL,
            ingestion_lease_queries.CLAIM_RECOVERABLE_LEASES_SQL,
        ):
            sql = _normalized_sql(query).lower()
            self.assertNotIn("last_bookmark_time", sql)
            self.assertNotIn("last_processed_filename", sql)
            self.assertNotIn("lease_cursor", sql)


class TestLeaseControlQueryContract(unittest.TestCase):
    """Tests for exact-grant heartbeat and neutral release SQL."""

    def test_heartbeat_locks_in_identity_order_and_checks_full_grant(
        self,
    ) -> None:
        sql = _normalized_sql(
            ingestion_lease_queries.RENEW_LEASE_HEARTBEATS_SQL
        )

        self.assertNotIn("WITH ORDINALITY", sql)
        self.assertNotIn("lock_ordinal", sql)
        self.assertIn("ORDER BY leases.source_type, leases.lease_key", sql)
        self.assertIn("FOR NO KEY UPDATE OF leases", sql)
        self.assertIn(
            "current_state.worker_id = current_state.owner_worker_id",
            sql,
        )
        self.assertIn(
            "current_state.fencing_token = "
            "current_state.requested_fencing_token",
            sql,
        )
        self.assertIn(
            "current_state.status = 'active'::public.feed_status",
            sql,
        )
        self.assertIn("last_heartbeat = NOW()", sql)
        self.assertIn("updated_at = NOW()", sql)
        set_clause = sql.split("SET", 1)[1].split("FROM current_state", 1)[0]
        self.assertNotIn("fencing_token =", set_clause)
        for omitted in (
            "failure_count",
            "last_heartbeat,",
            "membership_revision",
            "retry_after",
            "status_reason",
            "status_reason_detail",
            "updated_at,",
        ):
            self.assertNotIn(omitted, sql)

    def test_release_is_one_neutral_exact_grant_transition(self) -> None:
        sql = _normalized_sql(ingestion_lease_queries.RELEASE_LEASE_SQL)

        self.assertIn("FOR NO KEY UPDATE", sql)
        self.assertIn("current_state.worker_id = $3", sql)
        self.assertIn("current_state.fencing_token = $4", sql)
        self.assertIn(
            "current_state.status = 'active'::public.feed_status",
            sql,
        )
        self.assertIn("status = 'unclaimed'::public.feed_status", sql)
        self.assertIn("worker_id = NULL", sql)
        self.assertIn("last_heartbeat = NULL", sql)
        self.assertIn("updated_at = NOW()", sql)
        self.assertNotIn("failure_count =", sql)
        self.assertNotIn("status_reason =", sql)
        for omitted in (
            "last_heartbeat,",
            "membership_revision",
            "retry_after",
            "status_reason_detail",
            "updated_at,",
        ):
            self.assertNotIn(omitted, sql)

    def test_no_worker_only_bulk_release_or_internal_retry_surface(
        self,
    ) -> None:
        store_source = pathlib.Path(
            "backend/pipeline/storage/ingestion_lease_store.py"
        ).read_text()

        self.assertNotIn("release_all", store_source)
        self.assertNotIn("release_leases_batch", store_source)
        self.assertNotIn("tenacity", store_source)
        self.assertNotIn("@retry", store_source)
        self.assertNotIn("sqlstate", store_source.lower())
        self.assertNotIn("while True", store_source)

    def test_public_control_methods_have_closed_results(self) -> None:
        annotations = {
            name: inspect.signature(
                getattr(ingestion_lease_store.IngestionLeaseStore, name)
            ).return_annotation
            for name in (
                "claim_unclaimed",
                "claim_recoverable",
                "renew_heartbeats",
                "release",
            )
        }

        for annotation in annotations.values():
            self.assertIsNot(annotation, inspect.Signature.empty)


class TestGrantRejectionContract(unittest.TestCase):
    """Tests for the shared exact-grant rejection value."""

    def test_rejection_reasons_are_closed(self) -> None:
        self.assertEqual(
            {
                reason.value
                for reason in ingestion_lease_store.GrantRejectionReason
            },
            {
                "missing",
                "owner_mismatch",
                "fence_mismatch",
                "status_ineligible",
            },
        )

    def test_rejection_is_frozen_and_slotted(self) -> None:
        rejection = ingestion_lease_store.GrantRejected(
            ingestion_lease_store.GrantRejectionReason.MISSING,
            None,
        )

        self.assertTrue(hasattr(type(rejection), "__slots__"))
        with self.assertRaises(dataclasses.FrozenInstanceError):
            rejection.snapshot = None  # type: ignore[misc]  # ty: ignore[invalid-assignment]


class TestMembershipSnapshotContract(unittest.TestCase):
    """Tests for authoritative immutable membership snapshot SQL."""

    def test_member_identity_has_no_revision_or_lease_projection(self) -> None:
        fields = {
            field.name
            for field in dataclasses.fields(
                ingestion_lease_store.LeaseMemberIdentity
            )
        }

        self.assertEqual(
            fields,
            {
                "feed_id",
                "source_type",
                "source_feed_id",
                "sid",
                "group_id",
            },
        )
        self.assertNotIn("membership_revision", fields)
        self.assertNotIn("owner_worker_id", fields)
        self.assertNotIn("fencing_token", fields)

    def test_membership_uses_maintained_index_identity_and_order(self) -> None:
        sql = _normalized_sql(
            ingestion_lease_queries.LOAD_BCFY_CALLS_MEMBERSHIP_SQL
        )

        self.assertIn("fp.bcfy_calls_sid", sql)
        self.assertIn("fp.bcfy_calls_group_id", sql)
        self.assertIn("fp.source_feed_id", sql)
        self.assertIn("fp.source_type = 'bcfy_calls'", sql)
        self.assertIn("fp.bcfy_calls_is_trunked IS TRUE", sql)
        self.assertIn("ORDER BY fp.bcfy_calls_group_id, fp.feed_id", sql)
        self.assertNotIn("split_part", sql.lower())
        self.assertNotRegex(
            sql.lower(), r"bcfy_calls_(sid|group_id)::(int|bigint)"
        )

    def test_revision_is_snapshot_state_not_a_lifecycle_fence(self) -> None:
        lifecycle_queries = (
            ingestion_lease_queries.CLAIM_UNCLAIMED_LEASES_SQL,
            ingestion_lease_queries.CLAIM_RECOVERABLE_LEASES_SQL,
            ingestion_lease_queries.RENEW_LEASE_HEARTBEATS_SQL,
            ingestion_lease_queries.RELEASE_LEASE_SQL,
        )

        for query in lifecycle_queries:
            sql = _normalized_sql(query)
            self.assertNotIn("membership_revision = $", sql)
            self.assertNotIn("membership_revision = input", sql)
            self.assertNotIn("membership_revision = current", sql)

    def test_membership_queries_are_storage_only_and_inert(self) -> None:
        query_text = pathlib.Path(
            "backend/pipeline/storage/ingestion_lease_queries.py"
        ).read_text()

        for token in (
            "CREATE TABLE",
            "ALTER TABLE",
            "CREATE TRIGGER",
            "INSERT INTO ingestion_leases",
            "pg_advisory",
            "LISTEN ",
            "SET LOCAL",
        ):
            self.assertNotIn(token, query_text)


if __name__ == "__main__":
    unittest.main()
