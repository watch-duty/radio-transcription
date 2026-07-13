# Simplify Ingestion Schema Migrations Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Preserve the Phase 1 lease and Broadcastify Calls schema behavior while removing brittle catalog-shape validation and recovery fixtures.

**Architecture:** Keep database constraints and triggers as the source of truth. Make migrations replay-safe with ordinary PostgreSQL DDL, and verify only observable behavior in CI: migrations apply twice, invalid lease state is rejected, and immutable lease operations remain blocked.

**Tech Stack:** PostgreSQL SQL migrations, GitHub Actions, Bash/psql.

## Global Constraints

- Do not touch unrelated uncommitted ingestion runtime files in this worktree.
- Do not assert PostgreSQL catalog OIDs, decompiled expressions, trigger bitmasks, or exact diagnostic strings.
- Keep the existing table constraints, membership state machine, and always-enabled lease guards.
- Preserve stacked CI work added after PR #984.

---

### Task 1: Simplify the migrations

**Files:**
- Modify: `terraform/modules/alloydb/sql/ingestion/031_ingestion_leases.sql`
- Modify: `terraform/modules/alloydb/sql/ingestion/032_ingestion_lease_guards.sql`
- Modify: `terraform/modules/alloydb/sql/ingestion/033_feed_properties_bcfy_calls_membership.sql`
- Modify: `terraform/modules/alloydb/sql/ingestion/034_validate_feed_properties_bcfy_calls_membership.sql`
- Delete: `terraform/modules/alloydb/sql/ingestion/035_feed_properties_bcfy_calls_membership_index_preflight.sql`
- Keep: `terraform/modules/alloydb/sql/ingestion/036_feed_properties_bcfy_calls_membership_index.sql`
- Delete: `terraform/modules/alloydb/sql/ingestion/037_feed_properties_bcfy_calls_membership_index_postflight.sql`

**Interfaces:**
- Consumes: existing `feed_status`, `source_types`, and `feed_properties` schema.
- Produces: `ingestion_leases`, lease guard triggers, membership columns/check, and the partial membership index.

- [x] Remove the exact `pg_catalog` validator following the `ingestion_leases` table DDL.
- [x] Replace the lease-guard catalog inspection with one explicit transaction containing `CREATE OR REPLACE FUNCTION`, `DROP TRIGGER IF EXISTS`, `CREATE TRIGGER`, and `ENABLE ALWAYS TRIGGER` statements.
- [x] Retain only the three `ADD COLUMN IF NOT EXISTS` statements and a minimal name-existence guard around the membership `CHECK` constraint.
- [x] Reduce validation migration 034 to:

```sql
ALTER TABLE public.feed_properties
    VALIDATE CONSTRAINT feed_properties_bcfy_calls_membership_check;
```

- [x] Delete the index preflight and postflight migrations; rely on the existing `CREATE INDEX CONCURRENTLY IF NOT EXISTS` migration.

### Task 2: Replace structural contracts with behavioral smoke coverage

**Files:**
- Delete: `backend/pipeline/common/tests/test_phase_1_lease_migration_contract.py`
- Delete: `terraform/modules/alloydb/sql/ci/phase_1_schema_contract.sql`
- Create: `terraform/modules/alloydb/sql/ci/phase_1_behavior_check.sql`
- Modify: `.github/workflows/ci.yml`
- Modify: `terraform/modules/alloydb/sql/ci/hot_protection_check.sql`

**Interfaces:**
- Consumes: the migrated PostgreSQL schema.
- Produces: a transaction-scoped smoke check with no persisted fixtures.

- [x] Add a compact SQL smoke check that inserts a valid lease and catches `check_violation` for ownerless active state, identity changes, fencing regression, and deletion.
- [x] Restore the existing PostgreSQL 16 HOT-protection job shape, apply migrations twice, and run the behavior check before the existing HOT guard.
- [x] Remove the PostgreSQL catalog/recovery matrix fixtures while leaving the later `ingestion-lease-storage-postgres` job unchanged.

### Task 3: Verify the reduced contract

**Files:**
- Verify all files modified in Tasks 1 and 2.

**Interfaces:**
- Consumes: final working-tree changes.
- Produces: evidence that migrations replay and behavior remains enforced.

- [x] Run `git diff --check` and scan for stale references to deleted validation files.
- [x] Apply all non-`pg_cron` ingestion migrations twice to PostgreSQL 16 and run `phase_1_behavior_check.sql` plus `hot_protection_check.sql`.
- [x] Run the focused existing Python regression tests affected by the PR and confirm unrelated dirty files were not changed.
