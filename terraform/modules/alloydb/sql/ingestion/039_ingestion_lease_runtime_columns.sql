-- Forward-only repair for columns already required by the generic Lease
-- runtime.  Migration 031 is historical and must remain unchanged.
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

ALTER TABLE public.ingestion_leases
    ADD COLUMN IF NOT EXISTS unclaimed_since TIMESTAMPTZ;

ALTER TABLE public.ingestion_leases
    ADD COLUMN IF NOT EXISTS status_reason_updated_at TIMESTAMPTZ;

ALTER TABLE public.ingestion_leases
    ADD COLUMN IF NOT EXISTS audit_revision BIGINT NOT NULL DEFAULT 0;

-- PostgreSQL has no ADD CONSTRAINT IF NOT EXISTS.  The stable table-local
-- name is guarded explicitly and the exact definition is checked below.
DO $migration$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_constraint
        WHERE conrelid = 'public.ingestion_leases'::regclass
          AND conname = 'ingestion_leases_audit_revision_nonnegative'
    ) THEN
        ALTER TABLE public.ingestion_leases
            ADD CONSTRAINT ingestion_leases_audit_revision_nonnegative
            CHECK (audit_revision >= 0) NOT VALID;
    END IF;
END
$migration$;

ALTER TABLE public.ingestion_leases
    VALIDATE CONSTRAINT ingestion_leases_audit_revision_nonnegative;

DO $postflight$
DECLARE
    invalid_column_count INTEGER;
    audit_default TEXT;
    audit_constraint RECORD;
BEGIN
    SELECT COUNT(*)
      INTO invalid_column_count
      FROM (
          VALUES
              (
                  'unclaimed_since'::name,
                  'timestamp with time zone'::regtype,
                  FALSE
              ),
              (
                  'status_reason_updated_at'::name,
                  'timestamp with time zone'::regtype,
                  FALSE
              ),
              ('audit_revision'::name, 'bigint'::regtype, TRUE)
      ) AS expected(attname, atttypid, attnotnull)
      LEFT JOIN pg_catalog.pg_attribute AS attribute
        ON attribute.attrelid = 'public.ingestion_leases'::regclass
       AND attribute.attname = expected.attname
       AND NOT attribute.attisdropped
     WHERE attribute.attname IS NULL
        OR attribute.atttypid <> expected.atttypid
        OR attribute.attnotnull <> expected.attnotnull;

    IF invalid_column_count <> 0 THEN
        RAISE EXCEPTION
            'ingestion Lease runtime column catalog shape is invalid';
    END IF;

    SELECT pg_catalog.pg_get_expr(
               column_default.adbin,
               column_default.adrelid
           )
      INTO audit_default
      FROM pg_catalog.pg_attribute AS attribute
      LEFT JOIN pg_catalog.pg_attrdef AS column_default
        ON column_default.adrelid = attribute.attrelid
       AND column_default.adnum = attribute.attnum
     WHERE attribute.attrelid = 'public.ingestion_leases'::regclass
       AND attribute.attname = 'audit_revision'
       AND NOT attribute.attisdropped;

    IF audit_default IS DISTINCT FROM '0' THEN
        RAISE EXCEPTION
            'ingestion Lease audit_revision default is %, expected 0',
            audit_default;
    END IF;

    SELECT
        constraint_state.convalidated,
        pg_catalog.pg_get_constraintdef(constraint_state.oid, TRUE)
            AS definition
      INTO audit_constraint
      FROM pg_catalog.pg_constraint AS constraint_state
     WHERE constraint_state.conrelid =
               'public.ingestion_leases'::regclass
       AND constraint_state.conname =
               'ingestion_leases_audit_revision_nonnegative'
       AND constraint_state.contype = 'c';

    IF NOT FOUND
       OR NOT audit_constraint.convalidated
       OR audit_constraint.definition IS DISTINCT FROM
           'CHECK (audit_revision >= 0)' THEN
        RAISE EXCEPTION
            'ingestion Lease audit_revision constraint is missing or invalid';
    END IF;
END
$postflight$;

COMMIT;
