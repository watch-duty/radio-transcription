-- Durable, generic ingestion Lease identities. Phase 1 intentionally creates
-- no rows and exposes no runtime mutation path.
CREATE TABLE IF NOT EXISTS public.ingestion_leases (
    source_type TEXT NOT NULL,
    lease_key TEXT NOT NULL,
    status public.feed_status NOT NULL
        DEFAULT 'deactivated'::public.feed_status,
    worker_id UUID NULL,
    fencing_token BIGINT NOT NULL DEFAULT 0,
    last_heartbeat TIMESTAMPTZ NULL,
    failure_count INTEGER NOT NULL DEFAULT 0,
    retry_after TIMESTAMPTZ NULL,
    unclaimed_since TIMESTAMPTZ NULL,
    status_reason TEXT NULL,
    status_reason_detail TEXT NULL,
    status_reason_updated_at TIMESTAMPTZ NULL,
    audit_revision BIGINT NOT NULL DEFAULT 0,
    membership_revision BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT ingestion_leases_pkey
        PRIMARY KEY (source_type, lease_key),
    CONSTRAINT ingestion_leases_source_type_fkey
        FOREIGN KEY (source_type)
        REFERENCES public.source_types (slug),
    CONSTRAINT ingestion_leases_identity_nonempty
        CHECK (source_type <> '' AND lease_key <> ''),
    CONSTRAINT ingestion_leases_fencing_token_nonnegative
        CHECK (fencing_token >= 0),
    CONSTRAINT ingestion_leases_failure_count_nonnegative
        CHECK (failure_count >= 0),
    CONSTRAINT ingestion_leases_audit_revision_nonnegative
        CHECK (audit_revision >= 0),
    CONSTRAINT ingestion_leases_membership_revision_nonnegative
        CHECK (membership_revision >= 0),
    CONSTRAINT ingestion_leases_owner_heartbeat_pair
        CHECK ((worker_id IS NULL) = (last_heartbeat IS NULL)),
    CONSTRAINT ingestion_leases_active_owned
        CHECK (
            status <> 'active'::public.feed_status
            OR worker_id IS NOT NULL
        ),
    CONSTRAINT ingestion_leases_status_reason_detail_length
        CHECK (
            status_reason_detail IS NULL
            OR char_length(status_reason_detail) <= 2048
        )
);
