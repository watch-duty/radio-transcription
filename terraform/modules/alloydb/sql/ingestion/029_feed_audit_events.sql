-- Add canonical diagnostic detail for current feed state.
-- Existing rows remain NULL until application code records or clears detail.
-- The text is bounded for durable diagnostic storage and is not indexed.
ALTER TABLE feeds
    ADD COLUMN IF NOT EXISTS status_reason_detail TEXT;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.table_constraints
        WHERE table_schema = current_schema()
          AND table_name = 'feeds'
          AND constraint_name = 'feeds_status_reason_detail_length'
    ) THEN
        ALTER TABLE feeds
            ADD CONSTRAINT feeds_status_reason_detail_length
            CHECK (
                status_reason_detail IS NULL
                OR char_length(status_reason_detail) <= 2048
            );
    END IF;
END $$;

-- Per-feed sequence counter foundation for later transactional writers.
-- No feed foreign key is used because audit history must survive hard deletes.
CREATE TABLE IF NOT EXISTS feed_audit_event_sequences (
    feed_id       UUID PRIMARY KEY,
    next_sequence BIGINT NOT NULL DEFAULT 1,
    updated_at    TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- Durable audit rows for meaningful feed mutations.
CREATE TABLE IF NOT EXISTS feed_audit_events (
    id                   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    feed_id              UUID NOT NULL,
    feed_name            VARCHAR(255) NOT NULL,
    source_type          TEXT NOT NULL REFERENCES source_types(slug),
    action               TEXT NOT NULL,
    actor_id             TEXT NOT NULL,
    occurred_at          TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    feed_sequence        BIGINT NOT NULL,
    status               feed_status,
    status_reason        TEXT,
    status_reason_detail TEXT,
    before_values        JSONB NOT NULL DEFAULT '{}'::jsonb,
    after_values         JSONB NOT NULL DEFAULT '{}'::jsonb,
    metadata             JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at           TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.table_constraints
        WHERE table_schema = current_schema()
          AND table_name = 'feed_audit_events'
          AND constraint_name = 'feed_audit_events_action_check'
    ) THEN
        ALTER TABLE feed_audit_events
            ADD CONSTRAINT feed_audit_events_action_check
            CHECK (
                action IN (
                    'feed.created',
                    'feed.updated',
                    'feed.deactivated',
                    'feed.reset',
                    'feed.deleted',
                    'feed.failure_reported',
                    'feed.quarantined',
                    'feed.recovered'
                )
            );
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.table_constraints
        WHERE table_schema = current_schema()
          AND table_name = 'feed_audit_events'
          AND constraint_name = 'feed_audit_events_actor_id_check'
    ) THEN
        ALTER TABLE feed_audit_events
            ADD CONSTRAINT feed_audit_events_actor_id_check
            CHECK (
                char_length(actor_id) <= 512
                AND (
                    actor_id = 'unknown:unknown'
                    OR (
                        actor_id LIKE 'user:google:%'
                        AND substring(
                            actor_id FROM char_length('user:google:') + 1
                        ) <> ''
                        AND substring(
                            actor_id FROM char_length('user:google:') + 1
                        ) !~ '[[:space:]]'
                    )
                    OR (
                        actor_id LIKE 'user-email:%'
                        AND substring(
                            actor_id FROM char_length('user-email:') + 1
                        ) <> ''
                        AND substring(
                            actor_id FROM char_length('user-email:') + 1
                        ) !~ '[[:space:]]'
                        AND substring(
                            actor_id FROM char_length('user-email:') + 1
                        ) LIKE '%@%'
                    )
                    OR (
                        actor_id LIKE 'service:%'
                        AND substring(
                            actor_id FROM char_length('service:') + 1
                        ) <> ''
                        AND substring(
                            actor_id FROM char_length('service:') + 1
                        ) !~ '[[:space:]]'
                    )
                    OR (
                        actor_id LIKE 'system:%'
                        AND substring(
                            actor_id FROM char_length('system:') + 1
                        ) <> ''
                        AND substring(
                            actor_id FROM char_length('system:') + 1
                        ) !~ '[[:space:]]'
                    )
                    OR (
                        actor_id LIKE 'job:%'
                        AND substring(
                            actor_id FROM char_length('job:') + 1
                        ) <> ''
                        AND substring(
                            actor_id FROM char_length('job:') + 1
                        ) !~ '[[:space:]]'
                    )
                    OR (
                        actor_id LIKE 'gcp-sa:%'
                        AND substring(
                            actor_id FROM char_length('gcp-sa:') + 1
                        ) <> ''
                        AND substring(
                            actor_id FROM char_length('gcp-sa:') + 1
                        ) !~ '[[:space:]]'
                    )
                )
            );
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.table_constraints
        WHERE table_schema = current_schema()
          AND table_name = 'feed_audit_events'
          AND constraint_name = 'feed_audit_events_sequence_positive'
    ) THEN
        ALTER TABLE feed_audit_events
            ADD CONSTRAINT feed_audit_events_sequence_positive
            CHECK (feed_sequence > 0);
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.table_constraints
        WHERE table_schema = current_schema()
          AND table_name = 'feed_audit_events'
          AND constraint_name = 'feed_audit_events_detail_length'
    ) THEN
        ALTER TABLE feed_audit_events
            ADD CONSTRAINT feed_audit_events_detail_length
            CHECK (
                status_reason_detail IS NULL
                OR char_length(status_reason_detail) <= 2048
            );
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.table_constraints
        WHERE table_schema = current_schema()
          AND table_name = 'feed_audit_events'
          AND constraint_name = 'feed_audit_events_feed_sequence_unique'
    ) THEN
        ALTER TABLE feed_audit_events
            ADD CONSTRAINT feed_audit_events_feed_sequence_unique
            UNIQUE (feed_id, feed_sequence);
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.table_constraints
        WHERE table_schema = current_schema()
          AND table_name = 'feed_audit_events'
          AND constraint_name = 'feed_audit_events_json_object_shape'
    ) THEN
        ALTER TABLE feed_audit_events
            ADD CONSTRAINT feed_audit_events_json_object_shape
            CHECK (
                jsonb_typeof(before_values) = 'object'
                AND jsonb_typeof(after_values) = 'object'
                AND jsonb_typeof(metadata) = 'object'
            );
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_feed_audit_events_feed_sequence
    ON feed_audit_events (feed_id, feed_sequence DESC);

CREATE INDEX IF NOT EXISTS idx_feed_audit_events_feed_occurred_at
    ON feed_audit_events (feed_id, occurred_at DESC, id DESC);

CREATE INDEX IF NOT EXISTS idx_feed_audit_events_occurred_at
    ON feed_audit_events (occurred_at);

CREATE INDEX IF NOT EXISTS idx_feed_audit_events_actor_id
    ON feed_audit_events (actor_id);
