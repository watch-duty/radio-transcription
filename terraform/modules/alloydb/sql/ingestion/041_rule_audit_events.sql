ALTER TABLE rules
    ADD COLUMN IF NOT EXISTS audit_revision BIGINT NOT NULL DEFAULT 0;

-- Durable audit rows for meaningful rule mutations.
CREATE TABLE IF NOT EXISTS rule_audit_events (
    id                   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    -- Deliberately not a FOREIGN KEY to rules(id): audit history must remain
    -- queryable after an admin hard-deletes the current-state rule row.
    rule_id              UUID NOT NULL,
    action               TEXT NOT NULL,
    actor_id             TEXT NOT NULL,
    occurred_at          TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    rule_revision        BIGINT NOT NULL,
    before_values        JSONB NOT NULL DEFAULT '{}'::jsonb,
    after_values         JSONB NOT NULL DEFAULT '{}'::jsonb
);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.table_constraints
        WHERE table_schema = current_schema()
          AND table_name = 'rule_audit_events'
          AND constraint_name = 'rule_audit_events_action_check'
    ) THEN
        ALTER TABLE rule_audit_events
            ADD CONSTRAINT rule_audit_events_action_check
            CHECK (
                action IN (
                    'rule.created',
                    'rule.updated',
                    'rule.deleted'
                )
            );
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.table_constraints
        WHERE table_schema = current_schema()
          AND table_name = 'rule_audit_events'
          AND constraint_name = 'rule_audit_events_actor_id_check'
    ) THEN
        ALTER TABLE rule_audit_events
            ADD CONSTRAINT rule_audit_events_actor_id_check
            CHECK (
                char_length(actor_id) > 0
                AND char_length(actor_id) <= 512
                AND actor_id !~ '[[:space:]]'
            );
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.table_constraints
        WHERE table_schema = current_schema()
          AND table_name = 'rule_audit_events'
          AND constraint_name = 'rule_audit_events_revision_positive'
    ) THEN
        ALTER TABLE rule_audit_events
            ADD CONSTRAINT rule_audit_events_revision_positive
            CHECK (rule_revision > 0);
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.table_constraints
        WHERE table_schema = current_schema()
          AND table_name = 'rule_audit_events'
          AND constraint_name = 'rule_audit_events_rule_revision_unique'
    ) THEN
        ALTER TABLE rule_audit_events
            ADD CONSTRAINT rule_audit_events_rule_revision_unique
            UNIQUE (rule_id, rule_revision);
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.table_constraints
        WHERE table_schema = current_schema()
          AND table_name = 'rule_audit_events'
          AND constraint_name = 'rule_audit_events_json_object_shape'
    ) THEN
        ALTER TABLE rule_audit_events
            ADD CONSTRAINT rule_audit_events_json_object_shape
            CHECK (
                jsonb_typeof(before_values) = 'object'
                AND jsonb_typeof(after_values) = 'object'
            );
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_rule_audit_events_rule_occurred_at
    ON rule_audit_events (rule_id, occurred_at DESC, rule_revision DESC);

CREATE INDEX IF NOT EXISTS idx_rule_audit_events_occurred_at
    ON rule_audit_events (occurred_at);

CREATE INDEX IF NOT EXISTS idx_rule_audit_events_actor_id
    ON rule_audit_events (actor_id);
