-- Replace the Phase 1 actor vocabulary before Phase 2 emits audit rows.
-- Legacy system:* rows must be explicitly cleaned or remapped first.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM feed_audit_events
        WHERE actor_id LIKE 'system:%'
        LIMIT 1
    ) THEN
        RAISE EXCEPTION
            'feed_audit_events contains legacy system: actor_id values; clean or remap those rows before enabling Phase 2 writers';
    END IF;

    ALTER TABLE feed_audit_events
        DROP CONSTRAINT IF EXISTS feed_audit_events_actor_id_check;

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

    INSERT INTO feed_audit_event_sequences (feed_id, next_sequence)
    SELECT feed_id, MAX(feed_sequence) + 1
    FROM feed_audit_events
    GROUP BY feed_id
    ON CONFLICT (feed_id) DO UPDATE
    SET next_sequence = GREATEST(
            feed_audit_event_sequences.next_sequence,
            EXCLUDED.next_sequence
        ),
        updated_at = NOW();
END $$;
