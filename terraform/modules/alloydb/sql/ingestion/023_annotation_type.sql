-- Idempotent: DO block to safely define the ANNOTATION_TYPE ENUM.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'annotation_type') THEN
        CREATE TYPE ANNOTATION_TYPE AS ENUM (
            'TRANSCRIPT',
            'EVALUATION'
        );
    END IF;
END
$$;

-- Idempotent: only convert annotations.type when it is not already the enum.
-- An unguarded ALTER COLUMN ... TYPE rebuilds the indexes on this column
-- under an ACCESS EXCLUSIVE lock on every replay, even when it is a no-op.
DO $$
BEGIN
    IF (
        SELECT atttypid
        FROM pg_attribute
        WHERE attrelid = 'public.annotations'::regclass
          AND attname = 'type'
    ) <> 'annotation_type'::regtype THEN
        ALTER TABLE annotations
            ALTER COLUMN type TYPE ANNOTATION_TYPE USING type::ANNOTATION_TYPE;
    END IF;
END
$$;
