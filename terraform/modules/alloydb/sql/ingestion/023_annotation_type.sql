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

-- Alter annotations table to use ANNOTATION_TYPE enum.
ALTER TABLE annotations 
    ALTER COLUMN type TYPE ANNOTATION_TYPE USING type::ANNOTATION_TYPE;