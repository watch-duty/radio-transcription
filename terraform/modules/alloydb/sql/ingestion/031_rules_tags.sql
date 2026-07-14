-- Add tags column to rules table and enforce strictly typed schema.

ALTER TABLE rules
    ADD COLUMN IF NOT EXISTS tags JSONB DEFAULT '[]'::jsonb;

-- Create function to validate tags schema.
CREATE OR REPLACE FUNCTION validate_rule_tags(tags JSONB)
RETURNS BOOLEAN AS $$
DECLARE
    elem JSONB;
BEGIN
    -- Allow NULL
    IF tags IS NULL THEN
        RETURN TRUE;
    END IF;

    -- Must be an array
    IF jsonb_typeof(tags) != 'array' THEN
        RETURN FALSE;
    END IF;

    -- Check each element
    FOR elem IN SELECT jsonb_array_elements(tags) LOOP
        -- Must be an object
        IF jsonb_typeof(elem) != 'object' THEN
            RETURN FALSE;
        END IF;
        -- Must have 'key' and 'value' and they must be strings
        IF NOT (elem ? 'key' AND elem ? 'value') THEN
            RETURN FALSE;
        END IF;
        IF jsonb_typeof(elem->'key') != 'string' OR jsonb_typeof(elem->'value') != 'string' THEN
            RETURN FALSE;
        END IF;
    END LOOP;

    RETURN TRUE;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Add constraint using the validation function if not exists.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.table_constraints
        WHERE table_name = 'rules' AND constraint_name = 'valid_rule_tags_schema'
    ) THEN
        ALTER TABLE rules ADD CONSTRAINT valid_rule_tags_schema CHECK (validate_rule_tags(tags));
    END IF;
END $$;
