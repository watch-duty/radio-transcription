-- Validate existing rows after adding the membership check without rewriting
-- or backfilling membership data.
ALTER TABLE public.feed_properties
    VALIDATE CONSTRAINT feed_properties_bcfy_calls_membership_check;
