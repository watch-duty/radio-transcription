-- AUTOCOMMIT
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_feed_properties_bcfy_calls_membership
ON public.feed_properties USING btree
    (bcfy_calls_sid, bcfy_calls_group_id, feed_id)
WHERE source_type = 'bcfy_calls'
  AND bcfy_calls_is_trunked IS TRUE;
