# Broadcastify Calls SID authority handoff

These are manually executed, fail-closed operations for the one-time
`legacy_feed` to `sid_lease` authority cutover. They deliberately require a
fresh reviewed manifest rather than embedding environment-specific feed or SID
counts.

Run every command with `psql` connected to exactly one environment. Do not
reuse a manifest after the database, feed configuration, or status set has
changed.

## Forward cutover

1. Run `000_review_manifest.sql`. Record both rows from the single read-only
   snapshot:

   - `full`: all Calls feeds. This is the input to the membership backfill.
   - `eligible`: non-deactivated Calls feeds. This is the input to all
     authority operations.

2. Have the operator who reviewed the `full` output run
   `000_backfill_membership.sql` with its three `reviewed_total_*` values and
   `backfill_confirmed=CONFIRMED`. It only fills the nullable structured Calls
   membership fields. Re-run `000_review_manifest.sql` after this commit and
   record the new `eligible` row; status changes cause a safe abort rather
   than a partial handoff.

3. Run `001_preseed.sql` with that current `eligible` row. This is safe while
   legacy workers are live because it creates only dormant parent SID leases.
   Re-running it is safe only while every expected lease remains dormant.

4. Drain the legacy fleet through `CAP_BCFY_CALLS=0` and prove no legacy
   workers can claim or renew Calls feeds. Then refresh the manifest once
   more. Do not continue if the `eligible` values changed; repeat the prior
   safe preparation steps with the fresh values.

5. After an operator has independently verified every legacy process is
   absent, run `002_activate.sql` with the current `eligible` values and
   `process_absence_confirmed=CONFIRMED`. This is the authority handoff: it
   makes parent leases claimable and leaves child feeds claim-safe. It aborts
   if any SID lease is missing, extra, owned, or non-dormant.

6. Change the collector configuration to `bcfy_calls_authority_mode=sid_lease`
   with legacy Calls capacity still zero, deploy/restart the collectors, and
   run `004_verify.sql` with the same reviewed eligible manifest. It makes no
   persistent-table mutations and reports membership, parent leases, fences,
   and ownership.

## Rollback to legacy child authority

1. Stop every SID-lease collector and independently prove SID processes are
   absent.
2. Refresh `000_review_manifest.sql` and run `003_rollback_children.sql` with
   the current eligible manifest and `process_absence_confirmed=CONFIRMED`.
   It makes unowned active children claimable again and does not mutate lease
   rows.
3. Restore `legacy_feed` configuration and only then re-enable a nonzero
   legacy Calls capacity.

`003_rollback_children.sql` is intentionally not a reversal of parent lease
fencing. Preserve those lease rows for forensic safety; it only transfers
execution eligibility back to the child feeds after the SID fleet is stopped.
