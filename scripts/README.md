# scripts/ — SLO runbooks

Non-CI runbook scripts for SLO contract verification. Invoke manually
during the release cycle or on demand after ops-team Terraform changes.
These scripts are operator-facing; they are NOT wired into CI and are
NOT auto-run by pre-commit.

## smoke_test_logs.py

**Purpose:** End-to-end verify that `chunk_ingested`, `call_download_failed`,
and `feed_quarantined` structured logs emit with the expected top-level
`json_fields` against the golden files under
`backend/pipeline/ingestion/tests/golden/`.

**Usage:**
```
python scripts/smoke_test_logs.py [--timeout 30]
```

**Expected output:** Exit 0 with `SMOKE PASS: 3/3 log events found` on stdout.

**Failure modes:**
- Exit 1 — at least one event missing or key-set mismatch against the
  golden file. Error lines on stderr name each missing/mismatched event.

## smoke_test_metric.py

**Purpose:** End-to-end verify that the `active_feed_count` metric reaches
Cloud Monitoring with the correct descriptor
(`custom.googleapis.com/ingestion/active_feed_count`), monitored-resource
type (`gce_instance`), and label schema (`{project_id, instance_id, zone}`).
Writes one point (value=42), waits 60s for propagation, reads it back via
`list_time_series`, asserts match.

**Usage:**
```
python scripts/smoke_test_metric.py \
    --project <dev-project-id> \
    --confirm-project <dev-project-id> \
    [--instance-id <id> --zone <zone>]
```

On-GCE: omit `--instance-id`/`--zone`; script probes metadata server.
Off-GCE (laptop, CI container): pass both. Synthetic path validates
descriptor + write + read-back but does NOT prove GCE-auth (IAM /
workload-identity). Use an on-GCE invocation when in doubt.

**Safety:** Two independent gates defend against production writes:
1. `--project` and `--confirm-project` are both required; the script
   refuses to run if they don't match (exit 2) BEFORE any GCP call.
2. An interactive `CONTINUE? [y/N]` prompt echoes the project-id once
   more before the write.

**Expected output:** Exit 0 with `SMOKE PASS: point written and read back`.

**Failure modes:**
- Exit 1 — descriptor/label/value mismatch on read-back OR metadata server
  unreachable when `--instance-id`/`--zone` not provided.
- Exit 2 — `--project` does not equal `--confirm-project`.
- Exit 3 — interactive prompt declined (typed anything other than `y`/`Y`).
- Exit 4 — `list_time_series` returned no points within the 60s window.

## Refreshing terraform-snapshots/slo_alerts.json

When `test_snapshot_is_not_stale` warns (>60 days old) or fails (>90 days
old), refresh the hand-extracted snapshot from the ops-team Terraform repo:

1. Pull the latest ops-team Terraform repo (external; typically a
   separate git repository owned by the ops / platform team).
2. Visually diff the alert-filter strings (event types, metric types,
   logger path, label keys, monitored resource type) against the
   constants in `backend/pipeline/ingestion/slo_contract.py`. If any
   value differs, coordinate with ops-team before changing either side.
3. Update `terraform-snapshots/slo_alerts.json`:
   - Replace any drifted string values.
   - Update `source_commit` to `HAND_EXTRACTED_YYYY-MM-DD` using today's
     date (ISO-dashed — the canonical format; the parser tolerates the
     legacy underscore form via `.replace("_", "-")`).
4. Re-run the lint tests:
   ```
   uv run python -m pytest backend/pipeline/ingestion/tests/test_slo_contract_lint.py -v
   ```
   All tests in `TestTerraformSnapshotMatchesSloContract` must pass
   (value-match tests + `test_monitored_resource_type_matches` +
   `test_snapshot_is_not_stale`).
