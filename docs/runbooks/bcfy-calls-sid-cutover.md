# Broadcastify Calls whole-SID cutover and rollback

**Runbook version:** 1.0  
**Status:** reviewed procedure; production gates remain pending  
**Scope:** the existing production regional ingestion MIG and existing AlloyDB
timeline only

This is the only authority-transition procedure. It is a stop-the-world state
machine: stage, suppress restart, stop/prove, activate, and start the same
frozen slots. Never infer an earlier gate from a later healthy observation and
never skip a state. The ordinary app rollout is forbidden as a cutover or
rollback mechanism because it recreates instances sequentially and can overlap
legacy and SID authority.

The process fence is authoritative. Database rows and logs are diagnostics,
not proof that a process cannot issue another request. Fixtures do not prove
process absence: fixtures do not prove process absence in the managed fleet,
and a tabletop does not replace the passing real multi-VM rehearsal.

## Absolute entry conditions

Stop before any production mutation unless all conditions are true:

1. `.planning/phases/07-production-verification-and-exclusive-cutover/07-NONPROD-REHEARSAL.md`
   remains a passing, independently reviewed **hard production prerequisite**.
   Its real two-VM/two-slot graceful, force, SSH ambiguity, numeric-ID
   replacement, same-slot restart, and exact control-restoration evidence may
   not be substituted with static tests or a tabletop.
   The rehearsal's public/deployment SHAs predate this runbook's candidate
   commits; they are not evidence of exact candidate equivalence. Two reviewers
   must approve a bounded delta from rehearsal public `e313ef52...` and
   deployment `03759b43...` through the exact candidate commits and prove that
   no intervening change invalidates its process-fence, image, or control
   assumptions.
2. The approved change names an operator, independent reviewer, incident owner,
   UTC window, immutable external evidence location, rollback authority, public
   commit, deployment commit, immutable image digest, and operation-job
   revision.
3. Both repositories are clean at the reviewed commits; all Phase 7 tests and
   both ASVS L1 reviews have no unmitigated HIGH finding.
4. The change owner has frozen Calls membership writers, the infrastructure
   workflow, the application workflow, manual MIG actions, and any scheduled
   automation that can mutate the production fleet. A queued or active deploy
   blocks entry.
5. The backup/change window is approved. No command in this document displays
   a database password, entire container environment, signed provider URL, or
   raw private provider payload.

## Command preflight and external evidence workspace

The command shapes were reviewed against official current Google Cloud SDK and
AlloyDB documentation resolved through Context7 as
`/websites/cloud_google_sdk` and `/websites/cloud_google_alloydb`, then checked
against Google Cloud SDK 565.0.0 (core 2026.04.10). Before each execution,
record the installed version and confirm the named flags still exist. A shape
mismatch is a stop; do not improvise a nearby command.

```bash
set -euo pipefail

: "${PROJECT:?set the reviewed production project ID}"
: "${REGION:?set the reviewed production region}"
: "${MIG:?set the reviewed regional MIG name}"
: "${AUTOSCALER:?set the reviewed regional autoscaler name}"
: "${SERVICE:?set the reviewed systemd service prefix}"
: "${OP_JOB:?set the reviewed controlled operation job name}"
: "${ARTIFACT_DIR:?set the approved external evidence directory}"

test "$SERVICE" = "icecast-collector-prod"
mkdir -p "$ARTIFACT_DIR"
gcloud version --format=json >"$ARTIFACT_DIR/gcloud-version.json"

gcloud compute instance-groups managed update-autoscaling --help \
  | grep -E -- '--mode.*off|--mode.*on'
gcloud compute instance-groups managed update --help \
  | grep -E -- '--clear-autohealing|--default-action-on-vm-failure'
gcloud compute instance-groups managed list-instances --help \
  | grep -E -- '--region|--format'
gcloud logging read --help | grep -E -- '--limit|--order|--format'
gcloud run jobs execute --help | grep -E -- '--args|--wait'
```

Cloud Logging's current CLI exposes no page-token input. Every evidence read
below is therefore one complete CLI collection over an exact half-open window,
with the CLI internally traversing service pages. The command uses a
predeclared high result limit and fails closed if that limit is reached; there
is no invented `page-token` loop. The single collection is represented as one
complete page in the checked reducer envelope.

Raw cloud JSON, raw logs, environment files, passwords, signed URLs, and
provider payloads stay in the approved immutable external artifact/change
system. Git retains only the secret-free index template.

## Shared primitive A: capture and compare the frozen fleet

First capture the maintained MIG response. It must contain 2–10 settled members
at the current production policy, but the frozen set is whatever complete live
inventory exists after autoscaling decisions stop; never assume two VMs.

```bash
gcloud compute instance-groups managed list-instances "$MIG" \
  --project="$PROJECT" --region="$REGION" --format=json \
  >"$ARTIFACT_DIR/mig-members-before.json"

jq -e 'length >= 2 and length <= 10 and
       all(.[]; .currentAction == "NONE")' \
  "$ARTIFACT_DIR/mig-members-before.json"

: >"$ARTIFACT_DIR/frozen-instances.ndjson"
jq -r '.[] | [.instance | split("/")[-1],
               .instance | split("/")[-3],
               .currentAction,
               (.version.instanceTemplate // "")] | @tsv' \
  "$ARTIFACT_DIR/mig-members-before.json" \
| while IFS=$'\t' read -r name zone action template; do
    gcloud compute instances describe "$name" \
      --project="$PROJECT" --zone="$zone" --format=json \
    | jq -c --arg action "$action" --arg template "$template" \
        '{instance_name:.name, zone:(.zone|split("/")[-1]),
          numeric_instance_id:(.id|tostring), machine_type:.machineType,
          current_action:$action, instance_template:$template,
          worker_indices:[1,2]}' \
    >>"$ARTIFACT_DIR/frozen-instances.ndjson"
  done

jq -s 'sort_by(.numeric_instance_id)' \
  "$ARTIFACT_DIR/frozen-instances.ndjson" \
  >"$ARTIFACT_DIR/frozen-instances.json"
jq -e 'length > 0 and all(.[];
       .current_action == "NONE" and .worker_indices == [1,2] and
       (.numeric_instance_id | test("^[0-9]+$")))' \
  "$ARTIFACT_DIR/frozen-instances.json"
sha256sum "$ARTIFACT_DIR/frozen-instances.json" \
  >"$ARTIFACT_DIR/frozen-instances.sha256"
```

For every later gate, re-run `list-instances` and every instance `describe`,
canonicalize to the same fields, and compare both sorted JSON and SHA-256. An
SSH-unreachable VM, a disappeared member, a replaced VM with the same name but
a different numeric instance ID, a new member, a missing member, or any
`currentAction` other than `NONE` is ambiguous authority and blocks the next
mutation.

## Shared primitive B: suppress restart and atomically stage one mode

Execute the following once on every frozen VM after rechecking its numeric
instance ID. It disables boot-start without stopping current workers, applies a
runtime `Restart=no` override, and replaces exactly two non-secret keys through
an atomic rename. Use `TARGET_MODE=sid_lease` for cutover and
`TARGET_MODE=legacy_feed` for rollback.

```bash
: "${TARGET_MODE:?set sid_lease or legacy_feed}"
case "$TARGET_MODE" in sid_lease|legacy_feed) ;; *) exit 64 ;; esac

jq -r '.[] | [.instance_name,.zone,.numeric_instance_id] | @tsv' \
  "$ARTIFACT_DIR/frozen-instances.json" \
| while IFS=$'\t' read -r name zone expected_id; do
    live_id=$(gcloud compute instances describe "$name" \
      --project="$PROJECT" --zone="$zone" --format='value(id)')
    test "$live_id" = "$expected_id"

    gcloud compute ssh "$name" --project="$PROJECT" --zone="$zone" \
      --tunnel-through-iap --command="sudo env SERVICE='$SERVICE' \
      TARGET_MODE='$TARGET_MODE' python3 - <<'PY'
import os
from pathlib import Path

service = os.environ['SERVICE']
target_mode = os.environ['TARGET_MODE']
path = Path(f'/etc/container-env/{service}.env')
targets = {
    'WORKER_PROFILE': 'mixed-dormant',
    'BCFY_CALLS_AUTHORITY_MODE': target_mode,
}

for index in (1, 2):
    unit = f'{service}@{index}.service'
    dropin = Path(f'/run/systemd/system/{unit}.d')
    dropin.mkdir(parents=True, exist_ok=True)
    (dropin / 'phase7-no-restart.conf').write_text(
        '[Service]\\nRestart=no\\n', encoding='utf-8'
    )
    os.system(f'systemctl disable {unit} >/dev/null')

original = path.read_text(encoding='utf-8').splitlines()
kept = [line for line in original if line.split('=', 1)[0] not in targets]
rendered = kept + [f'{key}={value}' for key, value in targets.items()]
temporary = path.with_name(path.name + '.phase7-new')
temporary.write_text('\\n'.join(rendered) + '\\n', encoding='utf-8')
temporary.chmod(0o600)
temporary.replace(path)
os.system('systemctl daemon-reload')
PY
      for index in 1 2; do
        unit=\"$SERVICE@\${index}.service\"
        test \"\$(systemctl show \"\$unit\" -p Restart --value)\" = no
        test \"\$(systemctl is-enabled \"\$unit\" 2>/dev/null || true)\" = disabled
      done
      awk -F= '\$1==\"WORKER_PROFILE\" ||
                 \$1==\"BCFY_CALLS_AUTHORITY_MODE\" {print}' \
        /etc/container-env/$SERVICE.env | sort
      test \"\$(grep -Ec '^WORKER_PROFILE=mixed-dormant$' \
        /etc/container-env/$SERVICE.env)\" -eq 1
      test \"\$(grep -Ec '^BCFY_CALLS_AUTHORITY_MODE=$TARGET_MODE$' \
        /etc/container-env/$SERVICE.env)\" -eq 1"
  done
```

Capture worker MainPID, exact container ID, image ID/digest, and environment-file
hash before and after staging. Every PID/container and the complete numeric-ID
fleet must be unchanged. Only the two key/value lines above may be printed;
never print or copy the whole environment.

## Shared primitive C: bounded stop and complete process proof

Issue non-blocking stops to every VM concurrently and record `STOP_T0`. Normal
runtime shutdown owns a 90-second graceful budget inside Docker's 100-second
and systemd's 120-second bounds. At T+90 seconds, recheck each exact slot and
force only the exact survivors after confirming their numeric instance ID. Do
not widen the target by name pattern. At T+120 seconds, stop waiting: any
survivor or uncertainty is an incident, not permission to continue.

```bash
STOP_T0=$(date -u +%Y-%m-%dT%H:%M:%SZ)
export STOP_T0

while IFS=$'\t' read -r name zone expected_id; do
  (
    live_id=$(gcloud compute instances describe "$name" \
      --project="$PROJECT" --zone="$zone" --format='value(id)')
    test "$live_id" = "$expected_id"
    gcloud compute ssh "$name" --project="$PROJECT" --zone="$zone" \
      --tunnel-through-iap \
      --command="sudo systemctl stop --no-block \
        '$SERVICE@1.service' '$SERVICE@2.service'"
  ) &
done < <(jq -r '.[] | [.instance_name,.zone,.numeric_instance_id] | @tsv' \
  "$ARTIFACT_DIR/frozen-instances.json")
wait
```

At T+90, create an exact survivor list from a proof observation. For each row
still having nonzero `MainPID`, an active/activating/deactivating unit, or an
exact running/stopped container, verify numeric identity again, then run only:

```bash
gcloud compute ssh "$name" --project="$PROJECT" --zone="$zone" \
  --tunnel-through-iap --command="sudo systemctl kill \
    --kill-who=all --signal=SIGKILL '$unit';
    sudo docker rm -f '$container' >/dev/null 2>&1 || true"
```

Re-run complete proof after force and before T+120. One machine-readable row is
required for every frozen `(numeric instance ID, worker index)`:

```json
{
  "instance_name": "external-only",
  "zone": "external-only",
  "expected_numeric_instance_id": "123",
  "live_numeric_instance_id": "123",
  "worker_index": 1,
  "unit": "icecast-collector-prod@1.service",
  "load_state": "loaded",
  "active_state": "inactive",
  "sub_state": "dead",
  "main_pid": 0,
  "container_name": "icecast-collector-prod-1",
  "container_exists": false,
  "current_action": "NONE",
  "observed_at_utc": "RFC3339"
}
```

The gate passes only when the row count is exactly `2 × frozen VM count`, each
expected and live numeric instance ID matches, both worker slots are present,
`LoadState=loaded`, `ActiveState` is `inactive` or a proved process-free
`failed`, `MainPID=0`, the exact container is absent even from `docker ps -a`,
the fleet set is unchanged, and `currentAction=NONE`. Unknown unit, unknown
container, including any unknown container, SSH failure, timeout, changed ID,
changed membership, or incomplete
output blocks. Re-run this proof immediately before and after activation, and
again before rollback child normalization.

The following function produces those rows without interpreting an SSH failure
as absence. Run it only after saving a fresh maintained-instance response and
proving its sorted member names equal the frozen set:

```bash
gcloud compute instance-groups managed list-instances "$MIG" \
  --project="$PROJECT" --region="$REGION" --format=json \
  >"$ARTIFACT_DIR/mig-members-proof.json"

FROZEN_NAMES=$(jq -c '[.[].instance_name] | sort' \
  "$ARTIFACT_DIR/frozen-instances.json")
CURRENT_NAMES=$(jq -c '[.[] | .instance | split("/")[-1]] | sort' \
  "$ARTIFACT_DIR/mig-members-proof.json")
test "$CURRENT_NAMES" = "$FROZEN_NAMES"

prove_slot() {
  name=$1 zone=$2 expected_id=$3 index=$4
  unit="$SERVICE@$index.service"
  container="$SERVICE-$index"
  live_id=$(gcloud compute instances describe "$name" \
    --project="$PROJECT" --zone="$zone" --format='value(id)')
  test "$live_id" = "$expected_id"
  action=$(jq -er --arg name "$name" '
    [.[] | select((.instance | split("/")[-1]) == $name) | .currentAction]
    | if length == 1 then .[0] else error("missing/duplicate member") end' \
    "$ARTIFACT_DIR/mig-members-proof.json")

  report=$(gcloud compute ssh "$name" --project="$PROJECT" --zone="$zone" \
    --tunnel-through-iap --command="set -eu
      sudo systemctl show '$unit' --no-page \
        -p LoadState -p ActiveState -p SubState -p MainPID
      sudo docker info >/dev/null
      ids=\$(sudo docker ps -aq --filter 'name=^/$container\$')
      if [ -z \"\$ids\" ]; then count=0;
      else count=\$(printf '%s\\n' \"\$ids\" | wc -l); fi
      printf 'ContainerCount=%s\\n' \"\$count\"")

  load_state=$(printf '%s\n' "$report" | awk -F= '$1=="LoadState"{print $2}')
  active_state=$(printf '%s\n' "$report" | awk -F= '$1=="ActiveState"{print $2}')
  sub_state=$(printf '%s\n' "$report" | awk -F= '$1=="SubState"{print $2}')
  main_pid=$(printf '%s\n' "$report" | awk -F= '$1=="MainPID"{print $2}')
  container_count=$(printf '%s\n' "$report" \
    | awk -F= '$1=="ContainerCount"{print $2}')
  test -n "$load_state" && test -n "$active_state" && test -n "$sub_state"
  case "$main_pid:$container_count" in
    *[!0-9:]*|:*|*:) exit 65 ;;
  esac

  jq -nc --arg instance_name "$name" --arg zone "$zone" \
    --arg expected_id "$expected_id" --arg live_id "$live_id" \
    --argjson worker_index "$index" --arg unit "$unit" \
    --arg load_state "$load_state" --arg active_state "$active_state" \
    --arg sub_state "$sub_state" --argjson main_pid "$main_pid" \
    --arg container_name "$container" \
    --argjson container_count "$container_count" --arg action "$action" \
    --arg observed_at_utc "$(date -u +%Y-%m-%dT%H:%M:%SZ)" '
      {instance_name:$instance_name, zone:$zone,
       expected_numeric_instance_id:$expected_id,
       live_numeric_instance_id:$live_id, worker_index:$worker_index,
       unit:$unit, load_state:$load_state, active_state:$active_state,
       sub_state:$sub_state, main_pid:$main_pid,
       container_name:$container_name,
       container_exists:($container_count != 0),
       current_action:$action, frozen_inventory_equal:true,
       observed_at_utc:$observed_at_utc}'
}

: >"$ARTIFACT_DIR/process-proof.ndjson"
while IFS=$'\t' read -r name zone expected_id; do
  for index in 1 2; do
    prove_slot "$name" "$zone" "$expected_id" "$index" \
      >>"$ARTIFACT_DIR/process-proof.ndjson"
  done
done < <(jq -r '.[] | [.instance_name,.zone,.numeric_instance_id] | @tsv' \
  "$ARTIFACT_DIR/frozen-instances.json")

jq -s 'sort_by(.expected_numeric_instance_id,.worker_index)' \
  "$ARTIFACT_DIR/process-proof.ndjson" \
  >"$ARTIFACT_DIR/process-proof.json"
EXPECTED_SLOT_COUNT=$((2 * $(jq 'length' \
  "$ARTIFACT_DIR/frozen-instances.json")))
jq -e --argjson expected "$EXPECTED_SLOT_COUNT" '
  length == $expected and
  all(.[];
    .expected_numeric_instance_id == .live_numeric_instance_id and
    (.worker_index == 1 or .worker_index == 2) and
    .load_state == "loaded" and
    (.active_state == "inactive" or .active_state == "failed") and
    .main_pid == 0 and .container_exists == false and
    .current_action == "NONE" and .frozen_inventory_equal == true)' \
  "$ARTIFACT_DIR/process-proof.json"
sha256sum "$ARTIFACT_DIR/process-proof.json" \
  >"$ARTIFACT_DIR/process-proof.sha256"
```

## Shared primitive D: controlled SQL job

The reviewed Cloud Run Job uses the schema-migrator identity, private VPC,
direct port 5432, `psql -X -v ON_ERROR_STOP=1`, a five-minute task timeout, and
zero retries. Each SQL transaction also bounds lock acquisition to 5 seconds
and statements to 30 seconds. It accepts only four closed operations. Always
use `--wait`, retain the terminal execution ID and bounded SQL report, and never
resubmit an activation whose outcome is unknown. Describe the exact execution
and run read-only `verify` first.

```bash
gcloud run jobs execute "$OP_JOB" --project="$PROJECT" --region="$REGION" \
  --wait --args=bcfy-calls-sid-operation,verify

gcloud run jobs execute "$OP_JOB" --project="$PROJECT" --region="$REGION" \
  --wait --args=bcfy-calls-sid-operation,preseed

gcloud run jobs execute "$OP_JOB" --project="$PROJECT" --region="$REGION" \
  --wait --args=bcfy-calls-sid-operation,activate,19,"$MANIFEST_DIGEST",CONFIRMED

gcloud run jobs execute "$OP_JOB" --project="$PROJECT" --region="$REGION" \
  --wait --args=bcfy-calls-sid-operation,rollback_children,CONFIRMED
```

The corresponding immutable SQL is
`001_preseed.sql`, `002_activate.sql`, `003_rollback_children.sql`, and
`004_verify.sql`. Do not delete Lease rows, do not reset Lease identity or
fences, and do not reduce any Lease or child fencing token.

## Shared primitive E: complete bootstrap and soak evidence

Use the approved sorted `EXPECTED_SIDS` JSON from the SQL manifest. A read is
scoped to the exact frozen numeric instance IDs, exact event types, and a
half-open UTC window. `RESULT_LIMIT=10000` exceeds the expected 3,600 steady
polls but remains fail closed.

```bash
: "${WINDOW_START:?set the inclusive UTC start}"
: "${WINDOW_END:?set the exclusive UTC end}"
: "${EXPECTED_SIDS:?set the sorted 19-SID JSON array}"
: "${EVENT_FILTER:?set the reviewed event-type Logging filter}"
: "${EVENT_TYPES_JSON:?set the matching sorted reducer event array}"
: "${RAW_EVIDENCE:?set the external raw JSON path}"
: "${EVIDENCE_ENVELOPE:?set the external reducer-envelope path}"

RESULT_LIMIT=10000
INSTANCE_FILTER=$(jq -r '
  map("resource.labels.instance_id=\\\"" + .numeric_instance_id + "\\\"")
  | join(" OR ")' "$ARTIFACT_DIR/frozen-instances.json")

POLL_FILTER='resource.type="gce_instance"
AND '"$EVENT_FILTER"'
AND timestamp>="'"$WINDOW_START"'"
AND timestamp<"'"$WINDOW_END"'"
AND ('"$INSTANCE_FILTER"')'

gcloud logging read "$POLL_FILTER" --project="$PROJECT" --order=asc \
  --limit="$RESULT_LIMIT" --format=json \
  >"$RAW_EVIDENCE"

RETURNED_COUNT=$(jq 'length' "$RAW_EVIDENCE")
test "$RETURNED_COUNT" -lt "$RESULT_LIMIT"
sha256sum "$RAW_EVIDENCE" >"$RAW_EVIDENCE.sha256"
```

Wrap that successful single complete CLI collection in the reducer's schema-v1
envelope. The CLI owns internal service pagination but exposes no page tokens;
therefore the envelope truthfully records one completed CLI collection, not a
fabricated API token chain:

```bash
FROZEN_IDS=$(jq -c '[.[].numeric_instance_id] | sort | unique' \
  "$ARTIFACT_DIR/frozen-instances.json")

jq --argjson frozen_ids "$FROZEN_IDS" \
   --argjson result_limit "$RESULT_LIMIT" \
   --arg window_start "$WINDOW_START" --arg window_end "$WINDOW_END" \
   --argjson event_types "$EVENT_TYPES_JSON" '
  . as $entries
  | {
      collection: {
        schema_version: 1,
        collection_complete: true,
        limit_reached: false,
        result_limit: $result_limit,
        returned_count: ($entries | length),
        query_event_types: $event_types,
        frozen_instance_ids: $frozen_ids,
        filter_instance_ids: $frozen_ids,
        page_count: 1,
        pages: [{
          page_number: 1,
          request_page_token: null,
          next_page_token: null,
          entry_count: ($entries | length),
          page_complete: true
        }],
        window_start: $window_start,
        window_end: $window_end
      },
      entries: $entries
    }' "$RAW_EVIDENCE" >"$EVIDENCE_ENVELOPE"
```

For bootstrap, set `EVENT_FILTER` to
`jsonPayload.event_type="bcfy_calls_sid_poll"`, `EVENT_TYPES_JSON` to
`["bcfy_calls_sid_poll"]`, and `EVIDENCE_ENVELOPE` to
`$ARTIFACT_DIR/bootstrap-envelope.json`. For soak, use the parenthesized OR
filter for `bcfy_calls_replay_window_truncated` and `bcfy_calls_sid_poll`, set
`EVENT_TYPES_JSON` to the same sorted two-value array, and write
`$ARTIFACT_DIR/soak-envelope.json`. Any nonzero CLI exit, reached limit,
malformed JSON, wrong scope, or incomplete log collection blocks evidence;
never treat a sample as complete.

Bootstrap invocation:

```bash
jq --argjson EXPECTED_SIDS "$EXPECTED_SIDS" \
  -f scripts/operations/bcfy_calls_sid/bootstrap.jq \
  "$ARTIFACT_DIR/bootstrap-envelope.json" \
  >"$ARTIFACT_DIR/bootstrap-result.json"
jq -e '.status == "pass" and .expected_sid_count == 19 and
       .observed_sid_count == 19 and (.BOOTSTRAP_END | type == "string")' \
  "$ARTIFACT_DIR/bootstrap-result.json"
```

The v1 bootstrap deadline is ten minutes after the last same-slot start. It is
longer than the process startup-pacing envelope and many normal ten-second poll
cadences; reaching it without exact 19/19 is a fail-closed incident/rollback
decision, not an indefinite wait.

Only after that passes, record a new `SOAK_START` strictly later than
`BOOTSTRAP_END`; set `SOAK_END=SOAK_START+30m`. The steady population is exactly
`[SOAK_START, SOAK_END)`, exactly 30 minutes, and therefore excludes bootstrap.
Collect both `bcfy_calls_sid_poll` and
`bcfy_calls_replay_window_truncated` for that window and invoke:

```bash
jq --argjson EXPECTED_SIDS "$EXPECTED_SIDS" \
  --arg BOOTSTRAP_END "$BOOTSTRAP_END" \
  --arg SOAK_START "$SOAK_START" --arg SOAK_END "$SOAK_END" \
  --argjson MISSING_SUCCESS_THRESHOLD_SECONDS 60 \
  --argjson CURSOR_LAG_THRESHOLD_SECONDS 300 \
  --argjson SUSTAINED_CONSECUTIVE_POLLS 6 \
  --argjson SUSTAINED_SECONDS 60 \
  -f scripts/operations/bcfy_calls_sid/soak.jq \
  "$ARTIFACT_DIR/soak-envelope.json" \
  >"$ARTIFACT_DIR/soak-result.json"
jq -e '.status == "pass"' "$ARTIFACT_DIR/soak-result.json"
```

The production v1 thresholds selected by this runbook are 60 seconds for a
missing qualifying success, 300 seconds for either Lease or maximum child Feed
cursor lag, and either six consecutive affected polls or 60 seconds for a
sustained hazard. Sixty seconds is six normal completion-paced polls; 300
seconds is the bounded replay floor beyond which delayed cursor progress risks
source-history loss. The operator and reviewer approve these values before
activation; they are not copied from fixture data and are never changed after
`SOAK_START`.

The aggregate gates run before bounded per-SID drill-down:

- exact 19-SID set and 2,700–3,600 logical events, or 90–120 logical polls/min;
- `SUM(http_attempt_count) / logical_poll_count <= 1.25`;
- `SUM(response_row_count) / SUM(response_distinct_audio_url_count) <= 1.25`
  over provider-observed responses, expressed as an integer inequality;
- no per-SID regression in valid `response_last_pos`;
- no missing successful poll gap over 60 seconds;
- no pressure, rejection, lag, invalid-lastPos, or reacquisition run lasting
  60 seconds or six consecutive polls;
- no replay-window truncation; and
- configured/process proof of zero legacy managed-fleet authority.

The amplification denominator is the sum of exact per-response distinct audio
URL counts. It is denominator-weighted and intentionally is not window-wide
cross-poll uniqueness; window-wide cross-poll uniqueness is not evaluated.
The latter would require a high-cardinality URL stream
or a new distributed sketch and lifecycle, and is deferred. The authority gate
combines the complete process fence, every `startup_pacing` record's
`bcfy_calls_authority_mode=sid_lease`, selected SID domain, profile/digest,
worker index/process ID, and static exclusive-mode tests. This configured/process
proof does not directly inspect a legacy wire selector; its claim is limited to
the managed frozen fleet.

## PREPARED_LEGACY

**Entry gate:** All absolute entry conditions pass. Public/deployment commits,
reviews, module pins, exact image digest, operation-job identity/IAM, backup
window, and change window are approved. Read-only `004_verify.sql` proves the
runtime Lease columns and guard constraint. Maintained projection completeness
is exact: 154 non-deactivated projected trunked Feeds, 19 derived SIDs, no
missing/duplicate tuple. The manifest deliberately includes structurally mapped
quarantined rows; activation separately requires at least one
active/unclaimed/failing eligible child for each SID and preserves quarantine.

**Mutation:** Run `001_preseed.sql` through the controlled job while legacy
workers remain live, then run it a second time as a no-op replay.

**Expected bounded output:** First execution inserts 0–19 missing dormant rows;
the replay inserts exactly 0. Both report exactly 19 SIDs/154 Feeds, the same
sorted manifest digest, and 19 deactivated, ownerless, heartbeat-free Leases.

**Evidence to retain:** Commits/reviews, image and module digests, schema report,
sorted SID/Feed manifest and SHA-256, two terminal execution IDs, bounded SQL
reports, change/backup approvals, and passing threat ledger.

**Stop condition:** Any count/digest drift, incomplete projection, unsafe Lease,
permission/job mismatch, failed replay, active/queued deploy, failed rehearsal,
or unmitigated HIGH stops the cutover with legacy authority unchanged.

**Rollback edge:** No authority changed. Leave valid dormant identities intact;
release the change window only after diagnosing the failed prerequisite.

**Accountability:** Operator: ____  Reviewer: ____  UTC: ____

## FROZEN_LEGACY

**Entry gate:** `PREPARED_LEGACY` evidence is approved, membership writers and
both deploy workflows are frozen, no run is queued/active, and original controls
are captured byte-for-byte.

**Mutation:** Capture autoscaler policy/mode, autohealing URL/delay, VM-failure
action, target size, versions/template/update policy, numeric fleet, unit state,
and workflow state. First set durable deployment maintenance with the exact
candidate image, `WORKER_PROFILE=mixed-dormant`, and the still-safe
`BCFY_CALLS_AUTHORITY_MODE=legacy_feed` through the saved-plan path, with
ordinary app dispatch suppressed. This first maintenance apply removes the
autoscaler, clears autohealing, and selects DO_NOTHING before any replacement
template can introduce SID authority. Compute rejects DO_NOTHING while an
autoscaler remains attached, even in OFF mode, so do not execute a stale
`mode=off` then DO_NOTHING sequence. Once the terminal apply proves the
autoscaler absent, the current SDK command below is an idempotent verification
of the desired MIG controls:

```bash
gcloud compute instance-groups managed update "$MIG" \
  --project="$PROJECT" --region="$REGION" --clear-autohealing \
  --default-action-on-vm-failure=do-nothing
```

Then use Shared primitive B with `TARGET_MODE=sid_lease` on every frozen VM.

**Expected bounded output:** The autoscaler makes no decision; durable
maintenance has no live autoscaler, no autohealing, and do-nothing failure
action. Each of exactly two units per VM is disabled with `Restart=no`; each
non-secret key occurs exactly once. Current PIDs, containers, image IDs,
numeric instance IDs, target/member set, and `currentAction=NONE` are unchanged.
The durable replacement template still says `legacy_feed`; only existing-host
env files are staged to `sid_lease`, inert until their current processes stop.

**Evidence to retain:** Original/frozen control JSON and hashes, deployment plan
and terminal apply ID, no-app-dispatch proof, frozen inventory/hash, per-slot
unit/env-key/PID/container/image report, and membership/deploy-freeze owners.

**Stop condition:** Failed durable apply, autohealing/repair/autoscaler remaining,
PID restart, identity drift, inaccessible VM, key duplication, template/image
mismatch, or any queued deploy blocks stopping workers. Restore captured controls
if safe; do not activate.

**Rollback edge:** Since legacy processes still own their loaded mode, restore
host env/unit state and captured controls, verify unchanged legacy PIDs, and
return to `PREPARED_LEGACY`.

**Accountability:** Operator: ____  Reviewer: ____  UTC: ____

## NO_AUTHORITY

**Entry gate:** `FROZEN_LEGACY` is complete; frozen identity is unchanged; every
automatic replacement/restart path remains suppressed.

**Mutation:** Execute Shared primitive C concurrently. At the 90-second boundary
force only exact survivors, require Docker completion inside the 100-second
envelope, and require complete reproof before the 120-second systemd deadline.

**Expected bounded output:** Exactly two process-proof rows per frozen VM; all
loaded units process-free, `MainPID=0`, exact containers absent including
stopped containers, exact numeric IDs unchanged, and `currentAction=NONE`.

**Evidence to retain:** `STOP_T0`, graceful/force target lists and timings,
complete slot rows, current/frozen inventory comparison and hashes, plus the
operator/reviewer gate decision.

**Stop condition:** Any process, container, SSH ambiguity, unknown unit/container,
unreachable/disappeared/replaced VM, ID/set/action mismatch, or missed 120-second
deadline blocks `002_activate.sql`. Keep both authorities stopped and invoke the
incident owner.

**Rollback edge:** Do not run child normalization. Resolve ambiguity while the
opposite authority remains stopped; if complete old-process absence is later
reproved and activation is abandoned, restore legacy env/unit/control state and
start only the same frozen slots.

**Accountability:** Operator: ____  Reviewer: ____  UTC: ____

## SID_DATA_READY

**Entry gate:** `NO_AUTHORITY` complete proof was reviewed and is repeated
immediately before mutation. Calls membership is still frozen; approved SID
count is 19 and the manifest digest is unchanged.

**Mutation:** Execute `002_activate.sql` once with reviewed count/digest and
`process_absence_confirmed=CONFIRMED`, wait for terminal outcome, run
`004_verify.sql`, and repeat complete process proof immediately afterward.

**Expected bounded output:** One atomic transaction changes exactly 19 Leases
from dormant to unclaimed, makes only healthy handled children ownerless active,
raises every Lease fence strictly above all mapped child fences, increments the
expected revision, preserves child cursor/lifecycle state, and reports no
missing/extra Lease. The process fence remains complete.

**Evidence to retain:** Last pre-activation proof, activation execution ID and
bounded output, verification report, post-activation proof, exact manifest, and
UTC transaction boundary.

**Stop condition:** A failed assertion or unknown execution outcome starts no
worker. The last activation assertion failure is an all-or-nothing rollback;
verify before deciding whether a new execution is safe. Any postflight/process
ambiguity invokes incident ownership.

**Rollback edge:** If activation committed but SID startup is abandoned, retain
the fence, run the inverse only after process absence remains complete, then
move through `LEGACY_DATA_READY`; never normalize by hand.

**Accountability:** Operator: ____  Reviewer: ____  UTC: ____

## SID_BOOTSTRAP

**Entry gate:** `SID_DATA_READY` and post-activation process proof pass. Frozen
VMs, numeric IDs, image digest, endpoint, and slots remain exact.

**Mutation:** Remove only the Phase 7 runtime `Restart=no` drop-ins, daemon
reload, restore approved `Restart=always`, enable/start the same frozen slots,
and no others. Capture one `startup_pacing` record per slot, then run
`bootstrap.jq` over complete raw evidence.

**Expected bounded output:** Every slot starts once with its same instance ID,
worker index, new PID, `profile=mixed-dormant`, exact profile digest,
`bcfy_calls_authority_mode=sid_lease`, selected SID domain, and candidate image
digest. `bootstrap.jq` returns exact 19/19 first successful provider-observed,
valid/non-regressive `lastPos` events and one `BOOTSTRAP_END`.

**Evidence to retain:** Same-slot start commands, startup event identities,
process/image/config report, complete bootstrap filter/envelope/raw object ID and
hash, reducer result, and `BOOTSTRAP_END`.

**Stop condition:** Missing/duplicate slot startup, wrong mode/profile/digest,
legacy configured authority, changed VM, incomplete log collection, or failure
to reach exact 19/19 blocks the soak and durable-control restoration. Apply the
hard-trigger decision; never add a separate provider probe.

**Rollback edge:** Reacquire the complete freezes, then enter
`ROLLBACK_NO_AUTHORITY`; do not start legacy directly.

**Accountability:** Operator: ____  Reviewer: ____  UTC: ____

## SID_DURABLE

**Entry gate:** `SID_BOOTSTRAP` is exact 19/19 and current SID processes remain
healthy on the frozen fleet.

**Mutation:** Through the saved-plan, pinned, no-app-dispatch Terraform path,
reconcile durable `sid_lease`, `mixed-dormant`, maintenance state, exact image,
and active AlloyDB endpoint. Do not use the application rollout. Compare the
rendered replacement template to every active process before changing controls.

**Expected bounded output:** Terminal apply with no worker recreation or member
change. Replacement template and all active processes agree on mode, profile,
image digest, endpoint, and two-slot topology; maintenance still suppresses
automatic replacement and ordinary application dispatch.

**Evidence to retain:** Bound plan/hash/public-source pin, apply execution ID,
rendered key-only template projection, active process projection, inventory
comparison, and no-dispatch proof.

**Stop condition:** Failed durable Terraform apply, replacement/action, endpoint
or template mismatch, image drift, or workflow dispatch blocks the soak/accept.
Keep automatic controls off and choose incident or exact rollback.

**Rollback edge:** Enter `ROLLBACK_NO_AUTHORITY` with current SID processes still
loaded; staging legacy is inert until their restart.

**Accountability:** Operator: ____  Reviewer: ____  UTC: ____

## SID_SOAK

**Entry gate:** `SID_DURABLE` passes. Record `SOAK_START` only now and require it
to be strictly later than `BOOTSTRAP_END`; predeclare 60-second, six-poll, and
300-second cursor-lag thresholds.

**Mutation:** Observe only. Set `SOAK_END=SOAK_START+30m`, collect one complete
raw log population over `[SOAK_START, SOAK_END)`, hash it, and run `soak.jq`.
Run aggregate gates first; inspect bounded per-SID detail only after failure.

**Expected bounded output:** Exact 19 SIDs; 90–120 logical polls/minute;
attempts/logical ≤1.25; rows/summed exact per-response distinct URLs ≤1.25;
monotonic lastPos; no gap over 60 seconds; no six-poll/60-second sustained
hazard; no replay truncation; and configured/process proof of zero managed-fleet
legacy authority.

**Evidence to retain:** Exact filters and half-open timestamps, result limit and
completion proof, raw-log object ID/hash, reducer version/result, aggregate
formulas/denominators, threshold approval, bounded drill-down, and explicit
managed-fleet authority inference boundary.

**Stop condition:** Authority overlap/ambiguity, missing SID, incomplete logs,
cadence/amplification breach, regressive lastPos, replay loss, or sustained
hazard blocks acceptance. An isolated recovered anomaly requires retained
review; it never erases a hard-gate failure.

**Rollback edge:** Hard trigger enters `ROLLBACK_NO_AUTHORITY`; otherwise keep
controls frozen while the reviewer adjudicates evidence.

**Accountability:** Operator: ____  Reviewer: ____  UTC: ____

## ACCEPTED

**Entry gate:** All bootstrap, durable, soak, process/configured-authority, and
review gates pass; no exception is unowned.

**Mutation:** Approve SID authority, unfreeze Calls membership, and restore the
captured autoscaler policy/mode, autohealing health check/delay, REPAIR failure
action, unit boot/`Restart=always`, and deploy controls. Set durable maintenance
false while retaining `sid_lease`, `mixed-dormant`, exact image/endpoint. This
is control restoration after acceptance, not the authority transition.

**Expected bounded output:** Restored controls exactly match the approved steady
configuration, all existing instances remain the frozen numeric IDs with
`currentAction=NONE`, and all processes remain SID authority. No unexpected
replacement or ordinary authority-switch rollout occurs.

**Evidence to retain:** Acceptance decision, reviewer signature/UTC, before/after
control comparison, final inventory/process report, durable plan/apply record,
and exception ledger.

**Stop condition:** Any control, template, process mode, endpoint, or inventory
mismatch keeps the change open and invokes incident response; do not declare
acceptance merely because provider traffic exists.

**Rollback edge:** A later hard trigger reacquires all freezes and begins only at
`ROLLBACK_NO_AUTHORITY`. A successful cutover does not execute rollback merely
for proof; its SQL/order is deterministically tested and table-topped.

**Accountability:** Operator: ____  Reviewer: ____  UTC: ____

## ROLLBACK_NO_AUTHORITY

**Entry gate:** Incident commander records a hard rollback trigger; Calls
membership and both deploy paths are frozen; autoscaling, autohealing, repair,
boot, and restart are suppressed; the complete current SID fleet is captured.

**Mutation:** Enter or confirm durable maintenance while the replacement
template remains `BCFY_CALLS_AUTHORITY_MODE=sid_lease`; this prevents an
unexpected replacement from starting legacy before SID absence. Use Shared
primitive B with `TARGET_MODE=legacy_feed`; running SID processes retain their
loaded mode. Execute Shared primitive C, including exact 90-second graceful,
100-second Docker, and 120-second systemd bounds, then reprove every SID slot
absent. Durable `legacy_feed` is reconciled only after `LEGACY_DATA_READY` and
same-slot legacy startup.

**Expected bounded output:** Exact same complete process absence proof shape for every
captured numeric VM ID and both worker slots: process-free unit, `MainPID=0`,
container absent, unchanged fleet, `currentAction=NONE`. No legacy process has
started.

**Evidence to retain:** Trigger/incident owner, captured controls, staged key-only
proof, stop/force timings and exact targets, full slot rows, inventory/hash, and
reviewed absence decision.

**Stop condition:** Any active/ambiguous SID process, inaccessible or replaced
VM, unknown unit/container, action, deploy, or deadline breach blocks
`003_rollback_children.sql` and blocks legacy start.

**Rollback edge:** There is no opposite-authority escape hatch. Resolve and
reprove complete SID absence while legacy remains stopped.

**Accountability:** Operator: ____  Reviewer: ____  UTC: ____

## LEGACY_DATA_READY

**Entry gate:** `ROLLBACK_NO_AUTHORITY` process absence is complete and repeated
immediately before SQL. Membership remains frozen.

**Mutation:** Execute only `003_rollback_children.sql` with
`process_absence_confirmed=CONFIRMED`, then `004_verify.sql`. Re-run complete SID
process proof after the transaction.

**Expected bounded output:** Healthy ownerless active children become legacy
claimable unclaimed; failing/quarantined/deactivated children retain lifecycle;
all Lease rows, owners/statuses, fences, revisions, progress, cursor fields, and
failure history are byte-for-byte preserved. No Lease is reset or deleted.

**Evidence to retain:** Last pre-SQL process proof, terminal operation IDs,
bounded child/lifecycle report, before/after Lease digest, verification report,
and post-SQL process proof.

**Stop condition:** Unknown SQL outcome, changed Lease identity/fence/progress,
unexpected child count/state, or process ambiguity blocks legacy start. Never
mass-normalize or rerun blindly.

**Rollback edge:** Keep both authorities stopped under incident ownership until
the exact database and process state is understood.

**Accountability:** Operator: ____  Reviewer: ____  UTC: ____

## LEGACY_RESTORED

**Entry gate:** `LEGACY_DATA_READY` proves healthy children claimable, permanent
Leases/history preserved, and all SID processes absent.

**Mutation:** Remove only the Phase 7 runtime no-restart overrides, restore
approved `Restart=always`, and start the same frozen slots in `legacy_feed`.
Verify startup records before reconciling durable legacy configuration, then
restore captured controls and deploy/membership writers.

**Expected bounded output:** Every exact slot starts once with the same numeric
VM ID, expected image/profile, and `bcfy_calls_authority_mode=legacy_feed`;
static mode contract proves SID admission disabled. Durable template agrees,
automatic controls equal the captured normal state, and no SID process runs.

**Evidence to retain:** Same-slot startup records and PIDs, process/configured
authority report, durable plan/apply record, final control and fleet comparison,
incident resolution, and reviewer decision.

**Stop condition:** Wrong mode/profile/image/endpoint, unexpected replacement,
SID configured authority, failed durable reconcile, or incomplete control
restoration keeps the incident open and prevents unfreezing automation.

**Rollback edge:** If legacy restoration itself fails, reacquire the frozen
process state and let the incident commander choose a reviewed recovery; never
start SID merely to regain traffic while authority is ambiguous.

**Accountability:** Operator: ____  Reviewer: ____  UTC: ____

## Hard rollback triggers

Immediate rollback decision is mandatory for any competing managed-fleet
authority, incomplete process proof, wrong startup mode, manifest/SID-set drift,
unknown activation outcome, failure to reach 19/19 bootstrap inside the approved
window, hard soak gate breach, durable configuration divergence, or loss of the
deployment/administrative freeze. A single self-recovered non-hard anomaly may
be investigated only while all hard gates continue to pass.

## Independent review 1 — process and authority tabletop

Reviewer 1 walks the exact commands and state edges with these injections:

| Injection | Required fail-closed result |
|---|---|
| SSH-unreachable | No activation/inverse SQL; opposite authority stays stopped. |
| numeric-ID replacement | Fleet identity mismatch blocks the process gate. |
| >120-second worker | Incident; no opposite authority starts. |
| last activation assertion failure | Transaction rolls back; no worker starts until exact verification. |
| incomplete log collection | Bootstrap/soak cannot pass. |
| failed durable Terraform apply | Automatic controls stay frozen; exact rollback or incident. |

Reviewer: ____  UTC: ____  Findings artifact/hash: ____  HIGH open: 0 / ____

## Independent review 2 — data, recovery, and evidence tabletop

Reviewer 2 independently checks inverse ordering, immutable Lease history,
secret-free evidence, and the PITR cross-timeline branches. The PITR companion
injects wrong restored credential, endpoint fan-out miss, pre-schema restore,
and dormant-row restore. The real non-production rehearsal remains the process
prerequisite. Production use requires **no unmitigated HIGH** finding.

Reviewer: ____  UTC: ____  Findings artifact/hash: ____  HIGH open: 0 / ____

## Publication boundary

This file defines a procedure and deterministic gates. Until Plan 07-07 records
the live evidence, its publication means exactly:

`NO PRODUCTION CUTOVER PERFORMED`
