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
   commit, deployment commit, immutable worker image digest, the shared
   immutable SQL Job image digest and numeric password-secret version pinned to
   both Jobs by the saved Terraform/config tests, and the controlled operation
   Job identity and generation.
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

The command shapes were reviewed against official current Google Cloud SDK,
AlloyDB, and GitHub CLI documentation resolved through Context7 as
`/websites/cloud_google_sdk`, `/websites/cloud_google_alloydb`, and
`/websites/cli_github_manual`, then checked against Google Cloud SDK 565.0.0
(core 2026.04.10) and the installed GitHub CLI. Before each execution, record
the installed versions and confirm the named flags still exist. A shape
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
: "${SIGNED_INPUTS:?set the reviewed non-secret input-manifest path}"
: "${SIGNED_INPUTS_SHA256:?set its approved lowercase SHA-256}"
: "${OPERATOR_ACCOUNT:?set the reviewed gcloud account}"
: "${PUBLIC_REPO_ROOT:?set the clean reviewed public-repository root}"

test "$SERVICE" = "icecast-collector-prod"
test "$(sha256sum "$SIGNED_INPUTS" | awk '{print $1}')" = \
  "$SIGNED_INPUTS_SHA256"
PROJECT_NUMBER=$(jq -er '.project_number|tostring' "$SIGNED_INPUTS")
SQL_BUCKET=$(jq -er '.sql_bucket' "$SIGNED_INPUTS")
EXPECTED_MIG_URI="https://www.googleapis.com/compute/v1/projects/$PROJECT/regions/$REGION/instanceGroupManagers/$MIG"
EXPECTED_AUTOSCALER_URI="https://www.googleapis.com/compute/v1/projects/$PROJECT/regions/$REGION/autoscalers/$AUTOSCALER"
EXPECTED_JOB_URI="projects/$PROJECT/locations/$REGION/jobs/$OP_JOB"
EXPECTED_SQL_BUCKET_URI="projects/_/buckets/$SQL_BUCKET"
EXPECTED_SIDS=$(jq -c '.expected_sids' "$SIGNED_INPUTS")
MANIFEST_DIGEST=$(jq -er '.manifest_digest' "$SIGNED_INPUTS")
EXPECTED_STARTUP_CONTRACT=$(jq -ce '.startup_contract' "$SIGNED_INPUTS")
PUBLIC_COMMIT=$(jq -er '.public_commit' "$SIGNED_INPUTS")
jq -e --arg project "$PROJECT" --arg region "$REGION" --arg mig "$MIG" \
  --arg autoscaler "$AUTOSCALER" --arg service "$SERVICE" \
  --arg operation_job "$OP_JOB" --arg account "$OPERATOR_ACCOUNT" \
  --arg project_number "$PROJECT_NUMBER" --arg mig_uri "$EXPECTED_MIG_URI" \
  --arg autoscaler_uri "$EXPECTED_AUTOSCALER_URI" \
  --arg operation_job_uri "$EXPECTED_JOB_URI" \
  --arg sql_bucket_uri "$EXPECTED_SQL_BUCKET_URI" '
    .schema_version == 1 and .project == $project and .region == $region and
    (.project_number|tostring) == $project_number and
    .mig == $mig and .autoscaler == $autoscaler and .service == $service and
    .operation_job == $operation_job and .operator_account == $account and
    .mig_uri == $mig_uri and .autoscaler_uri == $autoscaler_uri and
    .operation_job_uri == $operation_job_uri and
    .sql_bucket_uri == $sql_bucket_uri and
    (.operation_job_uid | type == "string" and length > 0) and
    (.operation_job_generation|tostring|test("^[1-9][0-9]*$")) and
    (.operation_job_image | test("@sha256:[0-9a-f]{64}$")) and
    (.operation_job_command_sha256 | test("^[0-9a-f]{64}$")) and
    (.operation_job_service_account | type == "string" and length > 0) and
    (.operation_job_db_host | type == "string" and length > 0) and
    (.operation_job_db_name | type == "string" and length > 0) and
    (.postgres_secret_id | type == "string" and length > 0) and
    (.postgres_secret_version|tostring|test("^[1-9][0-9]*$")) and
    (.operation_job_network | type == "string" and length > 0) and
    (.operation_job_subnetwork | type == "string" and length > 0) and
    (.public_commit | test("^[0-9a-f]{40}$")) and
    (.deployment_commit | test("^[0-9a-f]{40}$")) and
    (.deployment_ref | type == "string" and
      test("^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")) and
    (.image_digest | test("@sha256:[0-9a-f]{64}$")) and
    (.sql_bucket | type == "string" and length > 0) and
    (.manifest_digest | test("^[0-9a-f]{64}$")) and
    (.public_runtime_sha256 | type == "object" and
      keys == ["bootstrap_reducer","slo_contract","soak_reducer"] and
      all(.[]; type == "string" and test("^[0-9a-f]{64}$"))) and
    (.expected_sids | type == "array" and length == 19 and
      . == (sort | unique) and
      all(.[]; type == "string" and test("^[0-9]+$"))) and
    (.startup_contract | type == "object" and
      keys == ["lease_admission_cycle_budget","max_feeds_per_worker",
               "profile","profile_digests","selected_domains",
               "startup_jitter_max_sec","startup_stagger_max_sec"] and
      .profile == "mixed-dormant" and
      .selected_domains == ["feed","sid"] and
      (.profile_digests | type == "object" and
        keys == ["legacy_feed","sid_lease"] and
        all(.[]; type == "string" and test("^[0-9a-f]{64}$"))) and
      (.startup_stagger_max_sec | type == "number" and . >= 0) and
      (.startup_jitter_max_sec | type == "number" and . >= 0) and
      (.startup_stagger_max_sec + .startup_jitter_max_sec < 600) and
      (.lease_admission_cycle_budget | type == "number" and
        . == floor and . > 0) and
      (.max_feeds_per_worker | type == "number" and
        . == floor and . > 0))' "$SIGNED_INPUTS"
PUBLIC_REPO_ROOT=$(realpath -- "$PUBLIC_REPO_ROOT")
test -d "$PUBLIC_REPO_ROOT/.git" -o -f "$PUBLIC_REPO_ROOT/.git"
test "$(git -C "$PUBLIC_REPO_ROOT" rev-parse HEAD)" = "$PUBLIC_COMMIT"
test -z "$(git -C "$PUBLIC_REPO_ROOT" status --porcelain \
  --untracked-files=all)"
test "$(sha256sum \
  "$PUBLIC_REPO_ROOT/backend/pipeline/ingestion/slo_contract.py" \
  | awk '{print $1}')" = \
  "$(jq -er '.public_runtime_sha256.slo_contract' "$SIGNED_INPUTS")"
test "$(sha256sum \
  "$PUBLIC_REPO_ROOT/scripts/operations/bcfy_calls_sid/bootstrap.jq" \
  | awk '{print $1}')" = \
  "$(jq -er '.public_runtime_sha256.bootstrap_reducer' "$SIGNED_INPUTS")"
test "$(sha256sum \
  "$PUBLIC_REPO_ROOT/scripts/operations/bcfy_calls_sid/soak.jq" \
  | awk '{print $1}')" = \
  "$(jq -er '.public_runtime_sha256.soak_reducer' "$SIGNED_INPUTS")"
test "$(gcloud auth list --filter=status:ACTIVE --format='value(account)')" = \
  "$OPERATOR_ACCOUNT"
umask 077
test ! -e "$ARTIFACT_DIR"
mkdir -m 700 -- "$ARTIFACT_DIR"
test "$(stat -c '%a' "$ARTIFACT_DIR")" = 700
test "$(stat -c '%u' "$ARTIFACT_DIR")" -eq "$(id -u)"
jq -cS '{public_commit,public_runtime_sha256}' "$SIGNED_INPUTS" \
  >"$ARTIFACT_DIR/public-runtime-projection.json"
sha256sum "$ARTIFACT_DIR/public-runtime-projection.json" \
  >"$ARTIFACT_DIR/public-runtime-projection.sha256"

cloud_timeout() {
  local seconds soft_seconds
  seconds=$1
  shift
  test "$seconds" -gt 0
  if test "$seconds" -eq 1; then
    timeout --foreground --signal=KILL 1s "$@"
  else
    soft_seconds=$((seconds - 1))
    timeout --foreground --signal=TERM --kill-after=1s \
      "${soft_seconds}s" "$@"
  fi
}

# gcloud 565.0.0 has no `compute autoscalers describe` read. Its current beta
# managed-group export command handles authentication and writes canonical JSON
# autoscaling configuration, or literal null when no autoscaler is attached.
# Read twice, cross-bind both reads to MIG status, and retain only a projection.
export_autoscaling_projection() (
  set -euo pipefail
  temp=$(mktemp -d)
  chmod 700 "$temp"
  trap 'rm -rf -- "$temp"' EXIT
  for pass in first second; do
    cloud_timeout 30 gcloud compute instance-groups managed describe "$MIG" \
      --project="$PROJECT" --region="$REGION" --format=json \
    | jq -cS '.status.autoscaler // null' >"$temp/$pass-status.json"
    cloud_timeout 30 gcloud beta compute instance-groups managed \
      export-autoscaling "$MIG" --project="$PROJECT" --region="$REGION" \
      --autoscaling-file="$temp/$pass-export.json" --quiet
    jq -cS '
      if . == null then null
      elif (type == "object" and
            ((keys - ["autoscalingPolicy","description","recommendedSize",
                      "scalingScheduleStatus"]) | length) == 0 and
            (.autoscalingPolicy | type) == "object") then {
        description:(.description // null),
        autoscalingPolicy:.autoscalingPolicy}
      else error("autoscaling export must be an object or null") end' \
      "$temp/$pass-export.json" >"$temp/$pass-projection.json"
  done
  cmp -s "$temp/first-status.json" "$temp/second-status.json"
  cmp -s "$temp/first-projection.json" "$temp/second-projection.json"
  if jq -e '. == null' "$temp/second-projection.json" >/dev/null; then
    jq -e '. == null' "$temp/second-status.json" >/dev/null
    printf '%s\n' '{"present":false}'
  else
    jq -e --arg uri "$EXPECTED_AUTOSCALER_URI" '. == $uri' \
      "$temp/second-status.json" >/dev/null
    jq -cS --arg name "$AUTOSCALER" --arg uri "$EXPECTED_AUTOSCALER_URI" \
      --arg target "$EXPECTED_MIG_URI" '
      {present:true,name:$name,selfLink:$uri,target:$target,
       description,autoscalingPolicy}' "$temp/second-projection.json"
  fi
)

gcloud version --format=json >"$ARTIFACT_DIR/gcloud-version.json"
gh version >"$ARTIFACT_DIR/gh-version.txt"
cloud_timeout 30 gcloud projects describe "$PROJECT" --format=json \
  | jq -cS '{projectId,projectNumber,lifecycleState}' \
  >"$ARTIFACT_DIR/bound-project-projection.json"
jq -e --arg project "$PROJECT" --arg number "$PROJECT_NUMBER" '
  .projectId == $project and (.projectNumber|tostring) == $number and
  .lifecycleState == "ACTIVE"' "$ARTIFACT_DIR/bound-project-projection.json"

cloud_timeout 30 gcloud compute instance-groups managed describe "$MIG" \
  --project="$PROJECT" --region="$REGION" --format=json \
  | jq -cS '{name,selfLink,region}' \
  >"$ARTIFACT_DIR/bound-mig-projection.json"
export_autoscaling_projection \
  | jq -cS 'select(.present == true) | {name,selfLink,target}' \
  >"$ARTIFACT_DIR/bound-autoscaler-projection.json"
jq -e --arg mig "$MIG" --arg region "$REGION" \
  --arg uri "$EXPECTED_MIG_URI" '
  .name == $mig and .selfLink == $uri and
  (.region | endswith("/regions/" + $region))' \
  "$ARTIFACT_DIR/bound-mig-projection.json"
jq -e --arg autoscaler "$AUTOSCALER" --arg uri "$EXPECTED_AUTOSCALER_URI" \
  --arg mig_uri "$EXPECTED_MIG_URI" '
  .name == $autoscaler and .selfLink == $uri and .target == $mig_uri' \
  "$ARTIFACT_DIR/bound-autoscaler-projection.json"
sha256sum "$ARTIFACT_DIR/bound-project-projection.json" \
  "$ARTIFACT_DIR/bound-mig-projection.json" \
  "$ARTIFACT_DIR/bound-autoscaler-projection.json" \
  >"$ARTIFACT_DIR/bound-resources.sha256"

gcloud compute instance-groups managed update-autoscaling --help \
  | grep -E -- '--mode.*off|--mode.*on'
gcloud beta compute instance-groups managed export-autoscaling --help \
  | grep -F -- '--autoscaling-file=PATH'
gcloud compute instance-groups managed update --help \
  | grep -E -- '--clear-autohealing|--default-action-on-vm-failure'
gcloud compute instance-groups managed list-instances --help \
  | grep -E -- '--region|--format'
gcloud compute instance-templates describe --help \
  | grep -E -- '--global|--format'
gcloud logging read --help | grep -E -- '--limit|--order|--format'
gcloud run jobs execute --help | grep -E -- '--args|--wait'
gh run list --help | grep -E -- '--workflow|--status|--json|--limit'
gh workflow run --help | grep -E -- '--ref|--field'
gh run watch --help | grep -F -- '--exit-status'
gh run download --help | grep -E -- '--name|--dir'
```

Cloud Logging's current CLI exposes no page-token input. Every evidence read
below is therefore one complete CLI collection over an exact half-open window,
with the CLI internally traversing service pages. The command uses a
predeclared high result limit and fails closed if that limit is reached; there
is no invented `page-token` loop. The single collection is represented as one
complete page in the checked reducer envelope.

Reducer-safe projected cloud JSON and logs stay in the approved immutable
external artifact/change system. Unvalidated provider payloads never enter
retained evidence. Complete environment files, password or token values,
signed URLs, private provider payloads, private keys, and bearer tokens are
never collected or stored as evidence anywhere. Leave them in Secret Manager
or their source system; retain only numeric secret-version resource IDs,
key-only projections, bounded sanitized output, and hashes. Git retains only
the secret-free index template.

## Shared primitive 0: capture and compare durable cutover controls

Use the exact deployment repository and protected production ref. The ref must
resolve to the signed deployment commit at every apply. This primitive records
only bounded control-plane fields; it never stores a complete provider response
or a secret.

```bash
DEPLOYMENT_REPO=watch-duty/radio-transcription-deployment
DEPLOYMENT_REF=$(jq -er '.deployment_ref' "$SIGNED_INPUTS")
DEPLOYMENT_COMMIT=$(jq -er '.deployment_commit' "$SIGNED_INPUTS")
CANDIDATE_IMAGE=$(jq -er '.image_digest' "$SIGNED_INPUTS")

capture_no_active_workflow_runs() (
  set -euo pipefail
  output=$1
  test ! -e "$output"
  temp=$(mktemp -d)
  chmod 700 "$temp"
  trap 'rm -rf -- "$temp"' EXIT
  : >"$temp/nonterminal.ndjson"
  for workflow in terraform_deploy.yml app_deploy.yml; do
    for status in queued in_progress requested waiting action_required; do
      cloud_timeout 30 gh run list --repo "$DEPLOYMENT_REPO" \
        --workflow "$workflow" --status "$status" --limit 1 \
        --json databaseId,status,event,headSha,createdAt \
      | jq -c --arg workflow "$workflow" --arg queried_status "$status" '
          .[] | {workflow:$workflow,queried_status:$queried_status,
            databaseId,status,event,headSha,createdAt}' \
      >>"$temp/nonterminal.ndjson"
    done
  done
  jq -csS 'sort_by(.workflow,.databaseId,.queried_status)' \
    "$temp/nonterminal.ndjson" >"$temp/nonterminal.json"
  jq -e 'length == 0' "$temp/nonterminal.json"
  install -m 600 "$temp/nonterminal.json" "$output"
)

capture_cutover_controls() (
  set -euo pipefail
  label=$1
  case "$label" in *[!A-Za-z0-9._-]*|'') return 64 ;; esac
  out="$ARTIFACT_DIR/controls-$label"
  test ! -e "$out"
  mkdir -m 700 "$out"
  temp=$(mktemp -d)
  chmod 700 "$temp"
  trap 'rm -rf -- "$temp"' EXIT

  cloud_timeout 30 gcloud compute instance-groups managed describe "$MIG" \
    --project="$PROJECT" --region="$REGION" --format=json \
  | jq -cS '{
      name,selfLink,targetSize,
      versions:[.versions[]? | {
        name:(.name // null),instanceTemplate:(.instanceTemplate // null),
        targetSize:((.targetSize // {}) | {
          fixed:(.fixed // null),percent:(.percent // null),
          calculated:(.calculated // null)})
      }],
      instanceTemplate:(.instanceTemplate // null),
      updatePolicy:((.updatePolicy // {}) | {
        type:(.type // null),minimalAction:(.minimalAction // null),
        mostDisruptiveAllowedAction:(.mostDisruptiveAllowedAction // null),
        replacementMethod:(.replacementMethod // null),
        instanceRedistributionType:(.instanceRedistributionType // null),
        maxSurge:((.maxSurge // {}) | {
          fixed:(.fixed // null),percent:(.percent // null),
          calculated:(.calculated // null)}),
        maxUnavailable:((.maxUnavailable // {}) | {
          fixed:(.fixed // null),percent:(.percent // null),
          calculated:(.calculated // null)})}),
      autoHealingPolicies:[.autoHealingPolicies[]? | {
        healthCheck:(.healthCheck // null),
        initialDelaySec:(.initialDelaySec // null)}],
      instanceLifecyclePolicy:((.instanceLifecyclePolicy // {}) | {
        defaultActionOnFailure:(.defaultActionOnFailure // null),
        forceUpdateOnRepair:(.forceUpdateOnRepair // null)}),
      status:((.status // {}) | {
        isStable:(.isStable // null),
        autoscaler:(.autoscaler // null),
        versionTargetIsReached:(.versionTarget.isReached // null)})
    }' >"$temp/mig.json"
  jq -e --arg name "$MIG" --arg uri "$EXPECTED_MIG_URI" '
    .name == $name and .selfLink == $uri and .status.isStable == true and
    (.targetSize | type == "number")' "$temp/mig.json"

  export_autoscaling_projection >"$temp/autoscaler.json"
  jq -e --arg name "$AUTOSCALER" --arg uri "$EXPECTED_AUTOSCALER_URI" \
    --arg target "$EXPECTED_MIG_URI" '
    (.present == false and keys == ["present"]) or
    (.present == true and .name == $name and .selfLink == $uri and
      .target == $target)' "$temp/autoscaler.json"

  cloud_timeout 20 gcloud compute instance-groups managed list-instances "$MIG" \
    --project="$PROJECT" --region="$REGION" --format=json \
  | jq -cr 'sort_by(.instance)[] |
      [.instance|split("/")[-1],.instance|split("/")[-3],
       .currentAction,
       ((.version.instanceTemplate // "")|split("/")[-1])] | @tsv' \
  | while IFS=$'\t' read -r name zone action template; do
      numeric_id=$(cloud_timeout 20 gcloud compute instances describe "$name" \
        --project="$PROJECT" --zone="$zone" --format='value(id)')
      jq -nc --arg name "$name" --arg zone "$zone" --arg id "$numeric_id" \
        --arg action "$action" --arg template "$template" \
        '{instance_name:$name,zone:$zone,numeric_instance_id:$id,
          current_action:$action,instance_template:$template}'
    done | jq -csS 'sort_by(.numeric_instance_id)' >"$temp/fleet.json"
  jq -e 'length > 0 and all(.[];
    (.numeric_instance_id|test("^[0-9]+$")) and .current_action == "NONE")' \
    "$temp/fleet.json"

  cloud_timeout 30 gh variable list --repo "$DEPLOYMENT_REPO" --env prod \
    --json name,value \
  | python3 -c '
import hashlib
import json
import sys

wanted = (
    "ACTIVE_ALLOYDB_PRIMARY_INSTANCE_IP",
    "BCFY_CALLS_AUTHORITY_MODE",
    "INGESTION_CONTAINER_IMAGE",
    "INGESTION_MAINTENANCE_MODE",
)
source = json.load(sys.stdin)
result = []
for name in wanted:
    found = [row for row in source if row.get("name") == name]
    if len(found) > 1:
        raise SystemExit(f"duplicate production variable: {name}")
    value = found[0].get("value") if found else None
    row = {"name": name, "present": len(found) == 1}
    if name == "ACTIVE_ALLOYDB_PRIMARY_INSTANCE_IP":
        row["value_sha256"] = (
            hashlib.sha256(value.encode()).hexdigest()
            if isinstance(value, str)
            else None
        )
    else:
        row["value"] = value
    result.append(row)
json.dump(result, sys.stdout, separators=(",", ":"), sort_keys=True)
sys.stdout.write("\n")
' >"$temp/environment-variables.json"
  jq -e 'length == 4 and all(.[];
    (.name|type) == "string" and (.present|type) == "boolean") and
    ([.[] | select(.name == "ACTIVE_ALLOYDB_PRIMARY_INSTANCE_IP") |
      keys | sort] == [["name","present","value_sha256"]]) and
    ([.[] | select(.name == "ACTIVE_ALLOYDB_PRIMARY_INSTANCE_IP") |
      ((.present and (.value_sha256|test("^[0-9a-f]{64}$"))) or
       ((.present|not) and .value_sha256 == null))] | all) and
    ([.[] | select(.name != "ACTIVE_ALLOYDB_PRIMARY_INSTANCE_IP") |
      keys | sort] | all(. == ["name","present","value"])) and
    ([.[] | select(.name != "ACTIVE_ALLOYDB_PRIMARY_INSTANCE_IP") |
      ((.present and (.value|type) == "string") or
       ((.present|not) and .value == null))] | all)' \
    "$temp/environment-variables.json"

  for workflow in terraform_deploy.yml app_deploy.yml; do
    cloud_timeout 30 gh api \
      "repos/$DEPLOYMENT_REPO/actions/workflows/$workflow" \
    | jq -cS '{path,state}' >"$temp/workflow-$workflow.json"
    jq -e --arg suffix "/$workflow" '
      .state == "active" and (.path | endswith($suffix))' \
      "$temp/workflow-$workflow.json"
  done
  capture_no_active_workflow_runs "$temp/nonterminal-workflow-runs.json"

  for file in mig autoscaler fleet environment-variables; do
    install -m 600 "$temp/$file.json" "$out/$file.json"
  done
  for workflow in terraform_deploy.yml app_deploy.yml; do
    install -m 600 "$temp/workflow-$workflow.json" \
      "$out/workflow-$workflow.json"
  done
  install -m 600 "$temp/nonterminal-workflow-runs.json" \
    "$out/nonterminal-workflow-runs.json"
  sha256sum "$out"/*.json >"$out/controls.sha256"
)

compare_cutover_controls() (
  set -euo pipefail
  before=$1
  after=$2
  phase=$3
  expected_mode=$4
  expected_image=$5
  case "$phase" in frozen|restored) ;; *) return 64 ;; esac
  case "$expected_mode" in legacy_feed|sid_lease) ;; *) return 64 ;; esac
  test "$before" != "$after"
  temp=$(mktemp -d)
  chmod 700 "$temp"
  trap 'rm -rf -- "$temp"' EXIT

  # Template/version and the three intended tuple values may change. Target
  # size, update policy, endpoint, workflow state, member identity/action, and
  # all automatic controls are compared independently so a template change
  # cannot hide a control-restoration failure.
  jq -S '{name,selfLink,targetSize,updatePolicy}' "$before/mig.json" \
    >"$temp/before-stable-mig.json"
  jq -S '{name,selfLink,targetSize,updatePolicy}' "$after/mig.json" \
    >"$temp/after-stable-mig.json"
  cmp -s "$temp/before-stable-mig.json" "$temp/after-stable-mig.json"
  jq -S 'map(del(.instance_template))' "$before/fleet.json" \
    >"$temp/before-stable-fleet.json"
  jq -S 'map(del(.instance_template))' "$after/fleet.json" \
    >"$temp/after-stable-fleet.json"
  cmp -s "$temp/before-stable-fleet.json" \
    "$temp/after-stable-fleet.json"
  for workflow in terraform_deploy.yml app_deploy.yml; do
    cmp -s "$before/workflow-$workflow.json" \
      "$after/workflow-$workflow.json"
  done
  jq -e 'length == 0' "$after/nonterminal-workflow-runs.json"
  before_endpoint=$(jq -er '.[] | select(
    .name == "ACTIVE_ALLOYDB_PRIMARY_INSTANCE_IP") |
    [.present,.value_sha256] | @json' \
    "$before/environment-variables.json")
  after_endpoint=$(jq -er '.[] | select(
    .name == "ACTIVE_ALLOYDB_PRIMARY_INSTANCE_IP") |
    [.present,.value_sha256] | @json' \
    "$after/environment-variables.json")
  test "$before_endpoint" = "$after_endpoint"
  expected_maintenance=true
  if test "$phase" = restored; then expected_maintenance=false; fi
  jq -e --arg mode "$expected_mode" --arg image "$expected_image" \
    --arg maintenance "$expected_maintenance" '
    def exact($name;$value):
      [ .[] | select(.name == $name and .present and .value == $value) ] |
      length == 1;
    exact("BCFY_CALLS_AUTHORITY_MODE";$mode) and
    exact("INGESTION_MAINTENANCE_MODE";$maintenance) and
    exact("INGESTION_CONTAINER_IMAGE";$image)' \
    "$after/environment-variables.json"

  if test "$phase" = frozen; then
    jq -e '.present == false' "$after/autoscaler.json"
    jq -e '.autoHealingPolicies == [] and
      .instanceLifecyclePolicy.defaultActionOnFailure == "DO_NOTHING" and
      .status.isStable == true and .status.autoscaler == null' \
      "$after/mig.json"
  else
    cmp -s "$before/autoscaler.json" "$after/autoscaler.json"
    jq -S '{autoHealingPolicies,instanceLifecyclePolicy,
      status_autoscaler:.status.autoscaler}' "$before/mig.json" \
      >"$temp/before-automatic-controls.json"
    jq -S '{autoHealingPolicies,instanceLifecyclePolicy,
      status_autoscaler:.status.autoscaler}' "$after/mig.json" \
      >"$temp/after-automatic-controls.json"
    cmp -s "$temp/before-automatic-controls.json" \
      "$temp/after-automatic-controls.json"
  fi
)
```

Primitive B remains the one per-slot unit capture. Its original rows must say
`Restart=always Enabled=enabled`, its frozen rows must prove disabled plus
`Restart=no`, and Shared primitive E must later prove enabled plus
`Restart=always`. Do not add a second unit-state implementation. Call
`capture_cutover_controls original` before maintenance, `frozen` after the
first maintenance apply, `sid-durable` after its reconcile, and `restored`
after acceptance or rollback restoration. Use `compare_cutover_controls` with
`frozen` for both maintenance captures and with `restored` against `original`.

## Shared primitive 00: saved-plan ingestion-tuple reconcile

This is the only durable mode/maintenance/image mutation. It changes the three
production variables in a fail-closed order, dispatches exactly one saved-plan
workflow at a signed immutable protected tag, and proves that the ordinary
application deployment did not run. `suppress_app_deploy=true` is a reviewed
workflow input at `DEPLOYMENT_COMMIT`; its absence is a hard stop. These
commands are documentation only in this phase—do not execute them until the
separately approved production change.

```bash
set_and_apply_ingestion_tuple() (
  set -euo pipefail
  MODE=$1
  MAINTENANCE=$2
  IMAGE=$3
  LABEL=$4
  case "$MODE" in legacy_feed|sid_lease) ;; *) return 64 ;; esac
  case "$MAINTENANCE" in true|false) ;; *) return 64 ;; esac
  case "$LABEL" in *[!A-Za-z0-9._-]*|'') return 64 ;; esac
  test "$IMAGE" = "$CANDIDATE_IMAGE"
  printf '%s' "$IMAGE" | grep -Eq '@sha256:[0-9a-f]{64}$'
  test "$PUBLIC_COMMIT" = "$(jq -er '.public_commit' "$SIGNED_INPUTS")"
  test "$DEPLOYMENT_COMMIT" = \
    "$(jq -er '.deployment_commit' "$SIGNED_INPUTS")"
  test "$DEPLOYMENT_REF" = "$(jq -er '.deployment_ref' "$SIGNED_INPUTS")"
  cloud_timeout 30 gh api \
    "repos/$DEPLOYMENT_REPO/git/ref/tags/$DEPLOYMENT_REF" \
    --jq '.ref' | grep -Fx "refs/tags/$DEPLOYMENT_REF"
  resolved_ref=$(cloud_timeout 30 gh api \
    "repos/$DEPLOYMENT_REPO/commits/$DEPLOYMENT_REF" --jq '.sha')
  test "$resolved_ref" = "$DEPLOYMENT_COMMIT"

  out="$ARTIFACT_DIR/tuple-apply-$LABEL"
  test ! -e "$out"
  mkdir -m 700 "$out"
  temp=$(mktemp -d)
  chmod 700 "$temp"
  trap 'rm -rf -- "$temp"' EXIT

  cloud_timeout 30 gh run list --repo "$DEPLOYMENT_REPO" \
    --workflow terraform_deploy.yml \
    --event workflow_dispatch --commit "$DEPLOYMENT_COMMIT" --limit 100 \
    --json databaseId,headSha,event,status,createdAt \
  | jq -cS '[.[] | {databaseId,headSha,event,status,createdAt}]' \
    >"$temp/infra-before.json"
  cloud_timeout 30 gh run list --repo "$DEPLOYMENT_REPO" \
    --workflow app_deploy.yml \
    --event workflow_dispatch --limit 100 \
    --json databaseId,headSha,event,status,createdAt \
  | jq -cS '[.[] | {databaseId,headSha,event,status,createdAt}]' \
    >"$temp/app-before.json"
  capture_no_active_workflow_runs \
    "$temp/nonterminal-before-mutation.json"

  # The deploy writers are frozen. If that freeze is violated, true
  # maintenance first makes a stray plan fail closed until the immutable image
  # and mode are exact. When leaving maintenance, mode and image become exact
  # before maintenance becomes false.
  if test "$MAINTENANCE" = true; then
    cloud_timeout 30 gh variable set INGESTION_MAINTENANCE_MODE \
      --repo "$DEPLOYMENT_REPO" \
      --env prod --body true
    cloud_timeout 30 gh variable set INGESTION_CONTAINER_IMAGE \
      --repo "$DEPLOYMENT_REPO" \
      --env prod --body "$IMAGE"
    cloud_timeout 30 gh variable set BCFY_CALLS_AUTHORITY_MODE \
      --repo "$DEPLOYMENT_REPO" \
      --env prod --body "$MODE"
  else
    cloud_timeout 30 gh variable set BCFY_CALLS_AUTHORITY_MODE \
      --repo "$DEPLOYMENT_REPO" \
      --env prod --body "$MODE"
    cloud_timeout 30 gh variable set INGESTION_CONTAINER_IMAGE \
      --repo "$DEPLOYMENT_REPO" \
      --env prod --body "$IMAGE"
    cloud_timeout 30 gh variable set INGESTION_MAINTENANCE_MODE \
      --repo "$DEPLOYMENT_REPO" \
      --env prod --body false
  fi

  cloud_timeout 30 gh variable list --repo "$DEPLOYMENT_REPO" --env prod \
    --json name,value \
  | jq -cS --arg mode "$MODE" --arg maintenance "$MAINTENANCE" \
      --arg image "$IMAGE" '
      [ .[] | select(.name | IN(
        "BCFY_CALLS_AUTHORITY_MODE","INGESTION_MAINTENANCE_MODE",
        "INGESTION_CONTAINER_IMAGE")) | {name,value} ] | sort_by(.name)
      | select(length == 3)
      | select([.[] | select(
          (.name == "BCFY_CALLS_AUTHORITY_MODE" and .value == $mode) or
          (.name == "INGESTION_MAINTENANCE_MODE" and .value == $maintenance) or
          (.name == "INGESTION_CONTAINER_IMAGE" and .value == $image))]
        | length == 3)' >"$temp/tuple-reread.json"
  test -s "$temp/tuple-reread.json"

  jq -ncS --arg ref "$DEPLOYMENT_REF" --arg sha "$DEPLOYMENT_COMMIT" \
    --arg public_sha "$PUBLIC_COMMIT" \
    '{workflow:"terraform_deploy.yml",environment:"prod",ref:$ref,
      head_sha:$sha,deploy_release:true,public_sha:$public_sha,
      force_app_deploy:false,suppress_app_deploy:true}' \
    >"$temp/invocation-projection.json"
  cloud_timeout 30 gh workflow run terraform_deploy.yml \
    --repo "$DEPLOYMENT_REPO" \
    --ref "$DEPLOYMENT_REF" \
    --raw-field environment=prod \
    --raw-field deploy_release=true \
    --raw-field public_sha="$PUBLIC_COMMIT" \
    --raw-field force_app_deploy=false \
    --raw-field suppress_app_deploy=true

  discovery_deadline=$(( $(date +%s) + 120 ))
  while :; do
    cloud_timeout 30 gh run list --repo "$DEPLOYMENT_REPO" \
      --workflow terraform_deploy.yml \
      --event workflow_dispatch --commit "$DEPLOYMENT_COMMIT" --limit 100 \
      --json databaseId,headSha,event,status,createdAt \
    | jq -cS --slurpfile before "$temp/infra-before.json" '
        ($before[0] | map(.databaseId)) as $old_ids |
        [.[] | .databaseId as $id | select(($old_ids | index($id)) == null) |
         {databaseId,headSha,event,status,createdAt}]' \
      >"$temp/infra-new.json"
    test "$(jq 'length' "$temp/infra-new.json")" -le 1
    if test "$(jq 'length' "$temp/infra-new.json")" -eq 1; then break; fi
    test "$(date +%s)" -lt "$discovery_deadline"
    sleep 5
  done
  RUN_ID=$(jq -er '.[0].databaseId|tostring' "$temp/infra-new.json")
  jq -e --arg sha "$DEPLOYMENT_COMMIT" '
    length == 1 and .[0].headSha == $sha and
    .[0].event == "workflow_dispatch"' "$temp/infra-new.json"
  cloud_timeout 3600 gh run watch "$RUN_ID" --repo "$DEPLOYMENT_REPO" \
    --exit-status

  cloud_timeout 30 gh run view "$RUN_ID" --repo "$DEPLOYMENT_REPO" \
    --json databaseId,headSha,event,status,conclusion,workflowName,url,jobs \
  | jq -cS '{databaseId,headSha,event,status,conclusion,workflowName,url,
      jobs:[.jobs[] | {name,conclusion,
        steps:[.steps[] | {name,conclusion}]}]}' >"$temp/run-projection.json"
  jq -e --argjson run_id "$RUN_ID" --arg sha "$DEPLOYMENT_COMMIT" '
    .databaseId == $run_id and .headSha == $sha and
    .event == "workflow_dispatch" and
    .workflowName == "Deploy Infrastructure" and .status == "completed" and
    .conclusion == "success" and
    ([.jobs[] | select(.name == "Trigger App Deploy (prod)") |
      .steps[] | select(.name == "Trigger App Deploy Workflow") |
      .conclusion] == ["skipped"])' "$temp/run-projection.json"

  cloud_timeout 120 gh run download "$RUN_ID" --repo "$DEPLOYMENT_REPO" \
    --name tfplan-prod --dir "$temp/plan"
  mapfile -t plan_files < <(find "$temp/plan" -type f -name tfplan -print)
  mapfile -t pin_files < <(find "$temp/plan" -type f \
    -name public-source-pin.json -print)
  test "${#plan_files[@]}" -eq 1
  test "${#pin_files[@]}" -eq 1
  PLAN_SHA256=$(sha256sum "${plan_files[0]}" | awk '{print $1}')
  ENDPOINT_OVERRIDE_SHA256=$(cloud_timeout 30 gh variable list \
    --repo "$DEPLOYMENT_REPO" --env prod --json name,value | python3 -c '
import hashlib
import json
import sys

matches = [row.get("value") for row in json.load(sys.stdin)
           if row.get("name") == "ACTIVE_ALLOYDB_PRIMARY_INSTANCE_IP"]
if len(matches) > 1 or (matches and not isinstance(matches[0], str)):
    raise SystemExit("active AlloyDB endpoint variable is ambiguous")
normalized = (matches[0] if matches else "").strip()
print(hashlib.sha256(normalized.encode()).hexdigest())
')
  PLANNED_ENDPOINT_SHA256=$(python3 -c '
import hashlib
import json
import sys

with open(sys.argv[1], encoding="utf-8") as source:
    value = json.load(source)["operation_job_db_host"]
if not isinstance(value, str) or not value or value != value.strip():
    raise SystemExit("invalid signed active endpoint")
print(hashlib.sha256(value.encode()).hexdigest())
' "$SIGNED_INPUTS")
  jq -cS --arg public_sha "$PUBLIC_COMMIT" --arg plan_sha "$PLAN_SHA256" \
    --arg mode "$MODE" --arg maintenance "$MAINTENANCE" --arg image "$IMAGE" \
    --arg override_sha "$ENDPOINT_OVERRIDE_SHA256" \
    --arg planned_sha "$PLANNED_ENDPOINT_SHA256" '
    select(.schema_version == 2 and .public_sha == $public_sha and
      .terraform_plan_sha256 == $plan_sha and
      .cutover_contract.authority_mode == $mode and
      (.cutover_contract.maintenance_mode|tostring) == $maintenance and
      .cutover_contract.ingestion_image == $image and
      .cutover_contract.active_alloydb_primary_instance_ip_override_sha256 ==
        $override_sha and
      .cutover_contract.planned_active_alloydb_primary_instance_ip_sha256 ==
        $planned_sha)
    | {schema_version,public_sha,sources,terraform_plan_sha256,
       cutover_contract}' "${pin_files[0]}" >"$temp/plan-pin-projection.json"
  test -s "$temp/plan-pin-projection.json"

  cloud_timeout 30 gh run list --repo "$DEPLOYMENT_REPO" \
    --workflow app_deploy.yml \
    --event workflow_dispatch --limit 100 \
    --json databaseId,headSha,event,status,createdAt \
  | jq -cS '[.[] | {databaseId,headSha,event,status,createdAt}]' \
    >"$temp/app-after.json"
  cmp -s "$temp/app-before.json" "$temp/app-after.json"

  install -m 600 "$temp/tuple-reread.json" "$out/tuple-reread.json"
  install -m 600 "$temp/infra-before.json" "$out/infra-before.json"
  install -m 600 "$temp/infra-new.json" "$out/infra-new.json"
  install -m 600 "$temp/app-before.json" "$out/app-before.json"
  install -m 600 "$temp/app-after.json" "$out/app-after.json"
  install -m 600 "$temp/nonterminal-before-mutation.json" \
    "$out/nonterminal-before-mutation.json"
  install -m 600 "$temp/invocation-projection.json" \
    "$out/invocation-projection.json"
  install -m 600 "$temp/run-projection.json" "$out/run-projection.json"
  install -m 600 "$temp/plan-pin-projection.json" \
    "$out/plan-pin-projection.json"
  jq -ncS --arg sha "$PLAN_SHA256" '{terraform_plan_sha256:$sha}' \
    >"$out/plan-hash.json"
  sha256sum "$out"/*.json >"$out/tuple-apply.sha256"
)
```

Use exact calls: initial freeze is
`set_and_apply_ingestion_tuple legacy_feed true "$CANDIDATE_IMAGE" frozen`;
the post-bootstrap durable SID reconcile is
`set_and_apply_ingestion_tuple sid_lease true "$CANDIDATE_IMAGE" sid-durable`;
accepted control restoration is
`set_and_apply_ingestion_tuple sid_lease false "$CANDIDATE_IMAGE" accepted`;
rollback first reconciles the authority-changing template while frozen with
`set_and_apply_ingestion_tuple legacy_feed true "$CANDIDATE_IMAGE" legacy-durable`;
only after unchanged live legacy process/fleet proof does it restore automatic
controls with `set_and_apply_ingestion_tuple legacy_feed false
"$CANDIDATE_IMAGE" legacy-restored`.

## Shared primitive A: capture and compare the frozen fleet

First capture the maintained MIG response. It must contain 2–10 settled members
at the current production policy, but the frozen set is whatever complete live
inventory exists after autoscaling decisions stop; never assume two VMs.

```bash
set -euo pipefail

cloud_timeout 30 gcloud compute instance-groups managed list-instances "$MIG" \
  --project="$PROJECT" --region="$REGION" --format=json \
| jq -cS 'map({
    instance_name:(.instance | split("/")[-1]),
    zone:(.instance | split("/")[-3]),
    current_action:.currentAction,
    instance_template:((.version.instanceTemplate // "") | split("/")[-1])
  }) | sort_by(.instance_name)' \
  >"$ARTIFACT_DIR/mig-members-before.json"

jq -e 'length >= 2 and length <= 10 and
       all(.[]; .current_action == "NONE")' \
  "$ARTIFACT_DIR/mig-members-before.json"

: >"$ARTIFACT_DIR/frozen-instances.ndjson"
jq -r '.[] | [.instance_name,.zone,.current_action,.instance_template] | @tsv' \
  "$ARTIFACT_DIR/mig-members-before.json" \
| while IFS=$'\t' read -r name zone action template; do
    cloud_timeout 20 gcloud compute instances describe "$name" \
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
set -euo pipefail

: "${TARGET_MODE:?set sid_lease or legacy_feed}"
case "$TARGET_MODE" in sid_lease|legacy_feed) ;; *) exit 64 ;; esac
STAGE_PROOF="$ARTIFACT_DIR/unit-stage-proof-$TARGET_MODE.txt"
STAGE_PROOF_HASH="$ARTIFACT_DIR/unit-stage-proof-$TARGET_MODE.sha256"
test ! -e "$STAGE_PROOF"
test ! -e "$STAGE_PROOF_HASH"
: >"$STAGE_PROOF"

jq -r '.[] | [.instance_name,.zone,.numeric_instance_id] | @tsv' \
  "$ARTIFACT_DIR/frozen-instances.json" \
| while IFS=$'\t' read -r name zone expected_id; do
    live_id=$(timeout --foreground --signal=TERM --kill-after=5s 20s \
      gcloud compute instances describe "$name" \
      --project="$PROJECT" --zone="$zone" --format='value(id)')
    test "$live_id" = "$expected_id"

    timeout --foreground --signal=TERM --kill-after=5s 30s \
      gcloud compute ssh "$name" --project="$PROJECT" --zone="$zone" \
      --tunnel-through-iap --ssh-flag='-o ConnectTimeout=10' \
      --ssh-flag='-o ConnectionAttempts=1' --command="set -eu
      capture_slot() {
        index=\$1
        unit=\"$SERVICE@\${index}.service\"
        container=\"$SERVICE-\${index}\"
        main_pid=\$(systemctl show \"\$unit\" -p MainPID --value)
        case \"\$main_pid\" in *[!0-9]*|'') return 65 ;; esac
        test \"\$main_pid\" -gt 0
        ids=\$(sudo docker ps -aq --filter \"name=^/\$container\$\")
        test \"\$(printf '%s\\n' \"\$ids\" | awk 'NF' | wc -l)\" -eq 1
        image_id=\$(sudo docker inspect --format '{{.Image}}' \"\$ids\")
        configured_image=\$(sudo docker inspect --format '{{.Config.Image}}' \"\$ids\")
        printf '%s|%s|%s' \"\$main_pid\" \"\$ids\" \
          \"\$image_id@\$configured_image\"
      }
      for index in 1 2; do
        unit=\"$SERVICE@\${index}.service\"
        restart=\$(systemctl show \"\$unit\" -p Restart --value)
        enabled=\$(systemctl is-enabled \"\$unit\")
        printf 'InstanceName=%s ExpectedNumericID=%s LiveNumericID=%s OriginalUnit=%s Restart=%s Enabled=%s\\n' \
          '$name' '$expected_id' '$live_id' \
          \"\$unit\" \"\$restart\" \"\$enabled\"
        test \"\$restart\" = always
        test \"\$enabled\" = enabled
      done
      before_slot_1=\$(capture_slot 1)
      before_slot_2=\$(capture_slot 2)
      before_non_target=\$(sudo awk -F= '
        \$1!="WORKER_PROFILE" && \$1!="BCFY_CALLS_AUTHORITY_MODE" {print}' \
        /etc/container-env/$SERVICE.env)
      sudo env SERVICE='$SERVICE' \
      TARGET_MODE='$TARGET_MODE' python3 - <<'PY'
import os
import subprocess
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
    subprocess.run(
        ['systemctl', 'disable', unit],
        check=True,
        stdout=subprocess.DEVNULL,
    )

original = path.read_text(encoding='utf-8').splitlines()
kept = [line for line in original if line.split('=', 1)[0] not in targets]
rendered = kept + [f'{key}={value}' for key, value in targets.items()]
temporary = path.with_name(path.name + '.phase7-new')
temporary.write_text('\\n'.join(rendered) + '\\n', encoding='utf-8')
temporary.chmod(0o600)
temporary.replace(path)
subprocess.run(['systemctl', 'daemon-reload'], check=True)
PY
      for index in 1 2; do
        unit=\"$SERVICE@\${index}.service\"
        test \"\$(systemctl show \"\$unit\" -p Restart --value)\" = no
        test \"\$(systemctl is-enabled \"\$unit\" 2>/dev/null || true)\" = disabled
      done
      after_slot_1=\$(capture_slot 1)
      after_slot_2=\$(capture_slot 2)
      after_non_target=\$(sudo awk -F= '
        \$1!="WORKER_PROFILE" && \$1!="BCFY_CALLS_AUTHORITY_MODE" {print}' \
        /etc/container-env/$SERVICE.env)
      test \"\$after_slot_1\" = \"\$before_slot_1\"
      test \"\$after_slot_2\" = \"\$before_slot_2\"
      test \"\$after_non_target\" = \"\$before_non_target\"
      printf 'StageContinuity InstanceName=%s NumericID=%s BeforeSlot1=%s AfterSlot1=%s BeforeSlot2=%s AfterSlot2=%s NonTargetEnvEqual=true\\n' \
        '$name' '$expected_id' \"\$before_slot_1\" \"\$after_slot_1\" \
        \"\$before_slot_2\" \"\$after_slot_2\"
      sudo awk -F= '\$1==\"WORKER_PROFILE\" ||
                 \$1==\"BCFY_CALLS_AUTHORITY_MODE\" {print}' \
        /etc/container-env/$SERVICE.env | sort
      test \"\$(sudo grep -Ec '^WORKER_PROFILE=mixed-dormant$' \
        /etc/container-env/$SERVICE.env)\" -eq 1
      test \"\$(sudo grep -Ec '^BCFY_CALLS_AUTHORITY_MODE=$TARGET_MODE$' \
        /etc/container-env/$SERVICE.env)\" -eq 1" \
      >>"$STAGE_PROOF"
  done
sha256sum "$STAGE_PROOF" >"$STAGE_PROOF_HASH"
expected_rows=$((2 * $(jq 'length' "$ARTIFACT_DIR/frozen-instances.json")))
test "$(grep -c ' OriginalUnit=' "$STAGE_PROOF")" \
  -eq "$expected_rows"
jq -r '.[] | [.instance_name,.numeric_instance_id] | @tsv' \
  "$ARTIFACT_DIR/frozen-instances.json" \
| while IFS=$'\t' read -r name expected_id; do
    test "$(grep -c \
      "^InstanceName=$name ExpectedNumericID=$expected_id LiveNumericID=$expected_id " \
      "$STAGE_PROOF")" -eq 2
  done
test "$(grep -c '^StageContinuity ' "$STAGE_PROOF")" \
  -eq "$(jq 'length' "$ARTIFACT_DIR/frozen-instances.json")"
```

Capture worker MainPID, exact container ID, and image ID/digest before and after
staging. The remote shell compares every non-target environment line exactly in
memory and retains only `NonTargetEnvEqual=true` plus the two non-secret target
keys. Every PID/container and the complete numeric-ID fleet must be unchanged.
Never retain the full environment, a credential-derived hash, or the non-target
comparison values.

## Shared primitive C: bounded stop and complete process proof

Load all functions in this section into one fail-fast Bash shell before calling
the controller at the end. Every cloud/SSH operation has its own timeout inside
one wall-clock deadline. Normal runtime shutdown owns a 90-second graceful
budget inside Docker's 100-second and systemd's 120-second bounds. At T+90 the
controller emits an exact survivor artifact; it force-terminates only those
rows after another numeric-ID check. At T+120 it exits nonzero on any missing,
nonzero, late, or ambiguous row. Do not widen a target by a name pattern.

```bash
set -euo pipefail

wait_until_epoch() {
  target=$1
  while test "$(date +%s)" -lt "$target"; do
    remaining=$((target - $(date +%s)))
    test "$remaining" -gt 0 || break
    if test "$remaining" -gt 5; then sleep 5; else sleep "$remaining"; fi
  done
}

launch_timed_stop_controllers() {
  STOP_CONTROLLER_DIR="$ARTIFACT_DIR/stop-controller-$STOP_T0_EPOCH"
  test ! -e "$STOP_CONTROLLER_DIR"
  mkdir -m 700 "$STOP_CONTROLLER_DIR"
  local -a controller_pids=()
  local failed=0
  MAX_ARM_CLOCK_SKEW_SECONDS=5
  cancel_stop_controllers() {
    for pid in "${controller_pids[@]}"; do kill "$pid" 2>/dev/null || true; done
    for pid in "${controller_pids[@]}"; do wait "$pid" 2>/dev/null || true; done
  }

  while IFS=$'\t' read -r name zone expected_id; do
    (
      live_id=$(cloud_timeout 10 gcloud compute instances describe "$name" \
        --project="$PROJECT" --zone="$zone" --format='value(id)')
      test "$live_id" = "$expected_id"
      controller_budget=$((STOP_DOCKER_DEADLINE + 2 - $(date +%s)))
      test "$controller_budget" -gt 0
      cloud_timeout "$controller_budget" gcloud compute ssh "$name" \
        --project="$PROJECT" --zone="$zone" --tunnel-through-iap \
        --ssh-flag='-o ConnectTimeout=10' \
        --ssh-flag='-o ConnectionAttempts=1' --command="set -eu
        trap 'exit 70' HUP INT TERM
        metadata_id() {
          curl -fsS --max-time 2 -H 'Metadata-Flavor: Google' \
            http://metadata.google.internal/computeMetadata/v1/instance/id
        }
        wait_remote_until() {
          target=\$1
          while test \"\$(date +%s)\" -lt \"\$target\"; do
            remaining=\$((target - \$(date +%s)))
            if test \"\$remaining\" -gt 2; then sleep 2; else sleep \"\$remaining\"; fi
          done
        }
        snapshot_slot() {
          phase=\$1
          index=\$2
          module_prefix='backend.pipeline.ingestion.'
          module_name=\"\${module_prefix}main\"
          module_pattern=\$(printf '%s' \"\$module_name\" | sed 's/[.]/[.]/g')
          excluded_pids=\"\$\$\"
          ancestor_pid=\$PPID
          while test \"\$ancestor_pid\" -gt 1; do
            excluded_pids=\"\$excluded_pids \$ancestor_pid\"
            ancestor_pid=\$(ps -o ppid= -p \"\$ancestor_pid\" | awk 'NF {print \$1}')
            case \"\$ancestor_pid\" in *[!0-9]*|'') break ;; esac
          done
          unit='$SERVICE@'\$index'.service'
          container='$SERVICE-'\$index
          load=\$(systemctl show \"\$unit\" -p LoadState --value)
          active=\$(systemctl show \"\$unit\" -p ActiveState --value)
          sub=\$(systemctl show \"\$unit\" -p SubState --value)
          main=\$(systemctl show \"\$unit\" -p MainPID --value)
          control=\$(systemctl show \"\$unit\" -p ControlPID --value)
          control_group=\$(systemctl show \"\$unit\" -p ControlGroup --value)
          cgroup_count=0
          if [ -n \"\$control_group\" ] && [ \"\$control_group\" != - ] &&
             [ -d \"/sys/fs/cgroup\$control_group\" ]; then
            cgroup_count=\$(sudo find \"/sys/fs/cgroup\$control_group\" \
              -name cgroup.procs -type f -exec cat {} + 2>/dev/null \
              | awk 'NF' | sort -u | wc -l)
          fi
          ids=\$(sudo docker ps -aq --filter \"name=^/\$container\$\")
          if [ -z \"\$ids\" ]; then container_count=0
          else container_count=\$(printf '%s\\n' \"\$ids\" | awk 'NF' | wc -l); fi
          authority_count=0
          unknown_count=0
          for cid in \$(sudo docker ps -aq); do
            if sudo docker inspect --format \
                 '{{range .Config.Env}}{{println .}}{{end}}' \"\$cid\" \
                 | grep -Eq '^WORKER_INDEX=[0-9]+\$' ||
               sudo docker inspect --format \
                 '{{json .Config.Entrypoint}} {{json .Config.Cmd}}' \"\$cid\" \
                 | grep -Fq \"\$module_name\"; then
              authority_count=\$((authority_count + 1))
              authority_name=\$(sudo docker inspect --format '{{.Name}}' \"\$cid\")
              case \"\$authority_name\" in
                '/$SERVICE-1'|'/$SERVICE-2') ;;
                *) unknown_count=\$((unknown_count + 1)) ;;
              esac
            fi
          done
          host_count=\$(
            (sudo pgrep -af \"\$module_pattern\" || true) \
            | awk -v excluded=\"\$excluded_pids\" '
                BEGIN {
                  count = split(excluded, pid, /[[:space:]]+/)
                  for (i = 1; i <= count; i++) skip[pid[i]] = 1
                }
                !(\$1 in skip) { matches += 1 }
                END { print matches + 0 }
              '
          )
          printf '%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s\\n' \
            \"\$phase\" '$name' '$zone' '$expected_id' \
            \"\$metadata_numeric_id\" \"\$index\" \"\$unit\" \"\$load\" \
            \"\$active\" \"\$sub\" \
            \"\$main\" \"\$control\" \"\$cgroup_count\" \"\$container\" \
            \"\$container_count\" \"\$authority_count\" \"\$unknown_count\" \
            \"\$host_count\" \"\$(date +%s)\"
        }

        metadata_numeric_id=\$(metadata_id)
        test \"\$metadata_numeric_id\" = '$expected_id'
        test \"\$(date +%s)\" -lt '$STOP_T0_EPOCH'
        printf 'ARMED|%s|%s|%s|%s\\n' '$name' '$zone' '$expected_id' \
          \"\$(date +%s)\"
        wait_remote_until '$STOP_T0_EPOCH'
        sudo systemctl stop --no-block \
          '$SERVICE@1.service' '$SERVICE@2.service'
        wait_remote_until '$STOP_SOFT_DEADLINE'
        metadata_numeric_id=\$(metadata_id)
        test \"\$metadata_numeric_id\" = '$expected_id'

        ambiguous=0
        for index in 1 2; do
          row=\$(snapshot_slot T90 \"\$index\")
          printf '%s\\n' \"\$row\"
          main=\$(printf '%s' \"\$row\" | cut -d'|' -f11)
          control=\$(printf '%s' \"\$row\" | cut -d'|' -f12)
          cgroup=\$(printf '%s' \"\$row\" | cut -d'|' -f13)
          container_count=\$(printf '%s' \"\$row\" | cut -d'|' -f15)
          unknown_count=\$(printf '%s' \"\$row\" | cut -d'|' -f17)
          active=\$(printf '%s' \"\$row\" | cut -d'|' -f9)
          case \"\$main:\$control:\$cgroup:\$container_count:\$unknown_count\" in
            *[!0-9:]*|:*|*:) exit 65 ;;
          esac
          test \"\$unknown_count\" -eq 0 || ambiguous=1
          survivor=0
          case \"\$active\" in inactive|failed) ;; *) survivor=1 ;; esac
          if test \"\$main\" -ne 0 || test \"\$control\" -ne 0 ||
             test \"\$cgroup\" -ne 0 || test \"\$container_count\" -ne 0; then
            survivor=1
          fi
          if test \"\$survivor\" -eq 1; then
            unit='$SERVICE@'\$index'.service'
            container='$SERVICE-'\$index
            sudo systemctl kill --kill-who=all --signal=SIGKILL \"\$unit\" || true
            ids=\$(sudo docker ps -aq --filter \"name=^/\$container\$\")
            if [ -n \"\$ids\" ]; then sudo docker rm -f \$ids >/dev/null; fi
          fi
        done

        metadata_numeric_id=\$(metadata_id)
        test \"\$metadata_numeric_id\" = '$expected_id'
        test \"\$(date +%s)\" -le '$STOP_DOCKER_DEADLINE'
        for index in 1 2; do snapshot_slot T100 \"\$index\"; done
        printf 'FORCE|%s|%s|%s|%s|%s\\n' '$name' '$zone' '$expected_id' \
          \"\$metadata_numeric_id\" \"\$(date +%s)\"
        test \"\$(date +%s)\" -le '$STOP_DOCKER_DEADLINE'
        test \"\$ambiguous\" -eq 0"
    ) >"$STOP_CONTROLLER_DIR/$expected_id.stdout" \
      2>"$STOP_CONTROLLER_DIR/$expected_id.stderr" &
    controller_pids+=("$!")
  done < <(jq -r '.[] | [.instance_name,.zone,.numeric_instance_id] | @tsv' \
    "$ARTIFACT_DIR/frozen-instances.json")

  expected_vm_count=$(jq 'length' "$ARTIFACT_DIR/frozen-instances.json")
  arm_deadline=$((STOP_T0_EPOCH - 5))
  while test "$(date +%s)" -lt "$arm_deadline"; do
    armed=$({ grep -h '^ARMED|' "$STOP_CONTROLLER_DIR"/*.stdout \
      2>/dev/null || true; } | wc -l)
    test "$armed" -eq "$expected_vm_count" && break
    sleep 1
  done
  armed=$({ grep -h '^ARMED|' "$STOP_CONTROLLER_DIR"/*.stdout \
    2>/dev/null || true; } | wc -l)
  if test "$armed" -ne "$expected_vm_count"; then
    cancel_stop_controllers
    return 71
  fi
  armed_identity=$(grep -h '^ARMED|' "$STOP_CONTROLLER_DIR"/*.stdout \
    | awk -F'|' '{print $2 "|" $3 "|" $4}' | sort)
  frozen_identity=$(jq -r \
    '.[] | [.instance_name,.zone,.numeric_instance_id] | join("|")' \
    "$ARTIFACT_DIR/frozen-instances.json" | sort)
  if test "$armed_identity" != "$frozen_identity"; then
    cancel_stop_controllers
    return 72
  fi
  : >"$STOP_CONTROLLER_DIR/arm-clock-skew.ndjson"
  arm_clock_valid=1
  while IFS=$'\t' read -r name zone expected_id; do
    output="$STOP_CONTROLLER_DIR/$expected_id.stdout"
    line=$({ grep -F "ARMED|$name|$zone|$expected_id|" "$output" || true; })
    if test "$(printf '%s\n' "$line" | awk 'NF' | wc -l)" -ne 1; then
      arm_clock_valid=0
      continue
    fi
    remote_epoch=$(printf '%s' "$line" | cut -d'|' -f5)
    received_epoch=$(stat -c '%Y' "$output")
    case "$remote_epoch:$received_epoch" in *[!0-9:]*|:*|*:) arm_clock_valid=0; continue ;; esac
    delta_seconds=$((received_epoch - remote_epoch))
    if test "$delta_seconds" -lt 0; then absolute_delta=$((-delta_seconds))
    else absolute_delta=$delta_seconds; fi
    if test "$absolute_delta" -gt "$MAX_ARM_CLOCK_SKEW_SECONDS"; then
      arm_clock_valid=0
    fi
    jq -nc --arg instance_name "$name" --arg zone "$zone" \
      --arg numeric_id "$expected_id" --argjson remote_epoch "$remote_epoch" \
      --argjson received_epoch "$received_epoch" \
      --argjson delta_seconds "$delta_seconds" \
      '{instance_name:$instance_name,zone:$zone,numeric_instance_id:$numeric_id,
        remote_epoch:$remote_epoch,controller_received_epoch:$received_epoch,
        controller_minus_remote_seconds:$delta_seconds}' \
      >>"$STOP_CONTROLLER_DIR/arm-clock-skew.ndjson"
  done < <(jq -r '.[] | [.instance_name,.zone,.numeric_instance_id] | @tsv' \
    "$ARTIFACT_DIR/frozen-instances.json")
  if test "$arm_clock_valid" -ne 1; then
    cancel_stop_controllers
    return 74
  fi
  jq -s 'sort_by(.numeric_instance_id)' \
    "$STOP_CONTROLLER_DIR/arm-clock-skew.ndjson" \
    >"$ARTIFACT_DIR/arm-clock-skew-$STOP_T0_EPOCH.json"
  jq -e --argjson expected "$expected_vm_count" \
    --argjson bound "$MAX_ARM_CLOCK_SKEW_SECONDS" '
    def magnitude: if . < 0 then -. else . end;
    length == $expected and all(.[];
      (.controller_minus_remote_seconds | magnitude) <= $bound)' \
    "$ARTIFACT_DIR/arm-clock-skew-$STOP_T0_EPOCH.json"
  if test "$(date +%s)" -ge "$STOP_T0_EPOCH"; then
    cancel_stop_controllers
    return 75
  fi
  controllers_alive=1
  for pid in "${controller_pids[@]}"; do
    if ! kill -0 "$pid" 2>/dev/null; then controllers_alive=0; fi
  done
  if test "$controllers_alive" -ne 1; then
    cancel_stop_controllers
    return 73
  fi

  for pid in "${controller_pids[@]}"; do
    if ! wait "$pid"; then failed=1; fi
  done
  STOP_CONTROLLER_FAILED=$failed
}
```

The controller below constructs force targets; the former standalone force
sketch is intentionally absent because undefined `$name`/`$unit` variables or
a broad pattern are unsafe. One machine-readable row is required for every
frozen `(numeric instance ID, worker index)`:

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
  "control_pid": 0,
  "control_group_process_count": 0,
  "container_name": "icecast-collector-prod-1",
  "container_exists": false,
  "ingestion_authority_container_count": 0,
  "unknown_ingestion_authority_container_count": 0,
  "ingestion_host_process_count": 0,
  "current_action": "NONE",
  "observed_at_utc": "RFC3339"
}
```

The gate passes only when the row count is exactly `2 × frozen VM count`, each
expected and live numeric instance ID matches, both worker slots are present,
`LoadState=loaded`, `ActiveState` is `inactive` or a proved process-free
`failed`, `MainPID=ControlPID=0`, the unit cgroup is empty, the exact container
is absent even from `docker ps -a`, and a host-wide inventory finds no container
with `WORKER_INDEX` or ingestion main command and no
`backend.pipeline.ingestion.main` host process. The fleet set is unchanged and
`currentAction=NONE`. An unknown unit, unknown container, or unknown process,
SSH failure, timeout, changed ID/membership, or incomplete output blocks. Re-run this proof
immediately before and after activation, and before rollback child
normalization.

The following function produces those rows without interpreting an SSH failure
as absence. Run it only after saving a fresh maintained-instance response and
proving its sorted member names equal the frozen set:

```bash
set -euo pipefail

prove_slot() {
  name=$1 zone=$2 expected_id=$3 index=$4 proof_deadline=$5 members_file=$6
  unit="$SERVICE@$index.service"
  container="$SERVICE-$index"
  remaining=$((proof_deadline - $(date +%s)))
  test "$remaining" -gt 0
  if test "$remaining" -gt 20; then describe_timeout=20; else describe_timeout=$remaining; fi
  live_id=$(cloud_timeout "$describe_timeout" gcloud compute instances describe "$name" \
    --project="$PROJECT" --zone="$zone" --format='value(id)')
  test "$live_id" = "$expected_id"
  action=$(jq -er --arg name "$name" '
    [.[] | select(.instance_name == $name) | .current_action]
    | if length == 1 then .[0] else error("missing/duplicate member") end' \
    "$members_file")

  remaining=$((proof_deadline - $(date +%s)))
  test "$remaining" -gt 0
  report=$(cloud_timeout "$remaining" gcloud compute ssh "$name" \
    --project="$PROJECT" --zone="$zone" --tunnel-through-iap \
    --ssh-flag='-o ConnectTimeout=5' \
    --ssh-flag='-o ConnectionAttempts=1' --command="set -eu
      sudo systemctl show '$unit' --no-page \
        -p LoadState -p ActiveState -p SubState -p MainPID -p ControlPID \
        -p ControlGroup
      sudo docker info >/dev/null
      ids=\$(sudo docker ps -aq --filter 'name=^/$container\$')
      if [ -z \"\$ids\" ]; then count=0;
      else count=\$(printf '%s\\n' \"\$ids\" | wc -l); fi
      configured_image= image_id= alloydb_host_sha256= profile_count=0 sid_mode_count=0
      legacy_mode_count=0 worker_index_count=0
      if [ \"\$count\" -eq 1 ]; then
        configured_image=\$(sudo docker inspect --format '{{.Config.Image}}' \"\$ids\")
        image_id=\$(sudo docker inspect --format '{{.Image}}' \"\$ids\")
        profile_count=\$(sudo docker inspect --format \
          '{{range .Config.Env}}{{println .}}{{end}}' \"\$ids\" \
          | grep -Fxc 'WORKER_PROFILE=mixed-dormant' || true)
        sid_mode_count=\$(sudo docker inspect --format \
          '{{range .Config.Env}}{{println .}}{{end}}' \"\$ids\" \
          | grep -Fxc 'BCFY_CALLS_AUTHORITY_MODE=sid_lease' || true)
        legacy_mode_count=\$(sudo docker inspect --format \
          '{{range .Config.Env}}{{println .}}{{end}}' \"\$ids\" \
          | grep -Fxc 'BCFY_CALLS_AUTHORITY_MODE=legacy_feed' || true)
        worker_index_count=\$(sudo docker inspect --format \
          '{{range .Config.Env}}{{println .}}{{end}}' \"\$ids\" \
          | grep -Fxc 'WORKER_INDEX=$index' || true)
        alloydb_host_count=\$(sudo docker inspect --format \
          '{{range .Config.Env}}{{println .}}{{end}}' "\$ids" \
          | grep -Ec '^ALLOYDB_HOST=.+\$' || true)
        test "\$alloydb_host_count" -eq 1
        alloydb_host_sha256=\$(sudo docker inspect --format \
          '{{range .Config.Env}}{{println .}}{{end}}' "\$ids" \
          | awk -F= '\$1=="ALLOYDB_HOST" {sub(/^[^=]*=/,""); printf "%s", \$0}' \
          | sha256sum | awk '{print \$1}')
        printf '%s' "\$alloydb_host_sha256" \
          | grep -Eq '^[0-9a-f]{64}\$'
      fi
      control_group=\$(sudo systemctl show '$unit' -p ControlGroup --value)
      cgroup_count=0
      if [ -n \"\$control_group\" ] && [ \"\$control_group\" != - ] &&
         [ -d \"/sys/fs/cgroup\$control_group\" ]; then
        cgroup_count=\$(sudo find \"/sys/fs/cgroup\$control_group\" \
          -name cgroup.procs -type f -exec cat {} + 2>/dev/null \
          | awk 'NF' | sort -u | wc -l)
      fi
      authority_containers=0
      unknown_authority_containers=0
      module_prefix='backend.pipeline.ingestion.'
      module_name=\"\${module_prefix}main\"
      module_pattern=\$(printf '%s' \"\$module_name\" | sed 's/[.]/[.]/g')
      excluded_pids=\"\$\$\"
      ancestor_pid=\$PPID
      while test \"\$ancestor_pid\" -gt 1; do
        excluded_pids=\"\$excluded_pids \$ancestor_pid\"
        ancestor_pid=\$(ps -o ppid= -p \"\$ancestor_pid\" | awk 'NF {print \$1}')
        case \"\$ancestor_pid\" in *[!0-9]*|'') break ;; esac
      done
      for cid in \$(sudo docker ps -aq); do
        if sudo docker inspect --format \
             '{{range .Config.Env}}{{println .}}{{end}}' \"\$cid\" \
             | grep -Eq '^WORKER_INDEX=[0-9]+\$' ||
           sudo docker inspect --format \
             '{{json .Config.Entrypoint}} {{json .Config.Cmd}}' \"\$cid\" \
             | grep -Fq \"\$module_name\"; then
          authority_containers=\$((authority_containers + 1))
          authority_name=\$(sudo docker inspect --format '{{.Name}}' \"\$cid\")
          case \"\$authority_name\" in
            '/$SERVICE-1'|'/$SERVICE-2') ;;
            *) unknown_authority_containers=\$((unknown_authority_containers + 1)) ;;
          esac
        fi
      done
      host_processes=\$(
        (sudo pgrep -af \"\$module_pattern\" || true) \
        | awk -v excluded=\"\$excluded_pids\" '
            BEGIN {
              count = split(excluded, pid, /[[:space:]]+/)
              for (i = 1; i <= count; i++) skip[pid[i]] = 1
            }
            !(\$1 in skip) { matches += 1 }
            END { print matches + 0 }
          '
      )
      printf 'ContainerCount=%s\\nCgroupProcessCount=%s\\n' \
        \"\$count\" \"\$cgroup_count\"
      printf 'ConfiguredImage=%s\\nImageID=%s\\nProfileCount=%s\\n' \
        \"\$configured_image\" \"\$image_id\" \"\$profile_count\"
      printf 'AlloyDBHostSHA256=%s\\n' \"\$alloydb_host_sha256\"
      printf 'SidModeCount=%s\\nLegacyModeCount=%s\\nWorkerIndexCount=%s\\n' \
        \"\$sid_mode_count\" \"\$legacy_mode_count\" \"\$worker_index_count\"
      printf 'AuthorityContainerCount=%s\\nUnknownAuthorityContainerCount=%s\\n' \
        \"\$authority_containers\" \"\$unknown_authority_containers\"
      printf 'HostProcessCount=%s\\n' \"\$host_processes\"")

  load_state=$(printf '%s\n' "$report" | awk -F= '$1=="LoadState"{print $2}')
  active_state=$(printf '%s\n' "$report" | awk -F= '$1=="ActiveState"{print $2}')
  sub_state=$(printf '%s\n' "$report" | awk -F= '$1=="SubState"{print $2}')
  main_pid=$(printf '%s\n' "$report" | awk -F= '$1=="MainPID"{print $2}')
  control_pid=$(printf '%s\n' "$report" | awk -F= '$1=="ControlPID"{print $2}')
  container_count=$(printf '%s\n' "$report" \
    | awk -F= '$1=="ContainerCount"{print $2}')
  cgroup_count=$(printf '%s\n' "$report" \
    | awk -F= '$1=="CgroupProcessCount"{print $2}')
  configured_image=$(printf '%s\n' "$report" \
    | awk -F= '$1=="ConfiguredImage"{sub(/^[^=]*=/,""); print}')
  image_id=$(printf '%s\n' "$report" \
    | awk -F= '$1=="ImageID"{sub(/^[^=]*=/,""); print}')
  alloydb_host_sha256=$(printf '%s\n' "$report" \
    | awk -F= '$1=="AlloyDBHostSHA256"{print $2}')
  profile_count=$(printf '%s\n' "$report" \
    | awk -F= '$1=="ProfileCount"{print $2}')
  sid_mode_count=$(printf '%s\n' "$report" \
    | awk -F= '$1=="SidModeCount"{print $2}')
  legacy_mode_count=$(printf '%s\n' "$report" \
    | awk -F= '$1=="LegacyModeCount"{print $2}')
  worker_index_count=$(printf '%s\n' "$report" \
    | awk -F= '$1=="WorkerIndexCount"{print $2}')
  authority_containers=$(printf '%s\n' "$report" \
    | awk -F= '$1=="AuthorityContainerCount"{print $2}')
  unknown_authority_containers=$(printf '%s\n' "$report" \
    | awk -F= '$1=="UnknownAuthorityContainerCount"{print $2}')
  host_processes=$(printf '%s\n' "$report" \
    | awk -F= '$1=="HostProcessCount"{print $2}')
  test -n "$load_state" && test -n "$active_state" && test -n "$sub_state"
  case "$main_pid:$control_pid:$container_count:$cgroup_count:$profile_count:$sid_mode_count:$legacy_mode_count:$worker_index_count:$authority_containers:$unknown_authority_containers:$host_processes" in
    *[!0-9:]*|:*|*:) exit 65 ;;
  esac

  jq -nc --arg instance_name "$name" --arg zone "$zone" \
    --arg expected_id "$expected_id" --arg live_id "$live_id" \
    --argjson worker_index "$index" --arg unit "$unit" \
    --arg load_state "$load_state" --arg active_state "$active_state" \
    --arg sub_state "$sub_state" --argjson main_pid "$main_pid" \
    --argjson control_pid "$control_pid" --argjson cgroup_count "$cgroup_count" \
    --arg container_name "$container" --arg configured_image "$configured_image" \
    --arg image_id "$image_id" --arg alloydb_host_sha256 "$alloydb_host_sha256" \
    --argjson profile_count "$profile_count" \
    --argjson sid_mode_count "$sid_mode_count" \
    --argjson legacy_mode_count "$legacy_mode_count" \
    --argjson worker_index_count "$worker_index_count" \
    --argjson container_count "$container_count" \
    --argjson authority_containers "$authority_containers" \
    --argjson unknown_authority_containers "$unknown_authority_containers" \
    --argjson host_processes "$host_processes" --arg action "$action" \
    --arg observed_at_utc "$(date -u +%Y-%m-%dT%H:%M:%SZ)" '
      {instance_name:$instance_name, zone:$zone,
       expected_numeric_instance_id:$expected_id,
       live_numeric_instance_id:$live_id, worker_index:$worker_index,
       unit:$unit, load_state:$load_state, active_state:$active_state,
       sub_state:$sub_state, main_pid:$main_pid, control_pid:$control_pid,
       control_group_process_count:$cgroup_count,
       container_name:$container_name,
       container_exists:($container_count != 0),
       configured_image:$configured_image, image_id:$image_id,
       alloydb_host_sha256:$alloydb_host_sha256,
       profile_count:$profile_count, sid_mode_count:$sid_mode_count,
       legacy_mode_count:$legacy_mode_count,
       worker_index_count:$worker_index_count,
       ingestion_authority_container_count:$authority_containers,
       unknown_ingestion_authority_container_count:$unknown_authority_containers,
       ingestion_host_process_count:$host_processes,
       current_action:$action, frozen_inventory_equal:true,
       observed_at_utc:$observed_at_utc}'
}

EXPECTED_SLOT_COUNT=$((2 * $(jq 'length' \
  "$ARTIFACT_DIR/frozen-instances.json")))

collect_process_proof() {
  label=$1
  proof_timeout=$2
  template_policy=${3:-exact}
  local frozen_members current_members
  case "$template_policy" in exact|target-may-change) ;; *) return 64 ;; esac
  test "$proof_timeout" -gt 0
  proof_deadline=$(($(date +%s) + proof_timeout))
  case "$label" in *[!A-Za-z0-9._-]*|'') return 64 ;; esac
  proof_dir="$ARTIFACT_DIR/process-proof-$label"
  test ! -e "$proof_dir"
  mkdir -m 700 "$proof_dir"
  members_file="$proof_dir/mig-members.json"
  remaining=$((proof_deadline - $(date +%s)))
  test "$remaining" -gt 0
  if test "$remaining" -gt 20; then list_timeout=20; else list_timeout=$remaining; fi
  cloud_timeout "$list_timeout" gcloud compute instance-groups managed list-instances "$MIG" \
    --project="$PROJECT" --region="$REGION" --format=json \
  | jq -cS 'map({
      instance_name:(.instance | split("/")[-1]),
      zone:(.instance | split("/")[-3]),
      current_action:.currentAction,
      instance_template:((.version.instanceTemplate // "") | split("/")[-1])
    }) | sort_by(.instance_name)' >"$members_file"
  if test "$template_policy" = exact; then
    frozen_members=$(jq -c '
      [.[] | {instance_name,zone,current_action,instance_template}]
      | sort_by(.instance_name)' "$ARTIFACT_DIR/frozen-instances.json")
    current_members=$(jq -c 'sort_by(.instance_name)' "$members_file")
  else
    frozen_members=$(jq -c '
      [.[] | {instance_name,zone,current_action}] | sort_by(.instance_name)' \
      "$ARTIFACT_DIR/frozen-instances.json")
    current_members=$(jq -c '
      [.[] | {instance_name,zone,current_action}] | sort_by(.instance_name)' \
      "$members_file")
  fi
  test "$current_members" = "$frozen_members"
  jq -e 'all(.[]; .current_action == "NONE")' "$members_file"

  local -a proof_pids=()
  local failed=0
  while IFS=$'\t' read -r name zone expected_id; do
    for index in 1 2; do
      (prove_slot "$name" "$zone" "$expected_id" "$index" \
        "$proof_deadline" "$members_file" \
        >"$proof_dir/$expected_id-$index.json") &
      proof_pids+=("$!")
    done
  done < <(jq -r '.[] | [.instance_name,.zone,.numeric_instance_id] | @tsv' \
    "$ARTIFACT_DIR/frozen-instances.json")
  for pid in "${proof_pids[@]}"; do
    if ! wait "$pid"; then failed=1; fi
  done
  test "$failed" -eq 0
  jq -s 'sort_by(.expected_numeric_instance_id,.worker_index)' \
    "$proof_dir"/[0-9]*-[12].json >"$proof_dir/process-proof.json"
  jq -e --argjson expected "$EXPECTED_SLOT_COUNT" '
    length == $expected and
    ([.[] | .expected_numeric_instance_id + ":" + (.worker_index|tostring)]
      | unique | length) == $expected and
    all(.[];
      .expected_numeric_instance_id == .live_numeric_instance_id and
      (.worker_index == 1 or .worker_index == 2) and
      .current_action == "NONE" and .frozen_inventory_equal == true)' \
    "$proof_dir/process-proof.json"
  sha256sum "$proof_dir/mig-members.json" "$proof_dir/process-proof.json" \
    >"$proof_dir/proof.sha256"
  test "$(date +%s)" -le "$proof_deadline"
  PROCESS_PROOF_JSON="$proof_dir/process-proof.json"
}

assert_process_absent() {
  proof=$1
  jq -e --argjson expected "$EXPECTED_SLOT_COUNT" --arg service "$SERVICE" '
    length == $expected and all(.[];
      .load_state == "loaded" and
      (.active_state == "inactive" or .active_state == "failed") and
      .unit == ($service + "@" + (.worker_index|tostring) + ".service") and
      .container_name == ($service + "-" + (.worker_index|tostring)) and
      .main_pid == 0 and .control_pid == 0 and
      .control_group_process_count == 0 and .container_exists == false and
      .ingestion_authority_container_count == 0 and
      .unknown_ingestion_authority_container_count == 0 and
      .ingestion_host_process_count == 0 and .current_action == "NONE")' "$proof"
}

reprove_process_absent() {
  label=$1
  collect_process_proof "$label" 20
  assert_process_absent "$PROCESS_PROOF_JSON"
}
```

Run the controller once. It chooses a common T0 30 seconds in the future and
requires every exact VM session to emit `ARMED` by T0-5. Each already-open SSH
session stops both exact slots at T0, inventories and force-cleans only those
slots at T+90, and emits a second host-local proof by T+100. Thus no new IAP
handshake is placed in the ten-second Docker force window. The operator then
joins that proof to one bounded maintained-instance/action read by T+120.
Every background wait joins only a command with an absolute deadline. Before
T0, each remote `ARMED` epoch is compared with the controller-side stdout file
arrival epoch; the absolute combined clock/transport delta must be at most five
seconds and is retained per numeric VM. A wider or malformed delta cancels the
armed sessions and blocks the stop.

```bash
parse_stop_phase() {
  phase=$1
  output=$2
  jq -Rsc --arg phase "$phase" '
    split("\n")
    | map(select(startswith($phase + "|")) | split("|"))
    | map(select(length == 19) | {
        phase:.[0], instance_name:.[1], zone:.[2],
        expected_numeric_instance_id:.[3], live_numeric_instance_id:.[4],
        worker_index:(.[5]|tonumber), unit:.[6], load_state:.[7],
        active_state:.[8], sub_state:.[9], main_pid:(.[10]|tonumber),
        control_pid:(.[11]|tonumber),
        control_group_process_count:(.[12]|tonumber), container_name:.[13],
        container_exists:((.[14]|tonumber) != 0),
        ingestion_authority_container_count:(.[15]|tonumber),
        unknown_ingestion_authority_container_count:(.[16]|tonumber),
        ingestion_host_process_count:(.[17]|tonumber),
        observed_at_epoch:(.[18]|tonumber)
      }) | sort_by(.expected_numeric_instance_id,.worker_index)' \
    "$STOP_CONTROLLER_DIR"/*.stdout >"$output"
}

run_bounded_stop_controller() {
  STOP_T0_EPOCH=$(($(date +%s) + 30))
  STOP_T0=$(date -u -d "@$STOP_T0_EPOCH" +%Y-%m-%dT%H:%M:%SZ)
  STOP_SOFT_DEADLINE=$((STOP_T0_EPOCH + 90))
  STOP_DOCKER_DEADLINE=$((STOP_T0_EPOCH + 100))
  STOP_HARD_DEADLINE=$((STOP_T0_EPOCH + 120))
  export STOP_T0 STOP_T0_EPOCH STOP_SOFT_DEADLINE STOP_DOCKER_DEADLINE
  export STOP_HARD_DEADLINE

  launch_timed_stop_controllers
  parse_stop_phase T90 "$ARTIFACT_DIR/stop-t90-$STOP_T0_EPOCH.json"
  parse_stop_phase T100 "$ARTIFACT_DIR/stop-t100-$STOP_T0_EPOCH.json"

  jq -e --argjson expected "$EXPECTED_SLOT_COUNT" \
    --arg service "$SERVICE" \
    --argjson soft "$STOP_SOFT_DEADLINE" \
    --argjson docker "$STOP_DOCKER_DEADLINE" '
    length == $expected and
    ([.[] | .expected_numeric_instance_id + ":" + (.worker_index|tostring)]
      | unique | length) == $expected and
    all(.[];
      .expected_numeric_instance_id == .live_numeric_instance_id and
      (.worker_index == 1 or .worker_index == 2) and
      .unit == ($service + "@" + (.worker_index|tostring) + ".service") and
      .container_name == ($service + "-" + (.worker_index|tostring)) and
      .load_state == "loaded" and
      .unknown_ingestion_authority_container_count == 0 and
      .observed_at_epoch >= $soft and .observed_at_epoch <= $docker)' \
    "$ARTIFACT_DIR/stop-t90-$STOP_T0_EPOCH.json"
  jq '[.[] | select(
      .main_pid != 0 or .control_pid != 0 or
      .control_group_process_count != 0 or .container_exists or
      (.active_state != "inactive" and .active_state != "failed"))]' \
    "$ARTIFACT_DIR/stop-t90-$STOP_T0_EPOCH.json" \
    >"$ARTIFACT_DIR/stop-survivors-$STOP_T0_EPOCH.json"

  jq -Rsc '
    split("\n")
    | map(select(startswith("FORCE|")) | split("|"))
    | map(select(length == 6) | {
        instance_name:.[1], zone:.[2], expected_numeric_instance_id:.[3],
        live_numeric_instance_id:.[4], completed_at_epoch:(.[5]|tonumber)
      }) | sort_by(.expected_numeric_instance_id)' \
    "$STOP_CONTROLLER_DIR"/*.stdout \
    >"$ARTIFACT_DIR/stop-force-$STOP_T0_EPOCH.json"
  jq -e --argjson expected "$(jq 'length' \
      "$ARTIFACT_DIR/frozen-instances.json")" \
    --argjson deadline "$STOP_DOCKER_DEADLINE" '
    length == $expected and
    ([.[].expected_numeric_instance_id] | unique | length) == $expected and
    all(.[]; .expected_numeric_instance_id == .live_numeric_instance_id and
      .completed_at_epoch <= $deadline)' \
    "$ARTIFACT_DIR/stop-force-$STOP_T0_EPOCH.json"
  jq -e --argjson expected "$EXPECTED_SLOT_COUNT" \
    --arg service "$SERVICE" \
    --argjson soft "$STOP_SOFT_DEADLINE" \
    --argjson deadline "$STOP_DOCKER_DEADLINE" '
    length == $expected and
    ([.[] | .expected_numeric_instance_id + ":" + (.worker_index|tostring)]
      | unique | length) == $expected and all(.[];
      .expected_numeric_instance_id == .live_numeric_instance_id and
      .unit == ($service + "@" + (.worker_index|tostring) + ".service") and
      .container_name == ($service + "-" + (.worker_index|tostring)) and
      .load_state == "loaded" and
      (.active_state == "inactive" or .active_state == "failed") and
      .main_pid == 0 and .control_pid == 0 and
      .control_group_process_count == 0 and .container_exists == false and
      .ingestion_authority_container_count == 0 and
      .unknown_ingestion_authority_container_count == 0 and
      .ingestion_host_process_count == 0 and
      .observed_at_epoch >= $soft and .observed_at_epoch <= $deadline)' \
    "$ARTIFACT_DIR/stop-t100-$STOP_T0_EPOCH.json"
  test "$STOP_CONTROLLER_FAILED" -eq 0

  printf 'STOP_T0=%s STOP_SOFT_DEADLINE=%s STOP_DOCKER_DEADLINE=%s MaxArmClockSkewSeconds=%s ControllersJoinedAt=%s\n' \
    "$STOP_T0" "$STOP_SOFT_DEADLINE" "$STOP_DOCKER_DEADLINE" \
    "$MAX_ARM_CLOCK_SKEW_SECONDS" "$(date +%s)" \
    >"$ARTIFACT_DIR/stop-deadlines-$STOP_T0_EPOCH.txt"

  remaining=$((STOP_HARD_DEADLINE - $(date +%s)))
  test "$remaining" -gt 2
  if test "$remaining" -gt 10; then list_timeout=10; else list_timeout=$((remaining - 2)); fi
  cloud_timeout "$list_timeout" \
    gcloud compute instance-groups managed list-instances "$MIG" \
    --project="$PROJECT" --region="$REGION" --format=json \
  | jq -cS 'map({
      instance_name:(.instance | split("/")[-1]),
      zone:(.instance | split("/")[-3]),
      current_action:.currentAction,
      instance_template:((.version.instanceTemplate // "") | split("/")[-1])
    }) | sort_by(.instance_name)' \
    >"$ARTIFACT_DIR/stop-final-members-$STOP_T0_EPOCH.json"
  test "$(jq -c 'sort_by(.instance_name)' \
    "$ARTIFACT_DIR/stop-final-members-$STOP_T0_EPOCH.json")" = \
    "$(jq -c '[.[] | {instance_name,zone,current_action,instance_template}]
      | sort_by(.instance_name)' "$ARTIFACT_DIR/frozen-instances.json")"
  jq -e 'all(.[]; .current_action == "NONE")' \
    "$ARTIFACT_DIR/stop-final-members-$STOP_T0_EPOCH.json"
  jq --slurpfile members \
    "$ARTIFACT_DIR/stop-final-members-$STOP_T0_EPOCH.json" '
    map(. as $row
      | ($members[0] | map(select(
          .instance_name == $row.instance_name))) as $match
      | if ($match|length) != 1 then error("missing/duplicate MIG member")
        else . + {current_action:$match[0].current_action,
          frozen_inventory_equal:true,
          observed_at_utc:(.observed_at_epoch | todateiso8601)} end)' \
    "$ARTIFACT_DIR/stop-t100-$STOP_T0_EPOCH.json" \
    >"$ARTIFACT_DIR/process-proof.json"
  assert_process_absent "$ARTIFACT_DIR/process-proof.json"
  test "$(date +%s)" -le "$STOP_HARD_DEADLINE"
  sha256sum "$STOP_CONTROLLER_DIR"/*.stdout "$STOP_CONTROLLER_DIR"/*.stderr \
    "$ARTIFACT_DIR/stop-t90-$STOP_T0_EPOCH.json" \
    "$ARTIFACT_DIR/stop-t100-$STOP_T0_EPOCH.json" \
    "$ARTIFACT_DIR/stop-force-$STOP_T0_EPOCH.json" \
    "$ARTIFACT_DIR/arm-clock-skew-$STOP_T0_EPOCH.json" \
    "$ARTIFACT_DIR/stop-final-members-$STOP_T0_EPOCH.json" \
    "$ARTIFACT_DIR/process-proof.json" \
    >"$ARTIFACT_DIR/process-proof.sha256"
  test "$(date +%s)" -le "$STOP_HARD_DEADLINE"
}

run_bounded_stop_controller
```

Any pre-arm/SSH/identity failure cancels the exact local controller processes,
but legacy slots may be partially stopped if a remote session crossed T0 during
the failure. New SID authority remains stopped; legacy state is unknown until a
new complete proof and reviewed recovery. Any failure at or after T0 likewise
enters incident ownership and never implies fleet-wide absence. Immediately
before/after activation and before inverse child SQL, call
`reprove_process_absent` with a new unique label; never reuse an artifact.

## Shared primitive D: controlled SQL job

The job and its mounted SQL are mutable cloud resources, so a name is never
trusted by itself. Under the deploy and bucket-writer freeze, attest a bounded
job projection and the generation plus byte hash of all four mounted objects.
The signed input manifest supplies the expected service account, image, direct
5432 host/port/database, exact configured environment-key set, numeric secret
version, one exact network/subnetwork pair, SQL bucket, and Job generation.
Any mismatch blocks execution. Full provider responses are never retained: each describe
streams directly into the bounded projection that is asserted and hashed.
Those retained Job/Execution projections contain public resource identity,
key names, hashes, and match-only booleans; they never contain the database
endpoint/name, secret ID/version, bucket, network, subnetwork, command array,
or a plaintext password value. SQL objects download into a process-local
mode-0700 temporary directory and enter the evidence directory only after
their signed generation and byte digest match the committed SQL.

```bash
set -euo pipefail

SQL_BUCKET=$(jq -er '.sql_bucket' "$SIGNED_INPUTS")
EXPECTED_JOB_GENERATION=$(jq -er '.operation_job_generation|tostring' "$SIGNED_INPUTS")
EXPECTED_JOB_UID=$(jq -er '.operation_job_uid' "$SIGNED_INPUTS")
EXPECTED_JOB_SA=$(jq -er '.operation_job_service_account' "$SIGNED_INPUTS")
EXPECTED_JOB_IMAGE=$(jq -er '.operation_job_image' "$SIGNED_INPUTS")
EXPECTED_JOB_COMMAND_SHA256=$(jq -er '.operation_job_command_sha256' "$SIGNED_INPUTS")
EXPECTED_DB_HOST=$(jq -er '.operation_job_db_host' "$SIGNED_INPUTS")
EXPECTED_DB_NAME=$(jq -er '.operation_job_db_name' "$SIGNED_INPUTS")
EXPECTED_DB_SECRET=$(jq -er '.postgres_secret_id' "$SIGNED_INPUTS")
EXPECTED_DB_SECRET_VERSION=$(jq -er '.postgres_secret_version|tostring' "$SIGNED_INPUTS")
EXPECTED_NETWORK=$(jq -er '.operation_job_network' "$SIGNED_INPUTS")
EXPECTED_SUBNETWORK=$(jq -er '.operation_job_subnetwork' "$SIGNED_INPUTS")
printf '%s' "$EXPECTED_DB_SECRET_VERSION" | grep -Eq '^[1-9][0-9]*$'

attest_operation_job() {
  local label=$1 projection job_attestation_with_command command_sha256
  case "$label" in *[!A-Za-z0-9._-]*|'') return 64 ;; esac
  projection="$ARTIFACT_DIR/operation-job-$label-projection.json"
  test ! -e "$projection" || return 70
  if ! job_attestation_with_command=$(cloud_timeout 20 \
    gcloud run jobs describe "$OP_JOB" \
    --project="$PROJECT" --region="$REGION" --format=json \
  | jq -cS --arg expected_job_uri "$EXPECTED_JOB_URI" \
    --arg expected_service_account "$EXPECTED_JOB_SA" \
    --arg expected_image "$EXPECTED_JOB_IMAGE" \
    --arg expected_db_host "$EXPECTED_DB_HOST" \
    --arg expected_db_name "$EXPECTED_DB_NAME" \
    --arg expected_secret "$EXPECTED_DB_SECRET" \
    --arg expected_secret_version "$EXPECTED_DB_SECRET_VERSION" \
    --arg expected_sql_bucket "$SQL_BUCKET" \
    --arg expected_network "$EXPECTED_NETWORK" \
    --arg expected_subnetwork "$EXPECTED_SUBNETWORK" '
    def declared_network_interfaces:
      [
        (.. | objects
          | .annotations?["run.googleapis.com/network-interfaces"]?
          | fromjson? | .[]?),
        (.. | objects
          | (.networkInterfaces? // .network_interfaces? // empty)
          | select(type == "array") | .[]?)
      ];
    def declared_network_interface_pairs:
      (declared_network_interfaces
        | map({network:(.network // null),
               subnetwork:(.subnetwork // null)})
        | unique);
    {
    expected_job_uri:$expected_job_uri,
    name:(.metadata.name // .name),
    uid:(.metadata.uid // .uid),
    generation:(.metadata.generation // .generation),
    task_contract_matches:
      ([..|objects|(.taskCount? // .task_count?) // empty] == [1]),
    retry_contract_matches:
      ([..|objects|(.maxRetries? // .max_retries?) // empty] == [0]),
    timeout_contract_matches:(
      [..|objects|
        (.timeout? // .timeoutSeconds? // .timeout_seconds?) // empty | tostring]
      | (. == ["300s"] or . == ["300"])),
    service_account_matches:([..|objects|
      (.serviceAccount? // .serviceAccountName?) // empty]
      == [$expected_service_account]),
    image_matches:([..|objects|.image? // empty] == [$expected_image]),
    commands:[..|objects|.command? | select(type=="array")],
    argument_contract_matches:([..|objects|.args? | select(type=="array")]
      == [["bcfy-calls-sid-operation","verify"]]),
    db_host_matches:([..|objects|select(.name?=="DB_HOST")|.value? // empty]
      == [$expected_db_host]),
    db_port_matches:([..|objects|select(.name?=="DB_PORT")|.value? // empty]
      == ["5432"]),
    db_user_matches:([..|objects|select(.name?=="DB_USER")|.value? // empty]
      == ["postgres"]),
    db_name_matches:([..|objects|select(.name?=="DB_NAME")|.value? // empty]
      == [$expected_db_name]),
    env_names:([..|objects|
      select((.name? | type) == "string" and
             (has("value") or has("valueSource") or has("valueFrom")))|
      .name] | sort),
    password_ref_matches:([..|objects|select(.name?=="PGPASSWORD")|
      ((.valueSource.secretKeyRef? // .valueFrom.secretKeyRef?) // empty) |
      {secret:(.secret // .name // ""),
       version:((.version // .key // "")|tostring)}]
      == [{secret:$expected_secret,version:$expected_secret_version}]),
    plaintext_password_count:([..|objects|select(.name?=="PGPASSWORD")|
      .value? // empty] | length),
    gcs_volume_matches:([..|objects|
      if .gcs?.bucket? != null then
        {name:(.name // ""),bucket:.gcs.bucket,
         read_only:(.gcs.readOnly // .gcs.read_only // false)}
      elif (.csi?.driver? == "gcsfuse.run.googleapis.com" and
            .csi?.volumeAttributes?.bucketName? != null) then
        {name:(.name // ""),bucket:.csi.volumeAttributes.bucketName,
         read_only:(.csi.readOnly // .csi.read_only // false)}
      else empty end]
      == [{name:"sql-files",bucket:$expected_sql_bucket,read_only:true}]),
    volume_mount_matches:([..|objects|
      select((.mountPath? // .mount_path?) != null)|
      {name:(.name // ""),mount_path:(.mountPath // .mount_path)}]
      == [{name:"sql-files",mount_path:"/sql"}]),
    network_interface_matches:
      (declared_network_interface_pairs ==
        [{network:$expected_network,subnetwork:$expected_subnetwork}])
  }'); then
    return 70
  fi
  if ! command_sha256=$(printf '%s' "$job_attestation_with_command" \
    | jq -cS '.commands' | sha256sum | awk '{print $1}'); then
    return 70
  fi
  test "$command_sha256" = "$EXPECTED_JOB_COMMAND_SHA256" || return 70
  if ! printf '%s' "$job_attestation_with_command" \
    | jq -cS --arg command_sha256 "$command_sha256" \
      'del(.commands) | . + {command_sha256:$command_sha256}' \
      >"$projection"; then
    return 70
  fi
  if ! jq -e --arg name "$OP_JOB" --arg uri "$EXPECTED_JOB_URI" \
    --arg uid "$EXPECTED_JOB_UID" \
    --arg generation "$EXPECTED_JOB_GENERATION" \
    --arg command_sha256 "$EXPECTED_JOB_COMMAND_SHA256" '
    .expected_job_uri == $uri and
    (.name == $uri or .name == $name or (.name | endswith("/jobs/" + $name))) and
    .uid == $uid and (.generation|tostring) == $generation and
    .task_contract_matches and .retry_contract_matches and
    .timeout_contract_matches and .service_account_matches and
    .image_matches and .argument_contract_matches and
    .db_host_matches and .db_port_matches and .db_user_matches and
    .db_name_matches and
    .env_names == ["DB_HOST","DB_NAME","DB_PORT","DB_USER","PGPASSWORD"] and
    .password_ref_matches and .plaintext_password_count == 0 and
    .gcs_volume_matches and .volume_mount_matches and
    .network_interface_matches and
    .command_sha256 == $command_sha256' "$projection"; then
    return 70
  fi
  sha256sum "$projection" >"$ARTIFACT_DIR/operation-job-$label.sha256" || \
    return 70
  JOB_ATTEST_PROJECTION="$projection"
  JOB_ATTEST_COMMAND_SHA256="$command_sha256"
}

attest_operation_sql() (
  set -euo pipefail
  label=$1
  case "$label" in *[!A-Za-z0-9._-]*|'') return 64 ;; esac
  sql_snapshot="$ARTIFACT_DIR/operation-sql-$label"
  test ! -e "$sql_snapshot"
  mkdir -m 700 "$sql_snapshot"
  download_dir=$(mktemp -d)
  chmod 700 "$download_dir"
  trap 'rm -rf -- "$download_dir"' EXIT
  for file in 001_preseed.sql 002_activate.sql \
              003_rollback_children.sql 004_verify.sql; do
    object="gs://$SQL_BUCKET/operations/bcfy_calls_sid/$file"
    expected_generation=$(jq -er --arg file "$file" '
      .operation_sql_generations[$file] | tostring
      | select(test("^[1-9][0-9]*$"))' "$SIGNED_INPUTS")
    expected_sha256=$(jq -er --arg file "$file" '
      .operation_sql_sha256[$file]
      | select(type == "string" and test("^[0-9a-f]{64}$"))' \
      "$SIGNED_INPUTS")
    committed_sql="terraform/modules/alloydb/sql/operations/bcfy_calls_sid/$file"
    test "$(sha256sum "$committed_sql" | awk '{print $1}')" = \
      "$expected_sha256"

    cloud_timeout 20 gcloud storage objects describe "$object" \
      --project="$PROJECT" --format=json \
    | jq -cS '{name:((.name // "") | split("/")[-1]),
        generation:(.generation|tostring),size:(.size|tonumber),
        crc32c:(.crc32c // null),
        md5_hash:(.md5Hash // .md5_hash // null)}' \
      >"$sql_snapshot/$file.object-before-projection.json"
    jq -e --arg file "$file" --arg generation "$expected_generation" '
      .name == $file and .generation == $generation and .size > 0 and
      (.crc32c == null or (.crc32c | type == "string")) and
      (.md5_hash == null or (.md5_hash | type == "string"))' \
      "$sql_snapshot/$file.object-before-projection.json"

    cloud_timeout 30 gcloud storage cp "$object" "$download_dir/$file"
    test "$(sha256sum "$download_dir/$file" | awk '{print $1}')" = \
      "$expected_sha256"
    cloud_timeout 20 gcloud storage objects describe "$object" \
      --project="$PROJECT" --format=json \
    | jq -cS '{name:((.name // "") | split("/")[-1]),
        generation:(.generation|tostring),size:(.size|tonumber),
        crc32c:(.crc32c // null),
        md5_hash:(.md5Hash // .md5_hash // null)}' \
      >"$sql_snapshot/$file.object-after-projection.json"
    jq -e --arg file "$file" --arg generation "$expected_generation" '
      .name == $file and .generation == $generation and .size > 0' \
      "$sql_snapshot/$file.object-after-projection.json"
    cmp -s "$sql_snapshot/$file.object-before-projection.json" \
      "$sql_snapshot/$file.object-after-projection.json"

    # Invalid or mutable bytes never enter the retained evidence snapshot.
    install -m 600 "$download_dir/$file" "$sql_snapshot/$file"
  done
  jq -s 'map({name,generation:(.generation|tostring),size:(.size|tonumber),
      crc32c:(.crc32c // null),md5_hash:(.md5_hash // null)}) |
      sort_by(.name)' "$sql_snapshot"/*.object-after-projection.json \
    >"$sql_snapshot/object-generations.json"
  for file in "$sql_snapshot"/*.sql; do
    printf '%s\t%s\n' "$(basename "$file")" \
      "$(sha256sum "$file" | awk '{print $1}')"
  done | jq -Rsc 'split("\n")[:-1] | map(split("\t")) |
    map({key:.[0],value:.[1]}) | from_entries' \
    >"$sql_snapshot/content-sha256.json"
  jq -e --argjson expected "$(jq '.operation_sql_sha256' "$SIGNED_INPUTS")" \
    '. == $expected' "$sql_snapshot/content-sha256.json"
  jq -e --argjson expected "$(jq '.operation_sql_generations' "$SIGNED_INPUTS")" '
    map({key:(.name|split("/")[-1]),value:.generation}) | from_entries
    | . == $expected' "$sql_snapshot/object-generations.json"
  sha256sum "$sql_snapshot"/*.sql \
    "$sql_snapshot"/*.object-*-projection.json \
    "$sql_snapshot/object-generations.json" "$sql_snapshot/content-sha256.json" \
    >"$sql_snapshot/snapshot.sha256"
)

attest_operation_job preflight
attest_operation_sql preflight
```

Keep the exact generation/object snapshot frozen. Run one closed operation at a
time with this wrapper. It captures the exact execution name, describes that
execution, and hashes a bounded execution-scoped SQL report. The required
sentinel remains argument zero. With the reviewed GA gcloud surface, execute
and list use the validated short Job ID and execution describe uses the
validated short execution ID, always with explicit signed project and region.
The wrapper constructs and retains the canonical Job-qualified execution URI;
short CLI identifiers are never treated as canonical resource identity. Source
Job UID/generation continuity comes only from the immutable `before` and
`after` Job projections, never from Execution metadata.

```bash
set -euo pipefail

collect_operation_log_evidence() (
  set -euo pipefail
  execution_dir=$1
  execution_id=$2
  stdout_log="projects/$PROJECT/logs/run.googleapis.com%2Fstdout"
  safe_temp=$(mktemp -d)
  chmod 700 "$safe_temp"
  trap 'rm -rf -- "$safe_temp"' EXIT
  for retained in logs-first.projected.json logs-second.projected.json \
    sql-report.json; do
    test ! -e "$execution_dir/$retained"
  done

  # Cloud Logging is eventually consistent. The CLI emits only five selected
  # fields into process-local temporary storage. An exact reducer then accepts
  # only bounded stdout rows with a closed schema and a conservative text
  # alphabet; stderr, JSON payloads, URLs, credentials, and unknown shapes can
  # never enter retained evidence. Two identical projected reads are required.
  sleep 60
  log_filter='resource.type="cloud_run_job"
AND resource.labels.job_name="'"$OP_JOB"'"
AND labels."run.googleapis.com/execution_name"="'"$execution_id"'"
AND logName="'"$stdout_log"'"'
  project_operation_log_read() {
    local output=$1
    jq -cS --arg stdout_log "$stdout_log" '
      def nonempty: type == "string" and length > 0;
      def safe_text:
        type == "string" and
        (utf8bytelength > 0 and utf8bytelength <= 4096) and
        test("^[\\t\\n\\r -~]+$") and
        (test("(?i)(password|credential|secret|bearer|authorization|signed[ _-]?url|https?://|postgres(ql)?://|pgpassword)") | not) and
        (test("[?&][A-Za-z0-9_.~-]+=") | not) and
        (contains("{") | not) and (contains("}") | not);
      map(
        if ((keys | sort) ==
              ["insertId","logName","severity","textPayload","timestamp"] and
            (.insertId | nonempty) and (.timestamp | nonempty) and
            .logName == $stdout_log and
            (.severity == "DEFAULT" or .severity == "INFO" or
             .severity == "NOTICE") and
            (.textPayload | safe_text))
        then {insertId,timestamp,severity,logName,textPayload}
        else error("unsafe or schema-drifted SQL stdout entry") end
      ) | sort_by([.timestamp,.insertId,.textPayload])' >"$output"
  }
  cloud_timeout 30 gcloud logging read "$log_filter" \
    --project="$PROJECT" --order=asc --limit=1000 \
    --format='json(insertId,timestamp,severity,logName,textPayload)' \
    | project_operation_log_read "$safe_temp/first.projected.json"
  sleep 30
  cloud_timeout 30 gcloud logging read "$log_filter" \
    --project="$PROJECT" --order=asc --limit=1000 \
    --format='json(insertId,timestamp,severity,logName,textPayload)' \
    | project_operation_log_read "$safe_temp/second.projected.json"
  for pass in first second; do
    count=$(jq 'length' "$safe_temp/$pass.projected.json")
    test "$count" -gt 0 && test "$count" -lt 1000
  done
  cmp -s "$safe_temp/first.projected.json" "$safe_temp/second.projected.json"
  install -m 600 "$safe_temp/first.projected.json" \
    "$execution_dir/logs-first.projected.json"
  install -m 600 "$safe_temp/second.projected.json" \
    "$execution_dir/logs-second.projected.json"
  jq -cS 'map({timestamp,severity,textPayload})' \
    "$safe_temp/second.projected.json" >"$safe_temp/sql-report.json"
  install -m 600 "$safe_temp/sql-report.json" "$execution_dir/sql-report.json"
)

write_unsafe_to_verify() {
  local reason=$1 marker
  case "$reason" in
    discovery_failed|discovery_ambiguous|execution_identity_invalid|\
    execution_describe_failed|execution_attestation_failed|\
    execution_nonterminal|immutable_inputs_changed) ;;
    *) return 64 ;;
  esac
  marker="$ARTIFACT_DIR/operation-unsafe-to-verify-$OP_EPOCH.txt"
  if test -n "${execution_dir:-}" && test -d "$execution_dir"; then
    marker="$execution_dir/unsafe-to-verify.txt"
  fi
  test ! -e "$marker"
  printf 'UNSAFE_TO_VERIFY: %s; no second execution was issued.\n' "$reason" \
    >"$marker"
}

run_sid_operation() (
  set -euo pipefail
  local operation=$1
  local job_args digest execution_name execution_id execution_uri execution_dir
  local job_before_projection job_after_projection
  local sql_before_dir sql_after_dir
  local execute_rc unknown_outcome terminal_outcome expected_args_json
  local candidates candidates_after
  local execution_attestation_with_command execution_command_sha256
  local safety_verify_rc log_evidence_rc
  local post_job_attest_rc post_sql_attest_rc
  local OP_T0 OP_EPOCH
  case "$operation" in
    verify|preseed)
      test "$#" -eq 1
      job_args="bcfy-calls-sid-operation,$operation"
      ;;
    activate)
      test "$#" -eq 2
      digest=$2
      printf '%s' "$digest" | grep -Eq '^[0-9a-f]{64}$'
      job_args="bcfy-calls-sid-operation,activate,19,$digest,CONFIRMED"
      ;;
    rollback_children)
      test "$#" -eq 1
      job_args="bcfy-calls-sid-operation,rollback_children,CONFIRMED"
      ;;
    *) return 64 ;;
  esac

  OP_EPOCH=$(date +%s)
  attest_operation_job "before-$operation-$OP_EPOCH"
  job_before_projection=$JOB_ATTEST_PROJECTION
  sql_before_dir="$ARTIFACT_DIR/operation-sql-before-$operation-$OP_EPOCH"
  attest_operation_sql "before-$operation-$OP_EPOCH"
  test -d "$sql_before_dir"
  unknown_outcome=0
  # This is the request-issued boundary, not the earlier attestation start.
  OP_T0=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  set +e
  execution_name=$(cloud_timeout 360 gcloud run jobs execute "$OP_JOB" \
    --project="$PROJECT" --region="$REGION" --wait --args="$job_args" \
    --format='value(metadata.name)')
  execute_rc=$?
  set -e

  if test "$execute_rc" -ne 0 || test -z "$execution_name"; then
    # A lost response is outcome-unknown, not permission to resubmit.
    unknown_outcome=1
    candidates="$ARTIFACT_DIR/operation-unknown-candidates-$OP_EPOCH.json"
    candidates_after="$ARTIFACT_DIR/operation-unknown-after-t0-$OP_EPOCH.json"
    if ! cloud_timeout 20 gcloud run jobs executions list --job="$OP_JOB" \
      --project="$PROJECT" --region="$REGION" --limit=100 \
      --format='json(metadata.name,metadata.creationTimestamp,metadata.labels)' \
    | jq -cS --arg expected_job "$OP_JOB" \
      --arg expected_job_uri "$EXPECTED_JOB_URI" '
        map(
          (.metadata.name // error("missing execution name")) as $name
          | (.metadata.creationTimestamp //
             error("missing execution creation timestamp")) as $created
          | {execution_name:$name, creation_timestamp:$created,
             canonical_uri:($expected_job_uri + "/executions/" + $name),
             parent_job_matches:
               (.metadata.labels["run.googleapis.com/job"]? == $expected_job)}
        )' >"$candidates"; then
      write_unsafe_to_verify discovery_failed
      return 75
    fi
    # A full bounded result could hide another candidate and is ambiguous.
    if ! jq -e --arg job_uri "$EXPECTED_JOB_URI" '
      length < 100 and all(.[];
        (.execution_name | test("^[a-z0-9-]+$")) and
        .canonical_uri ==
          ($job_uri + "/executions/" + .execution_name) and
        .parent_job_matches == true and
        (.creation_timestamp | type == "string"))' "$candidates"; then
      write_unsafe_to_verify discovery_ambiguous
      return 75
    fi
    if ! jq --arg t0 "$OP_T0" '
      def epoch:
        if type == "string" then
          (sub("\\.[0-9]+Z$"; "Z") | fromdateiso8601)
        else null end;
      ($t0 | epoch) as $cutoff
      | [.[]
         | .creation_timestamp as $created
         | select(($created | epoch) != null and
                  ($created | epoch) >= $cutoff)]' \
      "$candidates" >"$candidates_after"; then
      write_unsafe_to_verify discovery_failed
      return 75
    fi
    if ! test "$(jq 'length' "$candidates_after")" -eq 1; then
      write_unsafe_to_verify discovery_ambiguous
      return 75
    fi
    if ! execution_name=$(jq -er '.[0].execution_name' "$candidates_after"); then
      write_unsafe_to_verify discovery_ambiguous
      return 75
    fi
  fi
  execution_id=$execution_name
  case "$execution_id" in
    ''|*/*|*[!a-z0-9-]*)
      write_unsafe_to_verify execution_identity_invalid
      return 75
      ;;
  esac
  execution_uri="$EXPECTED_JOB_URI/executions/$execution_id"
  execution_dir="$ARTIFACT_DIR/operation-execution-$execution_id"
  test ! -e "$execution_dir"
  mkdir -m 700 "$execution_dir"
  printf '%s\n' "$execution_name" >"$execution_dir/response-name.txt"
  printf '%s\n' "$execution_uri" >"$execution_dir/canonical-uri.txt"
  expected_args_json=$(printf '%s' "$job_args" | jq -R 'split(",")')
  if ! execution_attestation_with_command=$(cloud_timeout 20 \
    gcloud run jobs executions describe "$execution_id" \
    --project="$PROJECT" --region="$REGION" --format=json \
  | jq -cS --arg expected_job_uri "$EXPECTED_JOB_URI" \
    --arg expected_execution_uri "$execution_uri" \
    --arg expected_job "$OP_JOB" \
    --arg expected_service_account "$EXPECTED_JOB_SA" \
    --arg expected_image "$EXPECTED_JOB_IMAGE" \
    --arg expected_db_host "$EXPECTED_DB_HOST" \
    --arg expected_db_name "$EXPECTED_DB_NAME" \
    --arg expected_secret "$EXPECTED_DB_SECRET" \
    --arg expected_secret_version "$EXPECTED_DB_SECRET_VERSION" \
    --arg expected_sql_bucket "$SQL_BUCKET" \
    --arg expected_network "$EXPECTED_NETWORK" \
    --arg expected_subnetwork "$EXPECTED_SUBNETWORK" \
    --argjson expected_args "$expected_args_json" '
    def declared_network_interfaces:
      [
        (.. | objects
          | .annotations?["run.googleapis.com/network-interfaces"]?
          | fromjson? | .[]?),
        (.. | objects
          | (.networkInterfaces? // .network_interfaces? // empty)
          | select(type == "array") | .[]?)
      ];
    def declared_network_interface_pairs:
      (declared_network_interfaces
        | map({network:(.network // null),
               subnetwork:(.subnetwork // null)})
        | unique);
    {
      expected_job_uri:$expected_job_uri,
      expected_execution_uri:$expected_execution_uri,
      execution_name:(.metadata.name // .name),
      job_binding_matches:
        ([.metadata.labels["run.googleapis.com/job"]? // empty]
          == [$expected_job]),
      image_matches:([..|objects|.image? // empty] == [$expected_image]),
      commands:[..|objects|.command? | select(type=="array")],
      service_account_matches:([..|objects|
        (.serviceAccount? // .serviceAccountName?) // empty]
        == [$expected_service_account]),
      argument_contract_matches:
        ([..|objects|.args? | select(type == "array")] == [$expected_args]),
      task_contract_matches:
        ([..|objects|(.taskCount? // .task_count?) // empty] == [1]),
      retry_contract_matches:
        ([..|objects|(.maxRetries? // .max_retries?) // empty] == [0]),
      timeout_contract_matches:(
        [..|objects|
          (.timeout? // .timeoutSeconds? // .timeout_seconds?) // empty | tostring]
        | (. == ["300s"] or . == ["300"])),
      db_host_matches:([..|objects|select(.name?=="DB_HOST")|.value? // empty]
        == [$expected_db_host]),
      db_port_matches:([..|objects|select(.name?=="DB_PORT")|.value? // empty]
        == ["5432"]),
      db_user_matches:([..|objects|select(.name?=="DB_USER")|.value? // empty]
        == ["postgres"]),
      db_name_matches:([..|objects|select(.name?=="DB_NAME")|.value? // empty]
        == [$expected_db_name]),
      env_names:([..|objects|
        select((.name? | type) == "string" and
               (has("value") or has("valueSource") or has("valueFrom")))|
        .name] | sort),
      password_ref_matches:([..|objects|select(.name?=="PGPASSWORD")|
        ((.valueSource.secretKeyRef? // .valueFrom.secretKeyRef?) // empty) |
        {secret:(.secret // .name // ""),
         version:((.version // .key // "")|tostring)}]
        == [{secret:$expected_secret,version:$expected_secret_version}]),
      plaintext_password_count:([..|objects|select(.name?=="PGPASSWORD")|
        .value? // empty] | length),
      gcs_volume_matches:([..|objects|
        if .gcs?.bucket? != null then
          {name:(.name // ""),bucket:.gcs.bucket,
           read_only:(.gcs.readOnly // .gcs.read_only // false)}
        elif (.csi?.driver? == "gcsfuse.run.googleapis.com" and
              .csi?.volumeAttributes?.bucketName? != null) then
          {name:(.name // ""),bucket:.csi.volumeAttributes.bucketName,
           read_only:(.csi.readOnly // .csi.read_only // false)}
        else empty end]
        == [{name:"sql-files",bucket:$expected_sql_bucket,read_only:true}]),
      volume_mount_matches:([..|objects|
        select((.mountPath? // .mount_path?) != null)|
        {name:(.name // ""),mount_path:(.mountPath // .mount_path)}]
        == [{name:"sql-files",mount_path:"/sql"}]),
      network_interface_matches:
        (declared_network_interface_pairs ==
          [{network:$expected_network,subnetwork:$expected_subnetwork}]),
      create_times:[.metadata.creationTimestamp? // empty],
      start_times:[.status.startTime? // empty],
      completed_conditions:[.status.conditions[]? | select(.type? == "Completed")|
        {state:(.state // null),status:(.status // null)}]
    }'); then
    write_unsafe_to_verify execution_describe_failed
    return 75
  fi
  if ! execution_command_sha256=$(printf '%s' \
    "$execution_attestation_with_command" \
    | jq -cS '.commands' | sha256sum | awk '{print $1}'); then
    write_unsafe_to_verify execution_attestation_failed
    return 75
  fi
  if ! test "$execution_command_sha256" = "$EXPECTED_JOB_COMMAND_SHA256"; then
    write_unsafe_to_verify execution_attestation_failed
    return 75
  fi
  if ! printf '%s' "$execution_attestation_with_command" \
    | jq -cS --arg command_sha256 "$execution_command_sha256" \
      'del(.commands) | . + {command_sha256:$command_sha256}' \
      >"$execution_dir/execution-projection.json"; then
    write_unsafe_to_verify execution_attestation_failed
    return 75
  fi
  if ! jq -e --arg execution "$execution_name" \
    --arg execution_uri "$execution_uri" \
    --arg job_uri "$EXPECTED_JOB_URI" \
    --arg command_sha256 "$EXPECTED_JOB_COMMAND_SHA256" --arg t0 "$OP_T0" '
    def epoch:
      if type == "string" then
        (sub("\\.[0-9]+Z$"; "Z") | fromdateiso8601)
      else null end;
    .expected_job_uri == $job_uri and
    .expected_execution_uri == $execution_uri and
    .execution_name == $execution and
    .job_binding_matches and .image_matches and
    .service_account_matches and .argument_contract_matches and
    .task_contract_matches and .retry_contract_matches and
    .timeout_contract_matches and .db_host_matches and
    .db_port_matches and .db_user_matches and .db_name_matches and
    .env_names == ["DB_HOST","DB_NAME","DB_PORT","DB_USER","PGPASSWORD"] and
    .password_ref_matches and .plaintext_password_count == 0 and
    .gcs_volume_matches and .volume_mount_matches and
    .network_interface_matches and
    .command_sha256 == $command_sha256 and
    any(.create_times[]; (. | epoch) >= ($t0 | epoch)) and
    any(.start_times[]; (. | epoch) >= ($t0 | epoch))' \
    "$execution_dir/execution-projection.json"; then
    write_unsafe_to_verify execution_attestation_failed
    return 75
  fi

  if ! terminal_outcome=$(jq -er '
      .completed_conditions as $completed
      | ([$completed[] | select(
        .state == "CONDITION_SUCCEEDED" or .status == "True")] | length) as $ok
      | ([$completed[] | select(
        .state == "CONDITION_FAILED" or .status == "False")] | length) as $failed
      | if ($completed | length) == 1 and $ok == 1 and $failed == 0
        then "success"
        elif ($completed | length) == 1 and $ok == 0 and $failed == 1
        then "failed"
        else empty end' "$execution_dir/execution-projection.json"); then
    printf '%s\n' 'Execution is not in a proved terminal condition; do not retry.' \
      >"$execution_dir/nonterminal-unknown.txt"
    write_unsafe_to_verify execution_nonterminal
    return 75
  fi

  set +e
  attest_operation_job "after-$operation-$OP_EPOCH"
  post_job_attest_rc=$?
  set -e
  if test "$post_job_attest_rc" -ne 0; then
    write_unsafe_to_verify immutable_inputs_changed
    return 75
  fi
  job_after_projection=$JOB_ATTEST_PROJECTION
  if ! cmp -s "$job_before_projection" "$job_after_projection"; then
    write_unsafe_to_verify immutable_inputs_changed
    return 75
  fi
  sql_after_dir="$ARTIFACT_DIR/operation-sql-after-$operation-$OP_EPOCH"
  set +e
  attest_operation_sql "after-$operation-$OP_EPOCH"
  post_sql_attest_rc=$?
  set -e
  if test "$post_sql_attest_rc" -ne 0 ||
     ! test -d "$sql_after_dir" ||
     ! cmp -s "$sql_before_dir/object-generations.json" \
       "$sql_after_dir/object-generations.json" ||
     ! cmp -s "$sql_before_dir/content-sha256.json" \
       "$sql_after_dir/content-sha256.json"; then
    write_unsafe_to_verify immutable_inputs_changed
    return 75
  fi
  sha256sum "$job_before_projection" "$job_after_projection" \
    "$sql_before_dir/object-generations.json" \
    "$sql_after_dir/object-generations.json" \
    "$sql_before_dir/content-sha256.json" "$sql_after_dir/content-sha256.json" \
    >"$execution_dir/job-sql-before-after.sha256"

  safety_verify_rc=0
  printf '%s\n' 'NOT_REQUIRED: execution is not a failed/unknown mutation.' \
    >"$execution_dir/read-only-safety-verify.txt"
  printf '%s\n' "$terminal_outcome" >"$execution_dir/terminal-outcome.txt"
  if test "$operation" != verify &&
     { test "$terminal_outcome" = failed || test "$unknown_outcome" -eq 1; }; then
    # Safety verification precedes optional Logging evidence. A Logging outage
    # must never suppress the one distinct read-only state check.
    set +e
    run_sid_operation verify
    safety_verify_rc=$?
    set -e
    if test "$safety_verify_rc" -eq 0; then
      printf '%s\n' 'PASS: distinct read-only safety verification completed.' \
        >"$execution_dir/read-only-safety-verify.txt"
    else
      printf 'FAIL: distinct read-only safety verification returned %s.\n' \
        "$safety_verify_rc" >"$execution_dir/read-only-safety-verify.txt"
    fi
  fi

  log_evidence_rc=0
  set +e
  collect_operation_log_evidence "$execution_dir" "$execution_id"
  log_evidence_rc=$?
  set -e
  if test "$log_evidence_rc" -ne 0; then
    # Capture the failure independently; this execution remains unusable as
    # acceptance evidence even when its safety verification already ran.
    printf 'FAIL: bounded log evidence returned %s.\n' "$log_evidence_rc" \
      >"$execution_dir/log-evidence-failure.txt"
  fi
  if test "$log_evidence_rc" -eq 0; then
    sha256sum "$execution_dir/response-name.txt" \
      "$execution_dir/canonical-uri.txt" \
      "$execution_dir/execution-projection.json" \
      "$execution_dir/job-sql-before-after.sha256" \
      "$execution_dir/read-only-safety-verify.txt" \
      "$execution_dir/terminal-outcome.txt" \
      "$execution_dir/logs-first.projected.json" \
      "$execution_dir/logs-second.projected.json" \
      "$execution_dir/sql-report.json" >"$execution_dir/execution.sha256"
  else
    sha256sum "$execution_dir/response-name.txt" \
      "$execution_dir/canonical-uri.txt" \
      "$execution_dir/execution-projection.json" \
      "$execution_dir/job-sql-before-after.sha256" \
      "$execution_dir/read-only-safety-verify.txt" \
      "$execution_dir/terminal-outcome.txt" \
      "$execution_dir/log-evidence-failure.txt" >"$execution_dir/execution.sha256"
  fi

  if test "$terminal_outcome" = failed; then
    printf '%s\n' \
      'Bound execution failed terminally; state advance and mutation retry are forbidden.' \
      >"$execution_dir/terminal-failure.txt"
    return 76
  fi
  if test "$unknown_outcome" -eq 1; then
    printf '%s\n' \
      'Execution response was outcome-unknown; mutation resubmission is forbidden.' \
      >"$execution_dir/outcome-unknown.txt"
    return 75
  fi
  test "$safety_verify_rc" -eq 0
  test "$log_evidence_rc" -eq 0 || return 77
)

run_sid_operation verify
# Forward preparation only: run_sid_operation preseed
# After fresh NO_AUTHORITY proof only: run_sid_operation activate "$MANIFEST_DIGEST"
# After fresh ROLLBACK_NO_AUTHORITY proof only: run_sid_operation rollback_children
```

The SQL transactions bound locks to 5 seconds and statements to 30 seconds.
After a mutation is safely bound to one proved-terminal execution and the
before/after immutable inputs match, a terminal failure or a lost execute
response automatically triggers one distinct read-only verify execution
**before** optional Logging evidence. The wrapper then returns nonzero to stop
the state machine; it never resubmits the mutation. If discovery is missing or
ambiguous, identity/attestation is untrusted, the execution is nonterminal, or
the immutable postflight differs, the wrapper records `UNSAFE_TO_VERIFY` and
issues no second execution. A Logging outage or unstable/limited result is
recorded separately, cannot suppress an eligible safety verify, and makes an
otherwise successful execution unusable for state advance. The attested SQL is
001_preseed.sql, 002_activate.sql, 003_rollback_children.sql, and 004_verify.sql.
Do not delete Lease identity. Do not reset Lease identity. Do not reduce any
Lease or child fencing token.

## Shared primitive E: exact same-slot start and local authority proof

Use this primitive only after a freshly reviewed process-absence artifact. It
removes only the Phase 7 drop-in, restores the captured normal
Restart=always/enabled state, starts indices 1 and 2 on the same numeric VMs,
and then reuses the complete inventory to prove local authority. It never
targets a replacement or expands a service-name pattern.

```bash
assert_process_started() {
  proof=$1
  expected_mode=$2
  expected_image=$3
  jq -e --argjson expected "$EXPECTED_SLOT_COUNT" --arg mode "$expected_mode" \
    --arg image "$expected_image" --arg service "$SERVICE" '
    length == $expected and all(.[];
      .load_state == "loaded" and .active_state == "active" and
      .sub_state == "running" and .main_pid > 0 and .control_pid == 0 and
      .unit == ($service + "@" + (.worker_index|tostring) + ".service") and
      .container_name == ($service + "-" + (.worker_index|tostring)) and
      .control_group_process_count > 0 and .container_exists == true and
      .configured_image == $image and
      (.image_id | test("^sha256:[0-9a-f]{64}$")) and
      (.alloydb_host_sha256 | test("^[0-9a-f]{64}$")) and
      .profile_count == 1 and .worker_index_count == 1 and
      (($mode == "sid_lease" and
        .sid_mode_count == 1 and .legacy_mode_count == 0) or
       ($mode == "legacy_feed" and
        .legacy_mode_count == 1 and .sid_mode_count == 0)) and
      .ingestion_authority_container_count == 2 and
      .unknown_ingestion_authority_container_count == 0 and
      .ingestion_host_process_count == 2 and .current_action == "NONE")' "$proof"
}

start_same_frozen_slots() {
  expected_mode=$1
  label=$2
  stopped_proof=$3
  case "$expected_mode" in sid_lease|legacy_feed) ;; *) return 64 ;; esac
  case "$label" in *[!A-Za-z0-9._-]*|'') return 64 ;; esac
  assert_process_absent "$stopped_proof"
  expected_image=$(jq -er '.image_digest' "$SIGNED_INPUTS")

  start_members="$ARTIFACT_DIR/start-members-$label.json"
  cloud_timeout 20 gcloud compute instance-groups managed list-instances "$MIG" \
    --project="$PROJECT" --region="$REGION" --format=json \
  | jq -cS 'map({
      instance_name:(.instance | split("/")[-1]),
      zone:(.instance | split("/")[-3]),
      current_action:.currentAction,
      instance_template:((.version.instanceTemplate // "") | split("/")[-1])
    }) | sort_by(.instance_name)' >"$start_members"
  test "$(jq -c 'sort_by(.instance_name)' "$start_members")" = \
    "$(jq -c '[.[] | {instance_name,zone,current_action,instance_template}]
      | sort_by(.instance_name)' "$ARTIFACT_DIR/frozen-instances.json")"
  jq -e 'all(.[]; .current_action == "NONE")' "$start_members"

  start_pids=
  failed=0
  start_started_epoch=$(date +%s)
  printf '%s\n' "$start_started_epoch" \
    >"$ARTIFACT_DIR/start-started-epoch-$label.txt"
  while IFS=$'\t' read -r name zone expected_id; do
    (
      live_id=$(cloud_timeout 20 gcloud compute instances describe "$name" \
        --project="$PROJECT" --zone="$zone" --format='value(id)')
      test "$live_id" = "$expected_id"
      cloud_timeout 90 gcloud compute ssh "$name" \
        --project="$PROJECT" --zone="$zone" --tunnel-through-iap \
        --ssh-flag='-o ConnectTimeout=10' \
        --ssh-flag='-o ConnectionAttempts=1' --command="set -eu
          for index in 1 2; do
            unit='$SERVICE@'\$index'.service'
            dropin='/run/systemd/system/'\$unit'.d/phase7-no-restart.conf'
            sudo test -f \"\$dropin\"
            test \"\$(systemctl is-enabled \"\$unit\" 2>/dev/null || true)\" = disabled
            sudo rm -- \"\$dropin\"
          done
          sudo systemctl daemon-reload
          for index in 1 2; do
            unit='$SERVICE@'\$index'.service'
            test \"\$(systemctl show \"\$unit\" -p Restart --value)\" = always
            sudo systemctl enable \"\$unit\" >/dev/null
            sudo systemctl start \"\$unit\"
            test \"\$(systemctl is-enabled \"\$unit\")\" = enabled
          done"
    ) &
    start_pids="$start_pids $!"
  done < <(jq -r '.[] | [.instance_name,.zone,.numeric_instance_id] | @tsv' \
    "$ARTIFACT_DIR/frozen-instances.json")
  for pid in $start_pids; do
    if ! wait "$pid"; then failed=1; fi
  done
  test "$failed" -eq 0
  last_start_completed_epoch=$(date +%s)
  test "$last_start_completed_epoch" -ge "$start_started_epoch"
  printf '%s\n' "$last_start_completed_epoch" \
    >"$ARTIFACT_DIR/last-start-completed-epoch-$label.txt"

  collect_process_proof "start-$label" 30
  assert_process_started "$PROCESS_PROOF_JSON" "$expected_mode" "$expected_image"
  cp "$PROCESS_PROOF_JSON" "$ARTIFACT_DIR/start-proof-$label.json"
  sha256sum "$ARTIFACT_DIR/start-members-$label.json" \
    "$ARTIFACT_DIR/start-started-epoch-$label.txt" \
    "$ARTIFACT_DIR/last-start-completed-epoch-$label.txt" \
    "$ARTIFACT_DIR/start-proof-$label.json" \
    >"$ARTIFACT_DIR/start-proof-$label.sha256"
}
```

If start, SSH, local proof, mode/profile/image, or identity validation fails,
some same-authority process may exist. Keep the opposite authority stopped and
run the bounded stop controller again before any inverse transition. Startup
telemetry is additional evidence; it does not replace this local proof.

## Shared primitive E1: live replacement-template/runtime agreement

Run this only while maintenance is still true, after the saved-plan apply and
its stable control capture. It dereferences the one regional target template,
but provider metadata and the plaintext database endpoint exist only in the
pipe to a process-local validator. Retained evidence contains the template
identity, non-secret mode/profile/image, and endpoint SHA-256. The same hash
must appear in every active container, and the reconciliation must not have
changed any running PID, image ID, or configured image.

```bash
capture_live_template_runtime_agreement() (
  set -euo pipefail
  label=$1
  mode=$2
  baseline_proof=$3
  controls_dir=$4
  tuple_dir=$5
  case "$label" in *[!A-Za-z0-9._-]*|'') return 64 ;; esac
  case "$mode" in sid_lease|legacy_feed) ;; *) return 64 ;; esac
  test -f "$baseline_proof"
  test -f "$controls_dir/mig.json"
  test -f "$tuple_dir/plan-pin-projection.json"
  out="$ARTIFACT_DIR/template-runtime-$label"
  test ! -e "$out"
  mkdir -m 700 "$out"
  temp=$(mktemp -d)
  chmod 700 "$temp"
  trap 'rm -rf -- "$temp"' EXIT

  template_uri=$(jq -er '
    ([.versions[]?.instanceTemplate | select(type == "string" and length > 0)] +
     [.instanceTemplate | select(type == "string" and length > 0)])
    | unique
    | if length == 1 then .[0]
      else error("expected exactly one target instance template") end' \
    "$controls_dir/mig.json")
  template_prefix="https://www.googleapis.com/compute/v1/projects/$PROJECT/global/instanceTemplates/"
  case "$template_uri" in
    "$template_prefix"*) ;;
    *) return 65 ;;
  esac
  template_name=${template_uri#"$template_prefix"}
  case "$template_name" in *[!A-Za-z0-9._-]*|'') return 65 ;; esac

  cloud_timeout 30 gcloud compute instance-templates describe "$template_name" \
    --project="$PROJECT" --global --format=json \
  | EXPECTED_TEMPLATE_URI="$template_uri" \
    EXPECTED_TEMPLATE_NAME="$template_name" EXPECTED_MODE="$mode" \
    EXPECTED_IMAGE="$CANDIDATE_IMAGE" EXPECTED_SERVICE="$SERVICE" \
    python3 -c '
import hashlib
import json
import os
import re
import sys

source = json.load(sys.stdin)
expected_uri = os.environ["EXPECTED_TEMPLATE_URI"]
expected_name = os.environ["EXPECTED_TEMPLATE_NAME"]
expected_mode = os.environ["EXPECTED_MODE"]
expected_image = os.environ["EXPECTED_IMAGE"]
service = os.environ["EXPECTED_SERVICE"]
if source.get("selfLink") != expected_uri or source.get("name") != expected_name:
    raise SystemExit("invalid template identity")
items = source.get("properties", {}).get("metadata", {}).get("items", [])
if not isinstance(items, list):
    raise SystemExit("invalid template metadata")
matches = [
    item.get("value")
    for item in items
    if isinstance(item, dict) and item.get("key") == "user-data"
]
if len(matches) != 1 or not isinstance(matches[0], str):
    raise SystemExit("invalid template metadata")
user_data = matches[0]
if len(user_data.encode()) > 2_000_000:
    raise SystemExit("invalid template metadata")

def block(path):
    lines = user_data.splitlines()
    marker = f"- path: {path}"
    positions = [i for i, line in enumerate(lines) if line == marker]
    if len(positions) != 1:
        raise SystemExit("invalid template metadata")
    start = positions[0]
    end = next(
        (i for i in range(start + 1, len(lines)) if lines[i].startswith("- path: ")),
        len(lines),
    )
    return lines[start:end]

env_block = block(f"/etc/container-env/{service}.env")
try:
    content_index = env_block.index("  content: |")
except ValueError as error:
    raise SystemExit("invalid template metadata") from error
env_lines = []
for line in env_block[content_index + 1 :]:
    if not line.startswith("    "):
        raise SystemExit("invalid template metadata")
    env_lines.append(line[4:])

def exact_env(name):
    prefix = name + "="
    values = [line[len(prefix) :] for line in env_lines if line.startswith(prefix)]
    if len(values) != 1 or not values[0]:
        raise SystemExit("invalid template environment")
    return values[0]

profile = exact_env("WORKER_PROFILE")
mode = exact_env("BCFY_CALLS_AUTHORITY_MODE")
endpoint = exact_env("ALLOYDB_HOST")
if profile != "mixed-dormant" or mode != expected_mode:
    raise SystemExit("invalid template authority")
worker_block = "\n".join(block(f"/etc/systemd/system/{service}@.service"))
digest_images = set(
    re.findall(r"[A-Za-z0-9._:/-]+@sha256:[0-9a-f]{64}", worker_block)
)
if digest_images != {expected_image} or worker_block.count(expected_image) < 2:
    raise SystemExit("invalid template image")
projection = {
    "schema_version": 1,
    "template_uri": expected_uri,
    "template_name": expected_name,
    "profile": profile,
    "mode": mode,
    "image": expected_image,
    "alloydb_host_sha256": hashlib.sha256(endpoint.encode()).hexdigest(),
}
json.dump(projection, sys.stdout, separators=(",", ":"), sort_keys=True)
sys.stdout.write("\n")
' >"$temp/template-projection.json"

  jq -e --arg mode "$mode" --arg image "$CANDIDATE_IMAGE" \
    --arg uri "$template_uri" '
    keys == ["alloydb_host_sha256","image","mode","profile","schema_version",
             "template_name","template_uri"] and
    .schema_version == 1 and .template_uri == $uri and
    .profile == "mixed-dormant" and .mode == $mode and .image == $image and
    (.alloydb_host_sha256 | test("^[0-9a-f]{64}$"))' \
    "$temp/template-projection.json"
  template_endpoint_sha=$(jq -er '.alloydb_host_sha256' \
    "$temp/template-projection.json")
  plan_endpoint_sha=$(jq -er '
    .cutover_contract.planned_active_alloydb_primary_instance_ip_sha256
    | select(type == "string" and test("^[0-9a-f]{64}$"))' \
    "$tuple_dir/plan-pin-projection.json")
  test "$template_endpoint_sha" = "$plan_endpoint_sha"

  collect_process_proof "runtime-$label" 30 target-may-change
  assert_process_started "$PROCESS_PROOF_JSON" "$mode" "$CANDIDATE_IMAGE"
  jq -e --arg endpoint_sha "$template_endpoint_sha" '
    length > 0 and all(.[]; .alloydb_host_sha256 == $endpoint_sha)' \
    "$PROCESS_PROOF_JSON"
  for side in baseline current; do
    source=$baseline_proof
    if test "$side" = current; then source=$PROCESS_PROOF_JSON; fi
    jq -cS 'map({
      expected_numeric_instance_id,live_numeric_instance_id,worker_index,
      main_pid,configured_image,image_id,alloydb_host_sha256
    }) | sort_by(.expected_numeric_instance_id,.worker_index)' \
      "$source" >"$temp/$side-runtime-identity.json"
  done
  cmp -s "$temp/baseline-runtime-identity.json" \
    "$temp/current-runtime-identity.json"

  install -m 600 "$temp/template-projection.json" \
    "$out/template-projection.json"
  install -m 600 "$temp/current-runtime-identity.json" \
    "$out/runtime-identity.json"
  install -m 600 "$PROCESS_PROOF_JSON" "$out/process-proof.json"
  sha256sum "$out"/*.json >"$out/agreement.sha256"
)
```

## Shared primitive F: complete bootstrap and soak evidence

The signed input manifest contains the exact sorted 19-SID projection from the
approved SQL manifest. The preflight binds that array and its manifest digest;
the collector below re-derives and validates it rather than accepting a caller
override. It also binds the reviewed startup contract: both authority-specific
profile digests, selected domains, pacing bounds, and Feed admission settings.
The startup collector requires exactly one completed startup event for every
frozen slot, binds each event to the local start proof's numeric instance ID,
and worker index, retains the container-local runtime PID and hostname as
evidence, and rejects any payload-key drift. Host systemd MainPID and container
PID namespace values are deliberately not equated. A poll read is scoped to
the same frozen IDs, a closed `bootstrap|soak` kind, internally selected event
types, and a half-open UTC window. `RESULT_LIMIT=10000` exceeds the expected
3,600 steady polls but remains fail closed.

```bash
collect_startup_completion_evidence() (
  set -euo pipefail
  MODE=$1
  LABEL=$2
  WINDOW_START=$3
  WINDOW_END=$4
  START_PROOF=$5
  case "$MODE" in sid_lease|legacy_feed) ;; *) return 64 ;; esac
  case "$LABEL" in *[!A-Za-z0-9._-]*|'') return 64 ;; esac
  declare -F cloud_timeout >/dev/null
  declare -F wait_until_epoch >/dev/null
  declare -F assert_process_started >/dev/null
  : "${SIGNED_INPUTS:?signed manifest is required}"
  : "${ARTIFACT_DIR:?external evidence directory is required}"
  : "${EXPECTED_SLOT_COUNT:?complete frozen slot count is required}"

  START_EPOCH=$(date -u -d "$WINDOW_START" +%s)
  END_EPOCH=$(date -u -d "$WINDOW_END" +%s)
  test "$START_EPOCH" -lt "$END_EPOCH"
  EXPECTED_IMAGE=$(jq -er '.image_digest' "$SIGNED_INPUTS")
  EXPECTED_CONTRACT=$(jq -ce '.startup_contract' "$SIGNED_INPUTS")
  EXPECTED_PROFILE_DIGEST=$(jq -er --arg mode "$MODE" \
    '.startup_contract.profile_digests[$mode]
     | select(type == "string" and test("^[0-9a-f]{64}$"))' \
    "$SIGNED_INPUTS")
  assert_process_started "$START_PROOF" "$MODE" "$EXPECTED_IMAGE"
  test "$(jq 'length' "$START_PROOF")" -eq "$EXPECTED_SLOT_COUNT"
  START_PROOF_SHA256=$(sha256sum "$START_PROOF" | awk '{print $1}')

  STARTUP_RESULT_LIMIT=$((EXPECTED_SLOT_COUNT + 1))
  STARTUP_INGESTION_GRACE_SECONDS=120
  STARTUP_STABILITY_GAP_SECONDS=30
  FIRST="$ARTIFACT_DIR/startup-complete-$LABEL.first.projected.json"
  SECOND="$ARTIFACT_DIR/startup-complete-$LABEL.second.projected.json"
  CANONICAL="$ARTIFACT_DIR/startup-complete-$LABEL.canonical.json"
  IDENTITIES="$ARTIFACT_DIR/startup-complete-$LABEL.identities.json"
  ENVELOPE="$ARTIFACT_DIR/startup-complete-$LABEL.envelope.json"
  HASHES="$ARTIFACT_DIR/startup-complete-$LABEL.sha256"
  for retained in "$FIRST" "$SECOND" "$CANONICAL" "$IDENTITIES" \
    "$ENVELOPE" "$HASHES"; do
    test ! -e "$retained"
  done

  INSTANCE_FILTER=$(jq -r '
    map(.numeric_instance_id) | sort |
    map("resource.labels.instance_id=\"" + . + "\"") | join(" OR ")' \
    "$ARTIFACT_DIR/frozen-instances.json")
  test -n "$INSTANCE_FILTER"
  test "$INSTANCE_FILTER" = "$(jq -r '
    map(.numeric_instance_id) | sort | unique |
    map("resource.labels.instance_id=\"" + . + "\"") | join(" OR ")' \
    "$ARTIFACT_DIR/frozen-instances.json")"
  case "$INSTANCE_FILTER" in *'\\"'*) return 65 ;; esac
  STARTUP_FILTER='resource.type="gce_instance"
AND jsonPayload.event_type="startup_pacing_complete"
AND timestamp>="'"$WINDOW_START"'"
AND timestamp<"'"$WINDOW_END"'"
AND ('"$INSTANCE_FILTER"')'

  COLLECTION_NOT_BEFORE=$((END_EPOCH + STARTUP_INGESTION_GRACE_SECONDS))
  wait_until_epoch "$COLLECTION_NOT_BEFORE"
  SAFE_TEMP_DIR=$(mktemp -d)
  chmod 700 "$SAFE_TEMP_DIR"
  trap 'rm -rf -- "$SAFE_TEMP_DIR"' EXIT

  project_startup_read() {
    local output=$1
    jq -cS --arg mode "$MODE" --arg digest "$EXPECTED_PROFILE_DIGEST" \
      --argjson contract "$EXPECTED_CONTRACT" --slurpfile proof "$START_PROOF" '
      def nonempty_string: type == "string" and length > 0;
      def nonnegative_number: type == "number" and . >= 0;
      def expected_payload_keys: [
        "bcfy_calls_authority_mode", "deterministic_delay_sec", "event_type",
        "hostname", "lease_admission_cycle_budget", "max_feeds_per_worker",
        "message", "process_id", "profile", "profile_digest",
        "random_delay_sec", "selected_domains", "startup_jitter_max_sec",
        "startup_stagger_max_sec", "total_delay_sec", "worker_id",
        "worker_index"
      ];
      map(
        . as $entry
        | (.jsonPayload // null) as $payload
        | ($proof[0] | map(select(
            .expected_numeric_instance_id == .live_numeric_instance_id and
            (.expected_numeric_instance_id | tostring) ==
              ($entry.resource.labels.instance_id | tostring) and
            .worker_index == $payload.worker_index))) as $slots
        | ($slots[0] // null) as $slot
        | if (($entry.insertId | nonempty_string) and
              ($entry.timestamp | nonempty_string) and
              ($entry.receiveTimestamp | nonempty_string) and
              ($entry.logName | nonempty_string) and
              $entry.resource.type == "gce_instance" and
              ($entry.resource.labels.instance_id | nonempty_string) and
              ($payload | type) == "object" and
              ($payload | keys | sort) == (expected_payload_keys | sort) and
              $payload.message == "Startup pacing complete" and
              $payload.event_type == "startup_pacing_complete" and
              ($payload.worker_id | type == "string" and
                test("^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")) and
              ($payload.worker_index | type == "number" and
                . == floor and (. == 1 or . == 2)) and
              ($payload.process_id | type == "number" and
                . == floor and . > 0) and
              ($payload.hostname | nonempty_string) and
              ($slots | length) == 1 and
              $payload.profile == $contract.profile and
              $payload.profile_digest == $digest and
              $payload.bcfy_calls_authority_mode == $mode and
              $payload.selected_domains == $contract.selected_domains and
              $payload.startup_stagger_max_sec ==
                $contract.startup_stagger_max_sec and
              $payload.startup_jitter_max_sec ==
                $contract.startup_jitter_max_sec and
              $payload.lease_admission_cycle_budget ==
                $contract.lease_admission_cycle_budget and
              $payload.max_feeds_per_worker ==
                $contract.max_feeds_per_worker and
              ($payload.deterministic_delay_sec | nonnegative_number) and
              ($payload.random_delay_sec | nonnegative_number) and
              ($payload.total_delay_sec | nonnegative_number) and
              $payload.deterministic_delay_sec <=
                $payload.startup_stagger_max_sec and
              $payload.random_delay_sec <= $payload.startup_jitter_max_sec and
              $payload.total_delay_sec ==
                ($payload.deterministic_delay_sec +
                 $payload.random_delay_sec))
          then {
            insertId:$entry.insertId,
            timestamp:$entry.timestamp,
            receiveTimestamp:$entry.receiveTimestamp,
            logName:$entry.logName,
            resource:{type:"gce_instance",labels:{
              instance_id:$entry.resource.labels.instance_id}},
            jsonPayload:($payload | del(.message))
          }
          else error("unsafe, unbound, or schema-drifted startup entry")
          end
      )' >"$output"
  }

  cloud_timeout 60 gcloud logging read "$STARTUP_FILTER" \
    --project="$PROJECT" --order=asc --limit="$STARTUP_RESULT_LIMIT" \
    --format=json | project_startup_read "$SAFE_TEMP_DIR/first.json"
  sleep "$STARTUP_STABILITY_GAP_SECONDS"
  cloud_timeout 60 gcloud logging read "$STARTUP_FILTER" \
    --project="$PROJECT" --order=asc --limit="$STARTUP_RESULT_LIMIT" \
    --format=json | project_startup_read "$SAFE_TEMP_DIR/second.json"

  for pass in first second; do
    jq -e --argjson expected "$EXPECTED_SLOT_COUNT" \
      'length == $expected' "$SAFE_TEMP_DIR/$pass.json" >/dev/null
    jq -cS 'sort_by([.resource.labels.instance_id,
      .jsonPayload.worker_index, .timestamp, .insertId])' \
      "$SAFE_TEMP_DIR/$pass.json" >"$SAFE_TEMP_DIR/$pass.canonical.json"
    jq -cS '[.[] | .logName + ":" + .insertId] | sort' \
      "$SAFE_TEMP_DIR/$pass.canonical.json" \
      >"$SAFE_TEMP_DIR/$pass.identities.json"
    jq -e 'length == (unique | length)' \
      "$SAFE_TEMP_DIR/$pass.identities.json" >/dev/null
  done
  EXPECTED_SLOT_KEYS=$(jq -c '[.[] |
    .expected_numeric_instance_id + ":" + (.worker_index | tostring)] | sort' \
    "$START_PROOF")
  OBSERVED_SLOT_KEYS=$(jq -c '[.[] |
    .resource.labels.instance_id + ":" +
      (.jsonPayload.worker_index | tostring)] | sort' \
    "$SAFE_TEMP_DIR/second.canonical.json")
  test "$OBSERVED_SLOT_KEYS" = "$EXPECTED_SLOT_KEYS"
  jq -e --argjson expected "$EXPECTED_SLOT_COUNT" '
    ([.[].jsonPayload.worker_id] | unique | length) == $expected' \
    "$SAFE_TEMP_DIR/second.canonical.json" >/dev/null
  cmp -s "$SAFE_TEMP_DIR/first.canonical.json" \
    "$SAFE_TEMP_DIR/second.canonical.json"
  cmp -s "$SAFE_TEMP_DIR/first.identities.json" \
    "$SAFE_TEMP_DIR/second.identities.json"

  FIRST_SHA256=$(sha256sum "$SAFE_TEMP_DIR/first.canonical.json" \
    | awk '{print $1}')
  SECOND_SHA256=$(sha256sum "$SAFE_TEMP_DIR/second.canonical.json" \
    | awk '{print $1}')
  IDENTITY_SHA256=$(sha256sum "$SAFE_TEMP_DIR/second.identities.json" \
    | awk '{print $1}')
  MIN_RECEIVE_TIMESTAMP=$(jq -er '[.[].receiveTimestamp] | min' \
    "$SAFE_TEMP_DIR/second.canonical.json")
  MAX_RECEIVE_TIMESTAMP=$(jq -er '[.[].receiveTimestamp] | max' \
    "$SAFE_TEMP_DIR/second.canonical.json")
  COLLECTION_COMPLETED_AT=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  test "$(date -u -d "$MIN_RECEIVE_TIMESTAMP" +%s)" -le \
    "$(date -u -d "$MAX_RECEIVE_TIMESTAMP" +%s)"
  test "$(date -u -d "$MAX_RECEIVE_TIMESTAMP" +%s)" -le \
    "$(date -u -d "$COLLECTION_COMPLETED_AT" +%s)"

  jq -ncS --arg mode "$MODE" --arg label "$LABEL" \
    --arg window_start "$WINDOW_START" --arg window_end "$WINDOW_END" \
    --argjson expected_count "$EXPECTED_SLOT_COUNT" \
    --argjson expected_contract "$EXPECTED_CONTRACT" \
    --arg first_sha256 "$FIRST_SHA256" --arg second_sha256 "$SECOND_SHA256" \
    --arg identity_sha256 "$IDENTITY_SHA256" \
    --arg start_proof_sha256 "$START_PROOF_SHA256" \
    --arg min_receive_timestamp "$MIN_RECEIVE_TIMESTAMP" \
    --arg max_receive_timestamp "$MAX_RECEIVE_TIMESTAMP" \
    --arg collection_completed_at "$COLLECTION_COMPLETED_AT" \
    --argjson grace_seconds "$STARTUP_INGESTION_GRACE_SECONDS" \
    --argjson stability_gap_seconds "$STARTUP_STABILITY_GAP_SECONDS" '
    {schema_version:1,event_type:"startup_pacing_complete",mode:$mode,
     label:$label,window_start:$window_start,window_end:$window_end,
     expected_slot_count:$expected_count,returned_count:$expected_count,
     expected_contract:$expected_contract,collection_complete:true,
     ingestion_grace_seconds:$grace_seconds,
     stability_gap_seconds:$stability_gap_seconds,stability_read_count:2,
     first_canonical_sha256:$first_sha256,
     second_canonical_sha256:$second_sha256,
     identity_sha256:$identity_sha256,
     start_proof_sha256:$start_proof_sha256,
     min_receive_timestamp:$min_receive_timestamp,
     max_receive_timestamp:$max_receive_timestamp,
     collection_completed_at:$collection_completed_at}' \
    >"$SAFE_TEMP_DIR/envelope.json"

  install -m 600 "$SAFE_TEMP_DIR/first.json" "$FIRST"
  install -m 600 "$SAFE_TEMP_DIR/second.json" "$SECOND"
  install -m 600 "$SAFE_TEMP_DIR/second.canonical.json" "$CANONICAL"
  install -m 600 "$SAFE_TEMP_DIR/second.identities.json" "$IDENTITIES"
  install -m 600 "$SAFE_TEMP_DIR/envelope.json" "$ENVELOPE"
  sha256sum "$FIRST" "$SECOND" "$CANONICAL" "$IDENTITIES" "$ENVELOPE" \
    "$START_PROOF" >"$HASHES"
)

collect_complete_log_window() (
  set -euo pipefail
  KIND=$1
  WINDOW_START=$2
  WINDOW_END=$3
  case "$KIND" in
    bootstrap)
      EVENT_FILTER='jsonPayload.event_type="bcfy_calls_sid_poll"'
      EVENT_TYPES_JSON='["bcfy_calls_sid_poll"]'
      ;;
    soak)
      EVENT_FILTER='(jsonPayload.event_type="bcfy_calls_replay_window_truncated" OR jsonPayload.event_type="bcfy_calls_sid_poll")'
      EVENT_TYPES_JSON='["bcfy_calls_replay_window_truncated","bcfy_calls_sid_poll"]'
      ;;
    *) return 64 ;;
  esac
  declare -F cloud_timeout >/dev/null
  declare -F wait_until_epoch >/dev/null
  : "${SIGNED_INPUTS:?signed manifest is required}"
  : "${ARTIFACT_DIR:?external evidence directory is required}"

  START_EPOCH=$(date -u -d "$WINDOW_START" +%s)
  END_EPOCH=$(date -u -d "$WINDOW_END" +%s)
  test "$START_EPOCH" -lt "$END_EPOCH"
  EXPECTED_SIDS=$(jq -ce '.expected_sids | select(
    type == "array" and length == 19 and . == (sort | unique) and
    all(.[]; type == "string" and test("^[0-9]+$")))' "$SIGNED_INPUTS")

  RESULT_LIMIT=10000
  LOG_INGESTION_GRACE_SECONDS=120
  LOG_STABILITY_GAP_SECONDS=30
  PROJECTED_EVIDENCE="$ARTIFACT_DIR/$KIND-events.canonical.json"
  EVIDENCE_ENVELOPE="$ARTIFACT_DIR/$KIND-envelope.json"
  SAFE_FIRST="$ARTIFACT_DIR/$KIND-events.first.projected.json"
  SAFE_SECOND="$ARTIFACT_DIR/$KIND-events.second.projected.json"
  CANONICAL_FIRST="$ARTIFACT_DIR/$KIND-events.first.canonical.json"
  CANONICAL_SECOND="$ARTIFACT_DIR/$KIND-events.second.canonical.json"
  IDENTITIES_FIRST="$ARTIFACT_DIR/$KIND-events.first.identities.json"
  IDENTITIES_SECOND="$ARTIFACT_DIR/$KIND-events.second.identities.json"
  for retained in "$PROJECTED_EVIDENCE" "$PROJECTED_EVIDENCE.sha256" \
    "$EVIDENCE_ENVELOPE" "$SAFE_FIRST" "$SAFE_SECOND" \
    "$CANONICAL_FIRST" "$CANONICAL_SECOND" \
    "$IDENTITIES_FIRST" "$IDENTITIES_SECOND"; do
    test ! -e "$retained"
  done

  SLO_FIELDS=$(EXPECTED_SLO_CONTRACT_PATH="$PUBLIC_REPO_ROOT/backend/pipeline/ingestion/slo_contract.py" \
    PYTHONPATH="$PUBLIC_REPO_ROOT" python3 -P -c '
import json
import os
import pathlib

from backend.pipeline.ingestion import slo_contract as contract

expected = pathlib.Path(os.environ["EXPECTED_SLO_CONTRACT_PATH"]).resolve()
actual = pathlib.Path(contract.__file__).resolve()
if actual != expected:
    raise SystemExit("unbound SLO contract module")
print(json.dumps({
    "poll": sorted(
        contract.BCFY_CALLS_SID_POLL_REQUIRED_FIELDS
        + contract.BCFY_CALLS_SID_POLL_OPTIONAL_FIELDS
    ),
    "replay": sorted(
        contract.BCFY_CALLS_REPLAY_WINDOW_TRUNCATED_REQUIRED_FIELDS
    ),
}, separators=(",", ":"), sort_keys=True))
')
  POLL_FIELDS=$(jq -ce '.poll' <<<"$SLO_FIELDS")
  REPLAY_FIELDS=$(jq -ce '.replay' <<<"$SLO_FIELDS")
  jq -e 'length > 0 and . == (sort | unique) and
    index("event_type") != null and index("schema_version") != null' \
    <<<"$POLL_FIELDS"
  jq -e 'length > 0 and . == (sort | unique) and
    index("event_type") != null and index("schema_version") != null' \
    <<<"$REPLAY_FIELDS"

  INSTANCE_FILTER=$(jq -r '
    map(.numeric_instance_id) | sort |
    map("resource.labels.instance_id=\"" + . + "\"") | join(" OR ")' \
    "$ARTIFACT_DIR/frozen-instances.json")
  test -n "$INSTANCE_FILTER"
  test "$INSTANCE_FILTER" = "$(jq -r '
    map(.numeric_instance_id) | sort | unique |
    map("resource.labels.instance_id=\"" + . + "\"") | join(" OR ")' \
    "$ARTIFACT_DIR/frozen-instances.json")"
  case "$INSTANCE_FILTER" in *'\\"'*) return 65 ;; esac
  POLL_FILTER='resource.type="gce_instance"
AND '"$EVENT_FILTER"'
AND timestamp>="'"$WINDOW_START"'"
AND timestamp<"'"$WINDOW_END"'"
AND ('"$INSTANCE_FILTER"')'

  COLLECTION_NOT_BEFORE=$((END_EPOCH + LOG_INGESTION_GRACE_SECONDS))
  wait_until_epoch "$COLLECTION_NOT_BEFORE"
  SAFE_TEMP_DIR=$(mktemp -d)
  chmod 700 "$SAFE_TEMP_DIR"
  trap 'rm -rf -- "$SAFE_TEMP_DIR"' EXIT

  project_log_read() {
    local output=$1
    jq -cS --arg kind "$KIND" --argjson poll_fields "$POLL_FIELDS" \
      --argjson replay_fields "$REPLAY_FIELDS" '
      def nonempty_string: type == "string" and length > 0;
      map(
        . as $entry
        | (.jsonPayload // null) as $payload
        | if (($entry.insertId | nonempty_string) and
              ($entry.timestamp | nonempty_string) and
              ($entry.receiveTimestamp | nonempty_string) and
              ($entry.logName | nonempty_string) and
              $entry.resource.type == "gce_instance" and
              ($entry.resource.labels.instance_id | nonempty_string) and
              ($payload | type) == "object" and
              (($payload.event_type == "bcfy_calls_sid_poll" and
                $payload.schema_version == 1 and
                $payload.message == "Broadcastify Calls SID poll settled" and
                ($payload | keys | sort) == (($poll_fields + ["message"]) | sort)) or
               ($kind == "soak" and
                $payload.event_type == "bcfy_calls_replay_window_truncated" and
                $payload.schema_version == 1 and
                $payload.message == "Broadcastify Calls replay window truncated" and
                ($payload | keys | sort) == (($replay_fields + ["message"]) | sort))))
          then {
            insertId:$entry.insertId,
            timestamp:$entry.timestamp,
            receiveTimestamp:$entry.receiveTimestamp,
            logName:$entry.logName,
            resource:{
              type:"gce_instance",
              labels:{instance_id:$entry.resource.labels.instance_id}
            },
            jsonPayload:($payload | del(.message))
          }
          else error("unsafe or schema-drifted Logging entry")
          end
      )' >"$output"
  }

  # Provider output exists only in the pipe. jq validates the exact event and
  # payload-key schema (plus Logging's exact expected message wrapper), removes
  # that wrapper, and emits the bounded projection into process-local
  # temporary storage before any byte enters retained evidence. In particular,
  # a bcfy_calls_missing_call payload and its signed audio_url are rejected.
  cloud_timeout 60 gcloud logging read "$POLL_FILTER" \
    --project="$PROJECT" --order=asc --limit="$RESULT_LIMIT" --format=json \
    | project_log_read "$SAFE_TEMP_DIR/first.projected.json"
  sleep "$LOG_STABILITY_GAP_SECONDS"
  cloud_timeout 60 gcloud logging read "$POLL_FILTER" \
    --project="$PROJECT" --order=asc --limit="$RESULT_LIMIT" --format=json \
    | project_log_read "$SAFE_TEMP_DIR/second.projected.json"

  canonicalize_log_read() {
    local projected=$1 canonical=$2 count
    count=$(jq 'length' "$projected")
    test "$count" -gt 0 && test "$count" -lt "$RESULT_LIMIT"
    jq -cS 'sort_by([.timestamp, .insertId, (.jsonPayload | tojson)])' \
      "$projected" >"$canonical"
  }
  canonicalize_log_read "$SAFE_TEMP_DIR/first.projected.json" \
    "$SAFE_TEMP_DIR/first.canonical.json"
  canonicalize_log_read "$SAFE_TEMP_DIR/second.projected.json" \
    "$SAFE_TEMP_DIR/second.canonical.json"

  for pass in first second; do
    canonical="$SAFE_TEMP_DIR/$pass.canonical.json"
    identities="$SAFE_TEMP_DIR/$pass.identities.json"
    jq -cS '[.[] | "insertId:" + .logName + ":" + .insertId] | sort' \
      "$canonical" >"$identities"
    jq -e 'length == (unique | length)' "$identities"
  done

  FIRST_COUNT=$(jq 'length' "$SAFE_TEMP_DIR/first.canonical.json")
  SECOND_COUNT=$(jq 'length' "$SAFE_TEMP_DIR/second.canonical.json")
  FIRST_SHA256=$(sha256sum "$SAFE_TEMP_DIR/first.canonical.json" | awk '{print $1}')
  SECOND_SHA256=$(sha256sum "$SAFE_TEMP_DIR/second.canonical.json" | awk '{print $1}')
  FIRST_IDENTITY_SHA256=$(sha256sum "$SAFE_TEMP_DIR/first.identities.json" | awk '{print $1}')
  SECOND_IDENTITY_SHA256=$(sha256sum "$SAFE_TEMP_DIR/second.identities.json" | awk '{print $1}')
  test "$FIRST_COUNT" = "$SECOND_COUNT"
  test "$FIRST_SHA256" = "$SECOND_SHA256"
  test "$FIRST_IDENTITY_SHA256" = "$SECOND_IDENTITY_SHA256"
  MIN_RECEIVE_TIMESTAMP=$(jq -er '[.[].receiveTimestamp] | min // empty' \
    "$SAFE_TEMP_DIR/second.canonical.json")
  MAX_RECEIVE_TIMESTAMP=$(jq -er '[.[].receiveTimestamp] | max // empty' \
    "$SAFE_TEMP_DIR/second.canonical.json")
  COLLECTION_COMPLETED_AT=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  test "$(date -u -d "$MIN_RECEIVE_TIMESTAMP" +%s)" -le \
    "$(date -u -d "$MAX_RECEIVE_TIMESTAMP" +%s)"
  test "$(date -u -d "$MAX_RECEIVE_TIMESTAMP" +%s)" -le \
    "$(date -u -d "$COLLECTION_COMPLETED_AT" +%s)"

  FROZEN_IDS=$(jq -c '[.[].numeric_instance_id] | sort | unique' \
    "$ARTIFACT_DIR/frozen-instances.json")
  jq --argjson frozen_ids "$FROZEN_IDS" \
     --argjson result_limit "$RESULT_LIMIT" \
     --arg window_start "$WINDOW_START" --arg window_end "$WINDOW_END" \
     --argjson event_types "$EVENT_TYPES_JSON" \
     --argjson grace_seconds "$LOG_INGESTION_GRACE_SECONDS" \
     --argjson stability_gap_seconds "$LOG_STABILITY_GAP_SECONDS" \
     --argjson first_count "$FIRST_COUNT" --argjson second_count "$SECOND_COUNT" \
     --arg first_sha256 "$FIRST_SHA256" --arg second_sha256 "$SECOND_SHA256" \
     --arg first_identity_sha256 "$FIRST_IDENTITY_SHA256" \
     --arg second_identity_sha256 "$SECOND_IDENTITY_SHA256" \
     --arg min_receive_timestamp "$MIN_RECEIVE_TIMESTAMP" \
     --arg max_receive_timestamp "$MAX_RECEIVE_TIMESTAMP" \
     --arg collection_completed_at "$COLLECTION_COMPLETED_AT" '
    . as $entries | {
      collection: {
        schema_version:1, collection_complete:true, limit_reached:false,
        result_limit:$result_limit, returned_count:($entries | length),
        query_event_types:$event_types, frozen_instance_ids:$frozen_ids,
        filter_instance_ids:$frozen_ids, page_count:1,
        pages:[{page_number:1,request_page_token:null,next_page_token:null,
          entry_count:($entries | length),page_complete:true}],
        window_start:$window_start, window_end:$window_end,
        ingestion_grace_seconds:$grace_seconds,
        stability_gap_seconds:$stability_gap_seconds, stability_read_count:2,
        first_returned_count:$first_count, second_returned_count:$second_count,
        first_canonical_sha256:$first_sha256,
        second_canonical_sha256:$second_sha256,
        first_identity_sha256:$first_identity_sha256,
        second_identity_sha256:$second_identity_sha256,
        min_receive_timestamp:$min_receive_timestamp,
        max_receive_timestamp:$max_receive_timestamp,
        collection_completed_at:$collection_completed_at
      }, entries:$entries
    }' "$SAFE_TEMP_DIR/second.canonical.json" \
    >"$SAFE_TEMP_DIR/envelope.json"

  install -m 600 "$SAFE_TEMP_DIR/first.projected.json" "$SAFE_FIRST"
  install -m 600 "$SAFE_TEMP_DIR/second.projected.json" "$SAFE_SECOND"
  install -m 600 "$SAFE_TEMP_DIR/first.canonical.json" "$CANONICAL_FIRST"
  install -m 600 "$SAFE_TEMP_DIR/second.canonical.json" "$CANONICAL_SECOND"
  install -m 600 "$SAFE_TEMP_DIR/first.identities.json" "$IDENTITIES_FIRST"
  install -m 600 "$SAFE_TEMP_DIR/second.identities.json" "$IDENTITIES_SECOND"
  install -m 600 "$SAFE_TEMP_DIR/second.canonical.json" "$PROJECTED_EVIDENCE"
  install -m 600 "$SAFE_TEMP_DIR/envelope.json" "$EVIDENCE_ENVELOPE"
  sha256sum "$SAFE_FIRST" "$SAFE_SECOND" "$CANONICAL_FIRST" \
    "$CANONICAL_SECOND" "$IDENTITIES_FIRST" "$IDENTITIES_SECOND" \
    "$PROJECTED_EVIDENCE" "$EVIDENCE_ENVELOPE" \
    >"$PROJECTED_EVIDENCE.sha256"
)
```

The function's two reads use the same exact filter. The CLI owns internal
service pagination but exposes no page tokens, so the schema-v1 envelope
truthfully records one stabilized CLI result. `collection_complete` means the
window was closed for 120 seconds, neither projected read reached the limit,
and the complete projected result and identity set were byte-identical after
30 more seconds. It is not an absolute no-late-arrival guarantee; a
later-discovered matching entry invalidates acceptance evidence.

After `start_same_frozen_slots sid_lease ...` succeeds, use its hashed start
boundaries for the exact bootstrap window. The inclusive boundary precedes the
first parallel start request; the exclusive deadline is ten minutes after the
last start SSH completed:

```bash
BOOTSTRAP_START_EPOCH=$(cat "$ARTIFACT_DIR/start-started-epoch-$START_LABEL.txt")
LAST_START_COMPLETED_EPOCH=$(cat \
  "$ARTIFACT_DIR/last-start-completed-epoch-$START_LABEL.txt")
case "$BOOTSTRAP_START_EPOCH:$LAST_START_COMPLETED_EPOCH" in
  *[!0-9:]*|:|*:) exit 65 ;;
esac
test "$BOOTSTRAP_START_EPOCH" -le "$LAST_START_COMPLETED_EPOCH"
BOOTSTRAP_WINDOW_END_EPOCH=$((LAST_START_COMPLETED_EPOCH + 600))
BOOTSTRAP_START=$(date -u -d "@$BOOTSTRAP_START_EPOCH" +%Y-%m-%dT%H:%M:%SZ)
BOOTSTRAP_WINDOW_END=$(date -u -d "@$BOOTSTRAP_WINDOW_END_EPOCH" \
  +%Y-%m-%dT%H:%M:%SZ)
collect_complete_log_window bootstrap "$BOOTSTRAP_START" \
  "$BOOTSTRAP_WINDOW_END"
collect_startup_completion_evidence sid_lease "$START_LABEL" \
  "$BOOTSTRAP_START" "$BOOTSTRAP_WINDOW_END" \
  "$ARTIFACT_DIR/start-proof-$START_LABEL.json"

jq --argjson EXPECTED_SIDS "$EXPECTED_SIDS" \
  -f "$PUBLIC_REPO_ROOT/scripts/operations/bcfy_calls_sid/bootstrap.jq" \
  "$ARTIFACT_DIR/bootstrap-envelope.json" \
  >"$ARTIFACT_DIR/bootstrap-result.json"
jq -e '.status == "pass" and .expected_sid_count == 19 and
       .observed_sid_count == 19 and (.BOOTSTRAP_END | type == "string")' \
  "$ARTIFACT_DIR/bootstrap-result.json"
BOOTSTRAP_END=$(jq -er '.BOOTSTRAP_END' \
  "$ARTIFACT_DIR/bootstrap-result.json")
BOOTSTRAP_END_EPOCH=$(date -u -d "$BOOTSTRAP_END" +%s)
test "$BOOTSTRAP_END_EPOCH" -ge "$BOOTSTRAP_START_EPOCH"
test "$BOOTSTRAP_END_EPOCH" -lt "$BOOTSTRAP_WINDOW_END_EPOCH"
sha256sum "$ARTIFACT_DIR/bootstrap-result.json" \
  >"$ARTIFACT_DIR/bootstrap-result.sha256"
```

Reaching the ten-minute deadline without exact 19/19 is a fail-closed
incident/rollback decision, not an indefinite wait. Only after `SID_DURABLE`
passes, set the new soak start to the later of current time and one second after
`BOOTSTRAP_END`. The exact 30-minute half-open window therefore excludes all
bootstrap events and is exactly 30 minutes:

```bash
NOW_EPOCH=$(date +%s)
SOAK_START_EPOCH=$((BOOTSTRAP_END_EPOCH + 1))
if test "$NOW_EPOCH" -gt "$SOAK_START_EPOCH"; then
  SOAK_START_EPOCH=$NOW_EPOCH
fi
SOAK_END_EPOCH=$((SOAK_START_EPOCH + 1800))
SOAK_START=$(date -u -d "@$SOAK_START_EPOCH" +%Y-%m-%dT%H:%M:%SZ)
SOAK_END=$(date -u -d "@$SOAK_END_EPOCH" +%Y-%m-%dT%H:%M:%SZ)
test "$SOAK_START_EPOCH" -gt "$BOOTSTRAP_END_EPOCH"
test "$((SOAK_END_EPOCH - SOAK_START_EPOCH))" -eq 1800
collect_complete_log_window soak "$SOAK_START" "$SOAK_END"

jq --argjson EXPECTED_SIDS "$EXPECTED_SIDS" \
  --arg BOOTSTRAP_END "$BOOTSTRAP_END" \
  --arg SOAK_START "$SOAK_START" --arg SOAK_END "$SOAK_END" \
  --argjson MISSING_SUCCESS_THRESHOLD_SECONDS 60 \
  --argjson CURSOR_LAG_THRESHOLD_SECONDS 300 \
  --argjson SUSTAINED_CONSECUTIVE_POLLS 6 \
  --argjson SUSTAINED_SECONDS 60 \
  -f "$PUBLIC_REPO_ROOT/scripts/operations/bcfy_calls_sid/soak.jq" \
  "$ARTIFACT_DIR/soak-envelope.json" \
  >"$ARTIFACT_DIR/soak-result.json"
jq -e '.status == "pass"' "$ARTIFACT_DIR/soak-result.json"
sha256sum "$ARTIFACT_DIR/soak-result.json" \
  >"$ARTIFACT_DIR/soak-result.sha256"
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
combines the complete process fence, every exact `startup_pacing_complete`
record's `bcfy_calls_authority_mode=sid_lease`, selected `[feed,sid]` domains,
signed profile digest, worker index/process ID, and static exclusive-mode tests.
This configured/process proof does not directly inspect a legacy wire selector;
its claim is limited to the managed frozen fleet.

## Accepted Phase 7 evidence contract

The approval for this runbook explicitly amends the stronger draft evidence
language; it does not silently treat missing evidence as passed:

- **POLL-07 amplification interpretation:** use the denominator-weighted sum of
  exact **per-response** distinct provider audio URLs over the post-bootstrap
  30-minute production soak. This is not window-wide cross-poll cardinality.
  That stronger measure remains unverified/deferred because it would require
  high-cardinality URL retention or a new distributed sketch and state
  lifecycle, adding long-term maintenance without improving ingestion
  correctness.
- **OPER-06:** use versioned direct Cloud Logging queries, deterministic
  checked-in reducers, and predeclared thresholds for bootstrap/soak; permanent alert policies
  are deferred and are not claimed by manual evidence.
- **ROLL-07:** deterministic provider/collector/SID contract tests run before
  activation, and the first normal 19/19 production SID traffic verifies the
  live path after activation; no separate live probe runs. The provider-specific
  cutover risk is explicit.
- **ROLL-08:** two independent reviewers cover the external-fencing PITR
  runbook/tabletop. It remains `REVIEWED — NOT EXERCISED`; no unit test or
  tabletop is a live restore.

All four requirements remain pending until their respective deterministic and
production evidence gates complete. Permanent alerts, window-wide URL
cardinality, a separate provider probe, and a live PITR exercise are not Phase 7
completion claims.

## PREPARED_LEGACY

**Entry gate:** All absolute entry conditions pass. Public/deployment commits,
reviews, module pins, exact worker and shared SQL Job image digests, numeric
password-secret version, saved Terraform/config proof that both SQL Jobs use
those pins, live operation-Job identity/IAM/generation, backup window, and
change window are approved. Read-only
`004_verify.sql` proves the
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
and workflow state with `capture_cutover_controls original`. Execute
`set_and_apply_ingestion_tuple legacy_feed true "$CANDIDATE_IMAGE" frozen`,
then `capture_cutover_controls frozen` and compare the exact stable and frozen
controls. This first maintenance apply removes the
autoscaler, clears autohealing, and selects DO_NOTHING before any replacement
template can introduce SID authority. Compute rejects DO_NOTHING while an
autoscaler remains attached, even in OFF mode, so do not execute a stale
`mode=off` then DO_NOTHING sequence. The second stable
`capture_cutover_controls frozen` read is the verification; do not issue a
second direct MIG mutation outside the reviewed saved-plan apply.

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

```bash
reprove_process_absent pre-activation
PRE_ACTIVATION_PROOF=$PROCESS_PROOF_JSON
run_sid_operation activate "$MANIFEST_DIGEST"
run_sid_operation verify
reprove_process_absent post-activation
POST_ACTIVATION_PROOF=$PROCESS_PROOF_JSON
```

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

**Mutation:** Invoke Shared primitive E as
`start_same_frozen_slots sid_lease "$START_LABEL" "$POST_ACTIVATION_PROOF"`;
it removes only the Phase 7 runtime `Restart=no` drop-ins, restores approved
`Restart=always`/enabled state, and locally proves the same numeric IDs, exact
slots, image, mode, and profile. Shared primitive F then captures exactly one
schema-checked `startup_pacing_complete` record per slot and runs
`bootstrap.jq` over the complete reducer-safe projected poll population.

```bash
START_LABEL="sid-bootstrap-$(date -u +%Y%m%dT%H%M%SZ)"
case "$START_LABEL" in *[!A-Za-z0-9._-]*|'') exit 64 ;; esac
start_same_frozen_slots sid_lease "$START_LABEL" "$POST_ACTIVATION_PROOF"
```

**Expected bounded output:** Every slot starts once with its same instance ID,
worker index, new PID, `profile=mixed-dormant`, signed profile digest,
`bcfy_calls_authority_mode=sid_lease`, selected `[feed,sid]` domains, and
candidate image digest. `bootstrap.jq` returns exact 19/19 first successful
provider-observed, valid/non-regressive `lastPos` events and one
`BOOTSTRAP_END`.

**Evidence to retain:** Same-slot start commands, two stable projected startup
reads, startup event identities/envelope, process/image/config report, complete
bootstrap filter/envelope/projected object ID and hash, reducer result, and
`BOOTSTRAP_END`.

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
run `set_and_apply_ingestion_tuple sid_lease true "$CANDIDATE_IMAGE"
sid-durable`, then `capture_cutover_controls sid-durable` and compare it to the
frozen capture. This reconciles durable `sid_lease`, `mixed-dormant`,
maintenance state, exact image, and unchanged hashed AlloyDB endpoint. Do not
use the application rollout. Compare the rendered replacement template to
every active process before changing controls.

```bash
set_and_apply_ingestion_tuple sid_lease true "$CANDIDATE_IMAGE" sid-durable
capture_cutover_controls sid-durable
compare_cutover_controls "$ARTIFACT_DIR/controls-frozen" \
  "$ARTIFACT_DIR/controls-sid-durable" frozen sid_lease "$CANDIDATE_IMAGE"
capture_live_template_runtime_agreement sid-durable sid_lease \
  "$ARTIFACT_DIR/start-proof-$START_LABEL.json" \
  "$ARTIFACT_DIR/controls-sid-durable" \
  "$ARTIFACT_DIR/tuple-apply-sid-durable"
```

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
reducer-safe projected log population over `[SOAK_START, SOAK_END)`, hash it,
and run `soak.jq`.
Run aggregate gates first; inspect bounded per-SID detail only after failure.

**Expected bounded output:** Exact 19 SIDs; 90–120 logical polls/minute;
attempts/logical ≤1.25; rows/summed exact per-response distinct URLs ≤1.25;
monotonic lastPos; no gap over 60 seconds; no six-poll/60-second sustained
hazard; no replay truncation; and configured/process proof of zero managed-fleet
legacy authority.

**Evidence to retain:** Exact filters and half-open timestamps, result limit and
completion proof, projected-population object ID/hash, reducer version/result,
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
false by running `set_and_apply_ingestion_tuple sid_lease false
"$CANDIDATE_IMAGE" accepted`, then `capture_cutover_controls restored` and the
exact `restored` comparison against `original`. Retain `sid_lease`,
`mixed-dormant`, exact image, and unchanged hashed endpoint. This is control
restoration after acceptance, not the authority transition.

```bash
set_and_apply_ingestion_tuple sid_lease false "$CANDIDATE_IMAGE" accepted
capture_cutover_controls restored
compare_cutover_controls "$ARTIFACT_DIR/controls-original" \
  "$ARTIFACT_DIR/controls-restored" restored sid_lease "$CANDIDATE_IMAGE"
```

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
membership writers, both deploy paths, and manual fleet mutation are frozen;
the complete current SID fleet is captured. Do not assume automatic controls
remain suppressed after `ACCEPTED`.

**Mutation:** If `ACCEPTED` restored automatic controls, first use the closed
branch below to perform a same-authority `sid_lease` maintenance reconcile and
capture/compare its frozen controls. Before acceptance, reuse and reprove the
already-frozen controls; do not add another reconcile. Then select and attest
the newest exact durable marker in this order: `rollback-frozen`,
`sid-durable`, `frozen`. Reject inconsistent acceptance artifacts, a missing
marker, or an unknown mode. Under an absent autoscaler, no autohealing,
DO_NOTHING failure action, and the deploy/manual freeze, either known
replacement template is inert. Use Shared primitive B with
`TARGET_MODE=legacy_feed`; it suppresses unit boot/restart while running SID
processes retain their loaded mode. Execute Shared primitive C, including exact
90-second graceful, 100-second Docker, and 120-second systemd bounds, then
reprove unchanged numeric fleet and `currentAction=NONE` with every SID slot
absent. Durable legacy reconciliation occurs only after inverse child state and
successful same-slot legacy startup.

```bash
ACCEPTED_PLAN="$ARTIFACT_DIR/tuple-apply-accepted/plan-pin-projection.json"
ACCEPTED_CONTROLS="$ARTIFACT_DIR/controls-restored/controls.sha256"
if test -s "$ACCEPTED_PLAN" && test -s "$ACCEPTED_CONTROLS"; then
  set_and_apply_ingestion_tuple sid_lease true "$CANDIDATE_IMAGE" \
    rollback-frozen
elif test -e "$ACCEPTED_PLAN" || test -e "$ACCEPTED_CONTROLS" || \
     test -e "$ARTIFACT_DIR/tuple-apply-rollback-frozen"; then
  exit 65
fi

capture_cutover_controls rollback-entry
if test -s "$ARTIFACT_DIR/tuple-apply-rollback-frozen/plan-pin-projection.json"; then
  test -s "$ACCEPTED_PLAN" && test -s "$ACCEPTED_CONTROLS"
  LAST_DURABLE_MARKER=tuple-apply-rollback-frozen
  LAST_DURABLE_MODE=sid_lease
  CONTROL_BASE="$ARTIFACT_DIR/controls-restored"
elif test -s "$ARTIFACT_DIR/tuple-apply-sid-durable/plan-pin-projection.json"; then
  LAST_DURABLE_MARKER=tuple-apply-sid-durable
  LAST_DURABLE_MODE=sid_lease
  CONTROL_BASE="$ARTIFACT_DIR/controls-sid-durable"
elif test -s "$ARTIFACT_DIR/tuple-apply-frozen/plan-pin-projection.json"; then
  LAST_DURABLE_MARKER=tuple-apply-frozen
  LAST_DURABLE_MODE=legacy_feed
  CONTROL_BASE="$ARTIFACT_DIR/controls-frozen"
else
  exit 65
fi
jq -e --arg public "$PUBLIC_COMMIT" --arg mode "$LAST_DURABLE_MODE" \
  --arg image "$CANDIDATE_IMAGE" '
  .public_sha == $public and
  .cutover_contract.authority_mode == $mode and
  .cutover_contract.maintenance_mode == true and
  .cutover_contract.ingestion_image == $image' \
  "$ARTIFACT_DIR/$LAST_DURABLE_MARKER/plan-pin-projection.json"
compare_cutover_controls "$CONTROL_BASE" \
  "$ARTIFACT_DIR/controls-rollback-entry" frozen \
  "$LAST_DURABLE_MODE" "$CANDIDATE_IMAGE"
jq -e '.present == false' \
  "$ARTIFACT_DIR/controls-rollback-entry/autoscaler.json"
jq -e '.autoHealingPolicies == [] and
  .instanceLifecyclePolicy.defaultActionOnFailure == "DO_NOTHING" and
  .status.isStable == true' \
  "$ARTIFACT_DIR/controls-rollback-entry/mig.json"
jq -e 'all(.[]; .current_action == "NONE")' \
  "$ARTIFACT_DIR/controls-rollback-entry/fleet.json"
printf '%s\n' "$LAST_DURABLE_MODE" \
  >"$ARTIFACT_DIR/rollback-last-durable-mode.txt"
sha256sum "$ARTIFACT_DIR/$LAST_DURABLE_MARKER/plan-pin-projection.json" \
  "$ARTIFACT_DIR/rollback-last-durable-mode.txt" \
  >"$ARTIFACT_DIR/rollback-last-durable-mode.sha256"

TARGET_MODE=legacy_feed
# Execute Shared primitive B, then Shared primitive C. Do not call
# set_and_apply_ingestion_tuple again before legacy starts.
```

**Expected bounded output:** Exact same complete process absence proof shape for every
captured numeric VM ID and both worker slots: process-free unit, `MainPID=0`,
container absent, unchanged fleet, `currentAction=NONE`. No legacy process has
started.

**Evidence to retain:** Trigger/incident owner, captured controls, exact last
durable template-mode branch and hash, staged key-only proof, stop/force timings
and exact targets, full slot rows, inventory/hash, and reviewed absence decision.

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

```bash
reprove_process_absent pre-inverse
PRE_INVERSE_PROOF=$PROCESS_PROOF_JSON
run_sid_operation rollback_children
run_sid_operation verify
reprove_process_absent post-inverse
POST_INVERSE_PROOF=$PROCESS_PROOF_JSON
```

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

**Mutation:** Invoke Shared primitive E as
`START_LABEL="legacy-rollback-$(date -u +%Y%m%dT%H%M%SZ)"` followed by
`start_same_frozen_slots legacy_feed "$START_LABEL" "$POST_INVERSE_PROOF"`.
Verify its exact local startup proof and startup records before reconciling
durable legacy authority while maintenance remains true with
`set_and_apply_ingestion_tuple legacy_feed true "$CANDIDATE_IMAGE"
legacy-durable`. Capture controls and reprove the same live legacy PIDs, exact
numeric fleet, and `currentAction=NONE`; a template-driven replacement or
action blocks restoration. Only then run `set_and_apply_ingestion_tuple
legacy_feed false "$CANDIDATE_IMAGE" legacy-restored`. Capture restored
controls, compare them exactly to `original`, and restore deploy/membership
writers.

```bash
START_LABEL="legacy-rollback-$(date -u +%Y%m%dT%H%M%SZ)"
case "$START_LABEL" in *[!A-Za-z0-9._-]*|'') exit 64 ;; esac
start_same_frozen_slots legacy_feed "$START_LABEL" "$POST_INVERSE_PROOF"
LEGACY_START_EPOCH=$(cat \
  "$ARTIFACT_DIR/start-started-epoch-$START_LABEL.txt")
LEGACY_LAST_START_EPOCH=$(cat \
  "$ARTIFACT_DIR/last-start-completed-epoch-$START_LABEL.txt")
case "$LEGACY_START_EPOCH:$LEGACY_LAST_START_EPOCH" in
  *[!0-9:]*|:|*:) exit 65 ;;
esac
test "$LEGACY_START_EPOCH" -le "$LEGACY_LAST_START_EPOCH"
LEGACY_START=$(date -u -d "@$LEGACY_START_EPOCH" +%Y-%m-%dT%H:%M:%SZ)
LEGACY_START_END=$(date -u -d "@$((LEGACY_LAST_START_EPOCH + 600))" \
  +%Y-%m-%dT%H:%M:%SZ)
collect_startup_completion_evidence legacy_feed "$START_LABEL" \
  "$LEGACY_START" "$LEGACY_START_END" \
  "$ARTIFACT_DIR/start-proof-$START_LABEL.json"

set_and_apply_ingestion_tuple legacy_feed true "$CANDIDATE_IMAGE" \
  legacy-durable
capture_cutover_controls legacy-durable
compare_cutover_controls "$ARTIFACT_DIR/controls-frozen" \
  "$ARTIFACT_DIR/controls-legacy-durable" frozen legacy_feed \
  "$CANDIDATE_IMAGE"
capture_live_template_runtime_agreement legacy-durable legacy_feed \
  "$ARTIFACT_DIR/start-proof-$START_LABEL.json" \
  "$ARTIFACT_DIR/controls-legacy-durable" \
  "$ARTIFACT_DIR/tuple-apply-legacy-durable"
set_and_apply_ingestion_tuple legacy_feed false "$CANDIDATE_IMAGE" \
  legacy-restored
capture_cutover_controls restored
compare_cutover_controls "$ARTIFACT_DIR/controls-original" \
  "$ARTIFACT_DIR/controls-restored" restored legacy_feed "$CANDIDATE_IMAGE"
```

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
