"""Static safety contract for the Phase 7 operator runbooks.

These tests intentionally validate the load-bearing state-machine vocabulary and
the accepted evidence boundary.  They do not claim that a production cutover or
an AlloyDB restore has been executed.
"""

from __future__ import annotations

import hashlib
import json
import os
import pathlib
import re
import subprocess

ROOT = pathlib.Path(__file__).resolve().parents[2]
CUTOVER = ROOT / "docs/runbooks/bcfy-calls-sid-cutover.md"
PITR = ROOT / "docs/runbooks/alloydb-pitr.md"
EVIDENCE = ROOT / "docs/runbooks/evidence/bcfy-calls-sid-cutover-template.md"
REQUIREMENTS = ROOT / ".planning/REQUIREMENTS.md"
ROADMAP = ROOT / ".planning/ROADMAP.md"
STATE = ROOT / ".planning/STATE.md"
VALIDATION = (
    ROOT
    / ".planning/phases/07-production-verification-and-exclusive-cutover"
    / "07-VALIDATION.md"
)

STATES = (
    "PREPARED_LEGACY",
    "FROZEN_LEGACY",
    "NO_AUTHORITY",
    "SID_DATA_READY",
    "SID_BOOTSTRAP",
    "SID_DURABLE",
    "SID_SOAK",
    "ACCEPTED",
    "ROLLBACK_NO_AUTHORITY",
    "LEGACY_DATA_READY",
    "LEGACY_RESTORED",
)
STATE_FIELDS = (
    "Entry gate",
    "Mutation",
    "Expected bounded output",
    "Evidence to retain",
    "Stop condition",
    "Rollback edge",
    "Accountability",
)


def _read(path: pathlib.Path) -> str:
    return path.read_text(encoding="utf-8")


def _section(text: str, heading: str) -> str:
    pattern = rf"(?ms)^## {re.escape(heading)}\s*$\n(.*?)(?=^## |\Z)"
    match = re.search(pattern, text)
    assert match is not None, f"missing state section: {heading}"
    return match.group(1)


def test_cutover_states_are_complete_and_strictly_ordered() -> None:
    text = _read(CUTOVER)
    positions = [text.index(f"## {state}\n") for state in STATES]
    assert positions == sorted(positions)

    for state in STATES:
        body = _section(text, state)
        for field in STATE_FIELDS:
            assert f"**{field}:**" in body, f"{state} lacks {field}"
        assert "Operator:" in body
        assert "Reviewer:" in body
        assert "UTC:" in body


def test_cutover_has_fail_closed_process_and_control_contract() -> None:
    text = _read(CUTOVER)
    for required in (
        "07-NONPROD-REHEARSAL.md",
        "hard production prerequisite",
        "numeric instance ID",
        "worker index",
        "ActiveState",
        "SubState",
        "MainPID",
        "container_exists",
        "currentAction=NONE",
        "90-second",
        "100-second",
        "120-second",
        "SSH",
        "replaced",
        "disappeared",
        "unknown unit",
        "unknown container",
        "--clear-autohealing",
        "defaultActionOnFailure",
        '== "DO_NOTHING"',
        "WORKER_PROFILE=mixed-dormant",
        "BCFY_CALLS_AUTHORITY_MODE=sid_lease",
        "BCFY_CALLS_AUTHORITY_MODE=legacy_feed",
        "Restart=always",
        "both worker slots",
        "same frozen slots",
    ):
        assert required in text

    no_authority = _section(text, "NO_AUTHORITY")
    activation = _section(text, "SID_DATA_READY")
    rollback_absence = _section(text, "ROLLBACK_NO_AUTHORITY")
    rollback_data = _section(text, "LEGACY_DATA_READY")
    assert text.index("Execute `002_activate.sql`") > text.index(
        "## NO_AUTHORITY\n"
    )
    assert "002_activate.sql" in activation
    assert text.index("Execute only `003_rollback_children.sql`") > text.index(
        "## ROLLBACK_NO_AUTHORITY\n"
    )
    assert "process absence" in no_authority.lower()
    assert "process absence" in rollback_absence.lower()
    assert "003_rollback_children.sql" in rollback_data


def test_cutover_uses_closed_operation_and_checked_reducer_seams() -> None:
    text = _read(CUTOVER)
    for required in (
        "001_preseed.sql",
        "002_activate.sql",
        "003_rollback_children.sql",
        "004_verify.sql",
        "bootstrap.jq",
        "soak.jq",
        "gcloud run jobs execute",
        "--wait",
        "SOAK_START",
        "SOAK_END",
        "[SOAK_START, SOAK_END)",
        "exactly 30 minutes",
        "90–120",
        "1.25",
        "six consecutive polls",
        "60 seconds",
        "19/19",
        "154",
        "configured/process proof",
        "does not directly inspect a legacy wire selector",
        "sum of exact per-response distinct",
        "window-wide cross-poll",
        "gcloud logging read",
        "--order=asc",
        "sha256sum",
    ):
        assert required in text
    assert "--page-token" not in text
    assert "--page-size" not in text


def test_cutover_forbids_unsafe_rollout_reset_and_false_evidence() -> None:
    text = _read(CUTOVER)
    lower = text.lower()
    for required in (
        "ordinary app rollout is forbidden",
        "database rows and logs are diagnostics",
        "fixtures do not prove process absence",
        "tabletop does not replace",
        "do not delete",
        "do not reset",
        "do not reduce",
    ):
        assert required in lower
    assert "rolling-action replace" not in lower
    assert not re.search(
        r"(?i)delete\s+from\s+(?:public\.)?ingestion_leases", text
    )
    assert not re.search(
        r"(?i)truncate\s+(?:table\s+)?(?:public\.)?ingestion_leases", text
    )


def test_evidence_index_is_secret_free_and_covers_every_gate() -> None:
    text = _read(EVIDENCE)
    for state in STATES:
        assert f"| `{state}` |" in text
    for required in (
        "public commit",
        "deployment commit",
        "image digest",
        "manifest digest",
        "execution ID",
        "numeric instance ID",
        "worker index",
        "projected-population object ID",
        "SHA-256",
        "Operator",
        "Reviewer",
        "UTC",
        "NO PRODUCTION CUTOVER PERFORMED",
        "PITR REVIEWED — NOT EXERCISED",
    ):
        assert required in text
    for forbidden in (
        "BEGIN PRIVATE KEY",
        "Authorization: Bearer",
        "X-Goog-Signature=",
        "PGPASSWORD=",
        "BROADCASTIFY_PASSWORD=",
    ):
        assert forbidden not in text


def test_pitr_is_out_of_place_externally_fenced_and_unexercised() -> None:
    text = _read(PITR)
    for required in (
        "REVIEWED — NOT EXERCISED",
        "out-of-place",
        "source remains fenced",
        "complete external process fence",
        "5432",
        "6432",
        "current Secret Manager credentials",
        "gcloud alloydb clusters restore",
        "--source-cluster",
        "--point-in-time",
        "gcloud alloydb instances create",
        "active_alloydb_primary_instance_ip",
        "all eight consumers",
        "activated rows",
        "dormant pre-seed rows",
        "pre-schema",
        "ordered migrations",
        "never mass-reset",
        "INTERNAL_AUTOMATION",
        "feeds-abandoned-lease-sweep",
        "feeds-vac",
        "alloydb.enable_pg_cron=off",
        "raw Terraform/user-data value",
        "numeric worker Secret Manager version",
        "automated-backup policy",
        "continuous-backup recovery window",
        "deletion protection",
        "Query Insights",
        "durable IaC ownership",
        "JOB_BEFORE",
        "JOB_AFTER",
        "EXECUTION_URI",
        "SIGNED_EXECUTION_DISCOVERY_REDUCER",
        "SIGNED_VERIFY_REDUCER",
        "--expected-manifest-digest",
        'gcloud run jobs execute "$SID_OPERATION_JOB_ID"',
        '--job="$SID_OPERATION_JOB_ID"',
        'EXECUTION_URI="${SID_OPERATION_JOB_URI}/executions/${EXECUTION_ID}"',
        "metadata.creationTimestamp",
        "v1/Kubernetes shape",
        "only `JOB_BEFORE` and `JOB_AFTER`",
    ):
        assert required in text
    assert re.search(r"byte-for-byte\s+digest equality", text)
    assert "delete the source" not in text.lower()
    assert "rename the source" not in text.lower()
    assert "live restore passed" not in text.lower()


def test_tabletop_matrix_covers_required_fail_closed_injections() -> None:
    combined = _read(CUTOVER) + _read(PITR)
    for injection in (
        "SSH-unreachable",
        "numeric-ID replacement",
        ">120-second worker",
        "last activation assertion failure",
        "incomplete log collection",
        "failed durable Terraform apply",
        "wrong restored credential",
        "endpoint fan-out miss",
        "pre-schema restore",
        "dormant-row restore",
        "source cron Job remains scheduled/running",
        "raw MIG password",
        "backup/deletion-protection/label/Query-Insights/IaC parity miss",
        "004 emits a digest but comparator is absent/nonzero",
        "execution short name cannot bind to canonical Job-qualified URI",
        "source Job UID/generation changes before/after execution",
    ):
        assert injection in combined
    assert combined.count("Independent review") >= 2
    assert "no unmitigated HIGH" in combined


def test_accepted_contract_amendments_are_explicit_but_pending() -> None:
    # .planning is intentionally local/ignored.  The tracked runbooks must
    # publish the contract in a clean clone; when GSD state is present, validate
    # that its four ledgers mirror the same decision and remain pending.
    tracked_contract = "\n".join((_read(CUTOVER), _read(PITR)))

    for required in (
        "POLL-07 amplification interpretation",
        "denominator-weighted",
        "per-response",
        "window-wide cross-poll",
        "distributed sketch",
        "versioned direct Cloud Logging queries",
        "permanent alert policies",
        "deterministic provider/collector/SID contract tests",
        "first normal 19/19 production SID traffic",
        "no separate live probe",
        "REVIEWED — NOT EXERCISED",
    ):
        assert required in tracked_contract

    planning_paths = (REQUIREMENTS, ROADMAP, STATE, VALIDATION)
    if not all(path.exists() for path in planning_paths):
        return

    requirements, roadmap, state, validation = map(_read, planning_paths)
    planning_contract = f"{requirements}\n{roadmap}\n{state}\n{validation}"
    for required in (
        "POLL-07 amplification interpretation",
        "denominator-weighted",
        "window-wide cross-poll",
        "permanent alert policies",
        "no separate live probe",
        "REVIEWED — NOT EXERCISED",
    ):
        assert required in planning_contract

    for requirement in ("POLL-07", "OPER-06", "ROLL-07", "ROLL-08"):
        assert re.search(rf"- \[ \] \*\*{requirement}\*\*", requirements)
        assert re.search(
            rf"\| {requirement} \| Phase 7 \| Pending \|", requirements
        )


def test_documents_do_not_preclaim_live_actions() -> None:
    paths = (CUTOVER, PITR, EVIDENCE, VALIDATION)
    combined = "\n".join(_read(path) for path in paths if path.exists())
    assert "NO PRODUCTION CUTOVER PERFORMED" in combined
    assert "PITR REVIEWED — NOT EXERCISED" in combined
    for false_claim in (
        "production cutover passed",
        "production soak passed",
        "live pitr passed",
        "pitr exercise passed",
    ):
        assert false_claim not in combined.lower()


def test_cutover_binds_signed_inputs_to_full_provider_resources() -> None:
    text = _read(CUTOVER)
    for required in (
        ".project_number|tostring",
        "EXPECTED_MIG_URI=",
        "EXPECTED_AUTOSCALER_URI=",
        "EXPECTED_JOB_URI=",
        "EXPECTED_SQL_BUCKET_URI=",
        ".mig_uri == $mig_uri",
        ".autoscaler_uri == $autoscaler_uri",
        ".operation_job_uri == $operation_job_uri",
        ".sql_bucket_uri == $sql_bucket_uri",
        ".selfLink == $uri",
        ".target == $mig_uri",
        'gcloud projects describe "$PROJECT"',
        'gcloud run jobs describe "$OP_JOB"',
        "operation-job-$label-projection.json",
        "bound-project-projection.json",
        "bound-mig-projection.json",
        "bound-autoscaler-projection.json",
        "object-generations.json",
        "content-sha256.json",
    ):
        assert required in text
    assert 'gcloud run jobs describe "$EXPECTED_JOB_URI"' not in text
    assert '"$ARTIFACT_DIR/bound-project.json"' not in text
    assert '"$ARTIFACT_DIR/bound-mig.json"' not in text
    assert '"$ARTIFACT_DIR/bound-autoscaler.json"' not in text
    assert "private provider payloads" in text


def test_autoscaler_reads_use_stable_bounded_sdk_export_projection() -> None:
    text = _read(CUTOVER)
    for required in (
        "export_autoscaling_projection() (",
        "gcloud beta compute instance-groups managed",
        'export-autoscaling "$MIG"',
        '--autoscaling-file="$temp/$pass-export.json"',
        ".status.autoscaler // null",
        "description:(.description // null)",
        '((keys - ["autoscalingPolicy","description","recommendedSize",',
        '"scalingScheduleStatus"]) | length) == 0',
        '(.autoscalingPolicy | type) == "object"',
        "autoscalingPolicy:.autoscalingPolicy",
        'cmp -s "$temp/first-status.json" "$temp/second-status.json"',
        'cmp -s "$temp/first-projection.json" "$temp/second-projection.json"',
        "jq -e '. == null' \"$temp/second-status.json\"",
        "jq -e --arg uri \"$EXPECTED_AUTOSCALER_URI\" '. == $uri'",
        "{present:true,name:$name,selfLink:$uri,target:$target,",
        "select(.present == true) | {name,selfLink,target}",
        "--autoscaling-file=PATH",
    ):
        assert required in text
    assert text.count("cloud_timeout() {") == 1
    assert text.index("cloud_timeout() {") < text.index(
        "export_autoscaling_projection() ("
    )
    assert text.index("export_autoscaling_projection() (") < text.index(
        "capture_cutover_controls() ("
    )
    assert "gcloud compute autoscalers" not in text
    assert "print-access-token" not in text
    assert "Authorization: Bearer" not in text
    assert '"$ARTIFACT_DIR/$pass-export.json"' not in text
    assert "scaleInControl:(.scaleInControl" not in text
    assert "recommendedSize:(.recommendedSize" not in text
    assert "scalingScheduleStatus:(.scalingScheduleStatus" not in text


def test_cutover_stop_and_restart_are_exact_complete_and_bounded() -> None:
    text = _read(CUTOVER)
    for required in (
        "InstanceName=%s ExpectedNumericID=%s LiveNumericID=%s OriginalUnit=%s",
        "StageContinuity InstanceName=%s NumericID=%s BeforeSlot1=%s",
        "before_slot_1=",
        "after_slot_1=",
        "before_slot_2=",
        "after_slot_2=",
        "NonTargetEnvEqual=true",
        "expected_rows=$((2 *",
        "STOP_SOFT_DEADLINE=$((STOP_T0_EPOCH + 90))",
        "STOP_DOCKER_DEADLINE=$((STOP_T0_EPOCH + 100))",
        "STOP_HARD_DEADLINE=$((STOP_T0_EPOCH + 120))",
        "proof_deadline=$(($(date +%s) + proof_timeout))",
        "STOP_T0_EPOCH=$(($(date +%s) + 30))",
        "launch_timed_stop_controllers",
        "ARMED|%s|%s|%s|%s",
        'if test "$(date +%s)" -ge "$STOP_T0_EPOCH"; then',
        "metadata.google.internal/computeMetadata/v1/instance/id",
        "snapshot_slot T90",
        "snapshot_slot T100",
        "no new IAP",
        "stop-final-members-$STOP_T0_EPOCH.json",
        ".completed_at_epoch <= $deadline",
        ".observed_at_epoch >= $soft and .observed_at_epoch <= $deadline",
        "stop-survivors-$STOP_T0_EPOCH.json",
        "ControlPID",
        "CgroupProcessCount",
        "UnknownAuthorityContainerCount",
        "HostProcessCount",
        "assert_process_absent",
        'test "$(date +%s)" -le "$STOP_HARD_DEADLINE"',
        "start_same_frozen_slots",
        "assert_process_started",
        ".configured_image == $image",
        ".worker_index_count == 1",
        ".ingestion_authority_container_count == 2",
        "sudo systemctl enable",
        "Restart --value",
        "= always",
    ):
        assert required in text
    assert "os.system" not in text
    assert "before_env_sha256" not in text
    assert "after_env_sha256" not in text
    assert "systemctl kill --kill-who=all" in text
    assert "name=^/$container\\$" in text


def test_all_persisted_mig_memberships_are_minimal_projections() -> None:
    text = _read(CUTOVER)
    captures = list(
        re.finditer(
            r'gcloud compute instance-groups managed list-instances "\$MIG"',
            text,
        )
    )
    assert len(captures) == 5
    projection_captures = []
    for capture in captures:
        command = text[capture.start() : capture.start() + 650]
        if "| jq -cS 'map({" not in command:
            assert "| jq -cr 'sort_by(.instance)[]" in command
            assert (
                "numeric_instance_id:$id"
                in text[capture.start() : capture.start() + 1300]
            )
            continue
        projection_captures.append(capture)
        redirect = command.index(">")
        projection = command.index("| jq -cS 'map({")
        assert projection < redirect
        for field in (
            'instance_name:(.instance | split("/")[-1])',
            'zone:(.instance | split("/")[-3])',
            "current_action:.currentAction",
            'instance_template:((.version.instanceTemplate // "") '
            '| split("/")[-1])',
            "sort_by(.instance_name)",
        ):
            assert field in command
    assert len(projection_captures) == 4

    assert (
        text.count("{instance_name,zone,current_action,instance_template}") == 3
    )

    assert not re.search(
        r'--format=json\s*\\\n\s*>(?:"\$ARTIFACT_DIR/[^\n]*members|'
        r'"\$(?:members_file|start_members))',
        text,
    )
    programs = re.findall(
        r"(?ms)--format=json \\\n\s*\| jq -cS '(map\(\{.*?\}\) "
        r"\| sort_by\(\.instance_name\))'",
        text,
    )
    assert len(programs) == 4
    provider_fixture = [
        {
            "instance": (
                "https://www.googleapis.com/compute/v1/projects/p/zones/"
                "us-west1-a/instances/worker-a"
            ),
            "currentAction": "NONE",
            "version": {"instanceTemplate": "templates/reviewed-template"},
            "privateProviderField": "must-not-survive",
        }
    ]
    for program in programs:
        result = subprocess.run(
            ["jq", "-cS", program],
            input=json.dumps(provider_fixture),
            text=True,
            capture_output=True,
            check=False,
        )
        assert result.returncode == 0, result.stderr
        assert json.loads(result.stdout) == [
            {
                "current_action": "NONE",
                "instance_name": "worker-a",
                "instance_template": "reviewed-template",
                "zone": "us-west1-a",
            }
        ]
        assert "must-not-survive" not in result.stdout


def test_host_process_counter_does_not_self_match_and_counts_workers() -> None:
    text = _read(CUTOVER)
    assert text.count("module_prefix='backend.pipeline.ingestion.'") == 2
    assert text.count('module_name=\\"\\${module_prefix}main\\"') == 2
    assert text.count('pgrep -af \\"\\$module_pattern\\"') == 2
    # The only complete module name is explanatory prose, never an SSH command
    # body. Both proof bodies construct the Docker needle and regex at runtime.
    assert text.count("backend.pipeline.ingestion.main") == 1

    fixture = r"""
set -euo pipefail
module_prefix="phase7-fixture-$$.pipeline.ingestion."
module_name="${module_prefix}main"
module_pattern=$(printf '%s' "$module_name" | sed 's/[.]/[.]/g')

count_matching() {
  excluded_pids="$$"
  ancestor_pid=$PPID
  while test "$ancestor_pid" -gt 1; do
    excluded_pids="$excluded_pids $ancestor_pid"
    ancestor_pid=$(ps -o ppid= -p "$ancestor_pid" | awk 'NF {print $1}')
    case "$ancestor_pid" in *[!0-9]*|'') break ;; esac
  done
  (pgrep -af "$module_pattern" || true) \
  | awk -v excluded="$excluded_pids" '
      BEGIN {
        count = split(excluded, pid, /[[:space:]]+/)
        for (i = 1; i <= count; i++) skip[pid[i]] = 1
      }
      !($1 in skip) { matches += 1 }
      END { print matches + 0 }
    '
}

first=
second=
cleanup() {
  test -z "$first" || kill "$first" 2>/dev/null || true
  test -z "$second" || kill "$second" 2>/dev/null || true
  test -z "$first" || wait "$first" 2>/dev/null || true
  test -z "$second" || wait "$second" 2>/dev/null || true
}
trap cleanup EXIT

before=$(count_matching)
test "$before" -eq 0
bash -c 'exec -a "$0" sleep 10' "$module_name-worker-1" &
first=$!
bash -c 'exec -a "$0" sleep 10' "$module_name-worker-2" &
second=$!
after=0
for _ in $(seq 1 40); do
  after=$(count_matching)
  test "$after" -eq 2 && break
  sleep 0.05
done
printf '%s\n%s\n' "$before" "$after"
test "$after" -eq 2
"""
    result = subprocess.run(
        ["bash", "-c", fixture],
        text=True,
        capture_output=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout == "0\n2\n"


def test_cutover_sql_job_is_generation_bound_and_unknown_outcomes_stop() -> (
    None
):
    text = _read(CUTOVER)
    for required in (
        "EXPECTED_JOB_GENERATION",
        "EXPECTED_JOB_SA",
        "EXPECTED_JOB_IMAGE",
        "EXPECTED_DB_SECRET_VERSION",
        "attest_operation_sql",
        ".argument_contract_matches",
        ".task_contract_matches",
        ".retry_contract_matches",
        "any(.create_times[]; (. | epoch) >= ($t0 | epoch))",
        "any(.start_times[]; (. | epoch) >= ($t0 | epoch))",
        "json(metadata.name,metadata.creationTimestamp,metadata.labels)",
        "parent_job_matches",
        "creation_timestamp",
        "operation-unknown-after-t0-",
        'test "$(jq \'length\' "$candidates_after")" -eq 1',
        "outcome-unknown.txt",
        "UNSAFE_TO_VERIFY",
        "discovery_ambiguous",
        "execution_nonterminal",
        "immutable_inputs_changed",
        "safety_verify_rc=$?",
        "read-only-safety-verify.txt",
        "log-evidence-failure.txt",
        "return 75",
        "logs-first.projected.json",
        "logs-second.projected.json",
    ):
        assert required in text
    assert '--filter="metadata.creationTimestamp>=' not in text
    assert "mutation resubmission is forbidden" in text


def test_unknown_execution_discovery_persists_only_a_bounded_projection() -> (
    None
):
    text = _read(CUTOVER)
    wrapper = text[text.index("run_sid_operation() (") :]
    anchor = "if ! cloud_timeout 20 gcloud run jobs executions list"
    section = wrapper[wrapper.index(anchor) :]
    marker = '--arg expected_job_uri "$EXPECTED_JOB_URI" \'\n'
    start = section.index(marker) + len(marker)
    end = section.index('\' >"$candidates"; then', start)
    program = section[start:end]

    provider_payload = [
        {
            "metadata": {
                "name": "public-execution-1",
                "creationTimestamp": "2026-07-14T12:00:00Z",
                "labels": {"run.googleapis.com/job": "public-job"},
            },
            "spec": {
                "privateDatabaseHost": "must-not-survive",
                "privateNetwork": "must-not-survive-either",
            },
        }
    ]
    result = subprocess.run(
        [
            "jq",
            "-cS",
            "--arg",
            "expected_job",
            "public-job",
            "--arg",
            "expected_job_uri",
            "projects/p/locations/r/jobs/public-job",
            program,
        ],
        input=json.dumps(provider_payload),
        text=True,
        capture_output=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    projection = json.loads(result.stdout)
    assert projection == [
        {
            "canonical_uri": (
                "projects/p/locations/r/jobs/public-job/executions/"
                "public-execution-1"
            ),
            "creation_timestamp": "2026-07-14T12:00:00Z",
            "execution_name": "public-execution-1",
            "parent_job_matches": True,
        }
    ]
    persisted = json.dumps(projection)
    assert "must-not-survive" not in persisted
    assert "--limit=100 --format=json" not in wrapper
    assert '--format=json >"$candidates"' not in wrapper


def test_sql_artifacts_are_validated_before_entering_retained_evidence() -> (
    None
):
    text = _read(CUTOVER)
    start = text.index("attest_operation_sql() (")
    end = text.index("\n)\n\nattest_operation_job preflight", start)
    sql_attestation = text[start:end]
    ordered = (
        'expected_generation=$(jq -er --arg file "$file"',
        '"$sql_snapshot/$file.object-before-projection.json"',
        'gcloud storage cp "$object" "$download_dir/$file"',
        'sha256sum "$download_dir/$file"',
        '"$sql_snapshot/$file.object-after-projection.json"',
        'cmp -s "$sql_snapshot/$file.object-before-projection.json"',
        'install -m 600 "$download_dir/$file" "$sql_snapshot/$file"',
    )
    positions = [sql_attestation.index(value) for value in ordered]
    assert positions == sorted(positions)
    for required in (
        "download_dir=$(mktemp -d)",
        'chmod 700 "$download_dir"',
        "trap 'rm -rf -- \"$download_dir\"' EXIT",
        ".operation_sql_generations[$file]",
        ".operation_sql_sha256[$file]",
        "md5_hash:(.md5Hash // .md5_hash // null)",
        "object-*-projection.json",
    ):
        assert required in sql_attestation
    for forbidden in (
        '"$ARTIFACT_DIR/.operation-sql-',
        'gcloud storage cp "$object" "$sql_snapshot/$file"',
        ".object-before.json",
        ".object-after.json",
        "SQL_ATTEST_DIR",
    ):
        assert forbidden not in text


def test_unsafe_execution_identity_never_falls_through_to_safety_verify() -> (
    None
):
    text = _read(CUTOVER)
    wrapper_start = text.index("run_sid_operation() (")
    wrapper = text[
        wrapper_start : text.index(
            "\n)\n\nrun_sid_operation verify", wrapper_start
        )
    ]
    for reason in (
        "discovery_failed",
        "discovery_ambiguous",
        "execution_identity_invalid",
        "execution_describe_failed",
        "execution_attestation_failed",
        "execution_nonterminal",
        "immutable_inputs_changed",
    ):
        assert f"write_unsafe_to_verify {reason}" in wrapper
    assert wrapper.count("run_sid_operation verify") == 1
    safety_verify = wrapper.index("run_sid_operation verify")
    assert wrapper.rindex("write_unsafe_to_verify") < safety_verify
    assert re.search(
        r'if test "\$operation" != verify &&\s+'
        r'\{ test "\$terminal_outcome" = failed \|\| '
        r'test "\$unknown_outcome" -eq 1; \}; then',
        wrapper,
    )
    log_collection = wrapper.index("collect_operation_log_evidence")
    assert safety_verify < log_collection
    assert "no second execution was issued" in text


def test_terminal_classification_requires_one_recognized_completed_condition() -> (
    None
):
    text = _read(CUTOVER)
    anchor = "if ! terminal_outcome=$(jq -er '\n"
    section = text[text.index(anchor) :]
    start = len(anchor)
    end = section.index(
        '\' "$execution_dir/execution-projection.json"); then', start
    )
    program = section[start:end]

    cases = (
        ([{"state": "CONDITION_SUCCEEDED", "status": None}], 0, "success\n"),
        ([{"state": "CONDITION_FAILED", "status": None}], 0, "failed\n"),
        ([{"state": None, "status": "True"}], 0, "success\n"),
        ([{"state": None, "status": "False"}], 0, "failed\n"),
        ([{"state": "CONDITION_UNKNOWN", "status": "Unknown"}], 4, ""),
        (
            [
                {"state": "CONDITION_SUCCEEDED", "status": None},
                {"state": "CONDITION_UNKNOWN", "status": "Unknown"},
            ],
            4,
            "",
        ),
        (
            [
                {"state": "CONDITION_SUCCEEDED", "status": None},
                {"state": "CONDITION_FAILED", "status": None},
            ],
            4,
            "",
        ),
    )
    for completed_conditions, expected_rc, expected_stdout in cases:
        result = subprocess.run(
            ["jq", "-er", program],
            input=json.dumps({"completed_conditions": completed_conditions}),
            text=True,
            capture_output=True,
            check=False,
        )
        assert result.returncode == expected_rc, (
            completed_conditions,
            result.stderr,
        )
        assert result.stdout == expected_stdout


def test_cutover_uses_ga_short_ids_and_canonical_execution_identity() -> None:
    text = _read(CUTOVER)
    wrapper_start = text.index("run_sid_operation() (")
    wrapper = text[
        wrapper_start : text.index(
            "\n)\n\nrun_sid_operation verify", wrapper_start
        )
    ]
    for required in (
        'gcloud run jobs execute "$OP_JOB"',
        'gcloud run jobs executions list --job="$OP_JOB"',
        'gcloud run jobs executions describe "$execution_id"',
        'execution_uri="$EXPECTED_JOB_URI/executions/$execution_id"',
        ".execution_name == $execution",
        ".job_binding_matches",
        "create_times:[.metadata.creationTimestamp? // empty]",
        "start_times:[.status.startTime? // empty]",
        "completed_conditions:[.status.conditions[]?",
        "request-issued boundary",
        "EXPECTED_DB_NAME",
        '.env_names == ["DB_HOST","DB_NAME","DB_PORT","DB_USER","PGPASSWORD"]',
    ):
        assert required in wrapper
    assert 'gcloud run jobs execute "$EXPECTED_JOB_URI"' not in wrapper
    assert 'executions list --job="$EXPECTED_JOB_URI"' not in wrapper
    assert 'executions describe "$execution_uri"' not in wrapper
    assert 'gcloud run jobs describe "$EXPECTED_JOB_URI"' not in text
    assert wrapper.index("OP_T0=$(date -u +%Y-%m-%dT%H:%M:%SZ)") > (
        wrapper.index('attest_operation_sql "before-$operation-$OP_EPOCH"')
    )
    assert "execution_uid" not in wrapper
    assert "execution_generation" not in wrapper
    assert (
        "printf '%s' \"$EXPECTED_DB_SECRET_VERSION\" | grep -Eq '^[1-9][0-9]*$'"
    ) in text
    assert '"$execution_dir/execution.json"' not in wrapper
    assert "operation-job-$label.json" not in text

    failed_branch = wrapper.index('if test "$terminal_outcome" = failed; then')
    assert wrapper.index(
        'attest_operation_job "after-$operation-$OP_EPOCH"'
    ) < (failed_branch)
    assert wrapper.index('>"$execution_dir/execution.sha256"') < failed_branch
    safety_verify = wrapper.index(
        "run_sid_operation verify\n    safety_verify_rc=$?"
    )
    log_evidence = wrapper.index(
        'collect_operation_log_evidence "$execution_dir" "$execution_id"'
    )
    assert (
        wrapper.index('attest_operation_job "after-$operation-$OP_EPOCH"')
        < safety_verify
    )
    assert safety_verify < log_evidence < failed_branch
    assert 'test "$log_evidence_rc" -eq 0 || return 77' in wrapper


def test_cloud_run_attestations_accept_v1_and_v2_without_private_values() -> (
    None
):
    text = _read(CUTOVER)
    for required in (
        '.csi?.driver? == "gcsfuse.run.googleapis.com"',
        ".csi?.volumeAttributes?.bucketName? != null",
        '.annotations?["run.googleapis.com/network-interfaces"]?',
        ".networkInterfaces? // .network_interfaces?",
        "del(.commands)",
    ):
        assert text.count(required) == 2

    def extract_program(anchor: str, marker: str) -> str:
        section = text[text.index(anchor) :]
        start = section.index(marker) + len(marker)
        end = section.index("'); then", start)
        return section[start:end]

    source_program = extract_program(
        "if ! job_attestation_with_command=$(cloud_timeout 20",
        '--arg expected_subnetwork "$EXPECTED_SUBNETWORK" \'\n',
    )
    execution_program = extract_program(
        "if ! execution_attestation_with_command=$(cloud_timeout 20",
        '--argjson expected_args "$expected_args_json" \'\n',
    )

    (
        job_uri,
        service_account,
        image,
        db_host,
        db_name,
        secret,
        secret_version,
        bucket,
        network,
        subnetwork,
        command,
        arguments,
    ) = (
        "projects/public-p/locations/public-r/jobs/public-job",
        "private-operation-sa@example.invalid",
        "private-registry/image@sha256:" + ("a" * 64),
        "private-db-host.example.internal",
        "private-radio-database",
        "private-db-secret-id",
        "731947",
        "private-sql-bucket",
        "projects/private-p/global/networks/private-network",
        "projects/private-p/regions/private-r/subnetworks/private-subnet",
        ["python", "-m", "safe.operation"],
        ["bcfy-calls-sid-operation", "verify"],
    )
    execution_uri = f"{job_uri}/executions/public-execution"

    def provider_fixture(surface: str) -> dict[str, object]:
        annotations: dict[str, str] = {}
        spec_network: dict[str, object] = {}
        if surface == "v1":
            annotations["run.googleapis.com/network-interfaces"] = json.dumps(
                [{"network": network, "subnetwork": subnetwork}]
            )
            volume = {
                "name": "sql-files",
                "csi": {
                    "driver": "gcsfuse.run.googleapis.com",
                    "readOnly": True,
                    "volumeAttributes": {"bucketName": bucket},
                },
            }
        else:
            spec_network["vpcAccess"] = {
                "networkInterfaces": [
                    {"network": network, "subnetwork": subnetwork}
                ]
            }
            volume = {
                "name": "sql-files",
                "gcs": {"bucket": bucket, "readOnly": True},
            }

        return {
            "metadata": {
                "name": "public-execution",
                "uid": "public-job-uid",
                "generation": "3",
                "creationTimestamp": "2026-07-14T12:00:00Z",
                "labels": {"run.googleapis.com/job": "public-job"},
            },
            "spec": {
                "taskCount": 1,
                "template": {
                    "metadata": {"annotations": annotations},
                    "spec": {
                        "maxRetries": 0,
                        "timeoutSeconds": 300,
                        "serviceAccountName": service_account,
                        "containers": [
                            {
                                "image": image,
                                "command": command,
                                "args": arguments,
                                "env": [
                                    {"name": "DB_HOST", "value": db_host},
                                    {"name": "DB_NAME", "value": db_name},
                                    {"name": "DB_PORT", "value": "5432"},
                                    {"name": "DB_USER", "value": "postgres"},
                                    {
                                        "name": "PGPASSWORD",
                                        "valueFrom": {
                                            "secretKeyRef": {
                                                "name": secret,
                                                "key": secret_version,
                                            }
                                        },
                                    },
                                ],
                                "volumeMounts": [
                                    {"name": "sql-files", "mountPath": "/sql"}
                                ],
                            }
                        ],
                        "volumes": [volume],
                        **spec_network,
                    },
                },
            },
            "status": {
                "startTime": "2026-07-14T12:00:01Z",
                "conditions": [
                    {"type": "Completed", "state": "CONDITION_SUCCEEDED"}
                ],
            },
        }

    common_args = [
        "--arg",
        "expected_job_uri",
        job_uri,
        "--arg",
        "expected_service_account",
        service_account,
        "--arg",
        "expected_image",
        image,
        "--arg",
        "expected_db_host",
        db_host,
        "--arg",
        "expected_db_name",
        db_name,
        "--arg",
        "expected_secret",
        secret,
        "--arg",
        "expected_secret_version",
        secret_version,
        "--arg",
        "expected_sql_bucket",
        bucket,
        "--arg",
        "expected_network",
        network,
        "--arg",
        "expected_subnetwork",
        subnetwork,
    ]
    programs = (
        (source_program, common_args),
        (
            execution_program,
            [
                "--arg",
                "expected_execution_uri",
                execution_uri,
                "--arg",
                "expected_job",
                "public-job",
                *common_args,
                "--argjson",
                "expected_args",
                json.dumps(arguments),
            ],
        ),
    )
    private_values = (
        service_account,
        image,
        db_host,
        db_name,
        secret,
        secret_version,
        bucket,
        network,
        subnetwork,
    )
    for surface in ("v1", "v2"):
        fixture = provider_fixture(surface)
        for program, jq_args in programs:
            result = subprocess.run(
                ["jq", "-cS", *jq_args, program],
                input=json.dumps(fixture),
                text=True,
                capture_output=True,
                check=False,
            )
            assert result.returncode == 0, result.stderr
            projection = json.loads(result.stdout)
            for key in (
                "task_contract_matches",
                "retry_contract_matches",
                "timeout_contract_matches",
                "service_account_matches",
                "image_matches",
                "argument_contract_matches",
                "db_host_matches",
                "db_port_matches",
                "db_user_matches",
                "db_name_matches",
                "password_ref_matches",
                "gcs_volume_matches",
                "volume_mount_matches",
                "network_interface_matches",
            ):
                assert projection[key] is True, (surface, key, projection)
            assert projection["plaintext_password_count"] == 0
            assert projection["env_names"] == [
                "DB_HOST",
                "DB_NAME",
                "DB_PORT",
                "DB_USER",
                "PGPASSWORD",
            ]
            projection.pop("commands")
            persisted = json.dumps(projection, sort_keys=True)
            for private_value in private_values:
                assert private_value not in persisted

    # Independent network/subnetwork set membership is insufficient: no one
    # interface in this provider payload binds the reviewed pair.
    for surface in ("v1", "v2"):
        fixture = provider_fixture(surface)
        split_interfaces = [
            {"network": network, "subnetwork": "wrong-subnetwork"},
            {"network": "wrong-network", "subnetwork": subnetwork},
        ]
        if surface == "v1":
            fixture["spec"]["template"]["metadata"]["annotations"][
                "run.googleapis.com/network-interfaces"
            ] = json.dumps(split_interfaces)
        else:
            fixture["spec"]["template"]["spec"]["vpcAccess"][
                "networkInterfaces"
            ] = split_interfaces
        for program, jq_args in programs:
            result = subprocess.run(
                ["jq", "-cS", *jq_args, program],
                input=json.dumps(fixture),
                text=True,
                capture_output=True,
                check=False,
            )
            assert result.returncode == 0, result.stderr
            assert (
                json.loads(result.stdout)["network_interface_matches"] is False
            )


def test_poll_log_collection_uses_stable_closed_window_evidence() -> None:
    text = _read(CUTOVER)
    exact_instance_expression = (
        'map("resource.labels.instance_id=\\"" + . + "\\"")'
    )
    assert exact_instance_expression in text
    assert 'resource.labels.instance_id=\\\\\\"' not in text
    for required in (
        "LOG_INGESTION_GRACE_SECONDS=120",
        "LOG_STABILITY_GAP_SECONDS=30",
        "COLLECTION_NOT_BEFORE",
        "SAFE_FIRST=",
        "SAFE_SECOND=",
        "FIRST_COUNT=",
        "SECOND_COUNT=",
        "FIRST_IDENTITY_SHA256=",
        "SECOND_IDENTITY_SHA256=",
        'test "$FIRST_COUNT" = "$SECOND_COUNT"',
        'test "$FIRST_SHA256" = "$SECOND_SHA256"',
        'test "$FIRST_IDENTITY_SHA256" = "$SECOND_IDENTITY_SHA256"',
        "MIN_RECEIVE_TIMESTAMP=",
        "MAX_RECEIVE_TIMESTAMP=",
        "stability_read_count:2",
        "first_returned_count:$first_count",
        "second_returned_count:$second_count",
        "first_identity_sha256:$first_identity_sha256",
        "second_identity_sha256:$second_identity_sha256",
        "collection_complete:true",
    ):
        assert required in text
    assert re.search(
        r"later-discovered\s+matching entry invalidates acceptance evidence",
        text,
    )
    assert text.count('gcloud logging read "$POLL_FILTER"') == 2

    program_match = re.search(
        r"(?ms)INSTANCE_FILTER=\$\(jq -r '\n(?P<program>.*?)' \\\n\s*"
        r'"\$ARTIFACT_DIR/frozen-instances\.json"\)',
        text,
    )
    assert program_match is not None
    rendered = subprocess.run(
        ["jq", "-r", program_match.group("program")],
        input=('[{"numeric_instance_id":"123"},{"numeric_instance_id":"456"}]'),
        text=True,
        capture_output=True,
        check=False,
    )
    assert rendered.returncode == 0, rendered.stderr
    assert rendered.stdout.strip() == (
        'resource.labels.instance_id="123" OR resource.labels.instance_id="456"'
    )
    assert "\\" not in rendered.stdout


def test_complete_log_windows_are_closed_reusable_and_reducer_safe() -> None:
    text = _read(CUTOVER)
    section = _section(
        text, "Shared primitive F: complete bootstrap and soak evidence"
    )
    for required in (
        "collect_complete_log_window() (",
        'case "$KIND" in',
        "bootstrap)",
        "soak)",
        "*) return 64 ;;",
        "EVENT_FILTER='jsonPayload.event_type=\"bcfy_calls_sid_poll\"'",
        "bcfy_calls_replay_window_truncated",
        'test "$START_EPOCH" -lt "$END_EPOCH"',
        ".expected_sids",
        "length == 19",
        ". == (sort | unique)",
        "BCFY_CALLS_SID_POLL_REQUIRED_FIELDS",
        "BCFY_CALLS_SID_POLL_OPTIONAL_FIELDS",
        "BCFY_CALLS_REPLAY_WINDOW_TRUNCATED_REQUIRED_FIELDS",
        'project_log_read "$SAFE_TEMP_DIR/first.projected.json"',
        'project_log_read "$SAFE_TEMP_DIR/second.projected.json"',
        'test ! -e "$retained"',
        "Broadcastify Calls SID poll settled",
        "Broadcastify Calls replay window truncated",
        "($entry.logName | nonempty_string)",
        "logName:$entry.logName",
        "jsonPayload:($payload | del(.message))",
        "a bcfy_calls_missing_call payload and its signed audio_url are rejected",
        "start-started-epoch-$START_LABEL.txt",
        "last-start-completed-epoch-$START_LABEL.txt",
        "BOOTSTRAP_WINDOW_END_EPOCH=$((LAST_START_COMPLETED_EPOCH + 600))",
        "SOAK_START_EPOCH=$((BOOTSTRAP_END_EPOCH + 1))",
        "SOAK_END_EPOCH=$((SOAK_START_EPOCH + 1800))",
    ):
        assert required in section
    assert ': "${EVENT_FILTER' not in section
    assert section.count("collect_complete_log_window bootstrap") == 1
    assert section.count("collect_complete_log_window soak") == 1
    first_read = section.index('gcloud logging read "$POLL_FILTER"')
    first_projection = section.index(
        'project_log_read "$SAFE_TEMP_DIR/first.projected.json"'
    )
    first_install = section.index(
        'install -m 600 "$SAFE_TEMP_DIR/first.projected.json"'
    )
    assert first_read < first_projection < first_install
    assert not re.search(
        r'gcloud logging read "\$POLL_FILTER"[^|>]*>"\$ARTIFACT_DIR',
        section,
    )

    marker = '--argjson replay_fields "$REPLAY_FIELDS" \'\n'
    start = section.index(marker) + len(marker)
    end = section.index('\' >"$output"', start)
    program = section[start:end]
    fixture = [
        {
            "insertId": "safe-id",
            "timestamp": "2026-07-14T12:00:00Z",
            "receiveTimestamp": "2026-07-14T12:00:01Z",
            "logName": "projects/public/logs/ingestion",
            "resource": {
                "type": "gce_instance",
                "labels": {
                    "instance_id": "123",
                    "private_zone": "must-not-survive",
                },
            },
            "jsonPayload": {
                "event_type": "bcfy_calls_sid_poll",
                "schema_version": 1,
                "message": "Broadcastify Calls SID poll settled",
            },
            "privateProviderField": "must-not-survive",
        }
    ]
    args = [
        "jq",
        "-cS",
        "--arg",
        "kind",
        "bootstrap",
        "--argjson",
        "poll_fields",
        '["event_type","schema_version"]',
        "--argjson",
        "replay_fields",
        '["event_type","schema_version"]',
        program,
    ]
    accepted = subprocess.run(
        args,
        input=json.dumps(fixture),
        text=True,
        capture_output=True,
        check=False,
    )
    assert accepted.returncode == 0, accepted.stderr
    projected = json.loads(accepted.stdout)
    assert projected[0]["jsonPayload"] == {
        "event_type": "bcfy_calls_sid_poll",
        "schema_version": 1,
    }
    persisted = json.dumps(projected)
    assert "must-not-survive" not in persisted
    drifted = json.loads(json.dumps(fixture))
    drifted[0]["jsonPayload"]["unexpected"] = True
    rejected = subprocess.run(
        args,
        input=json.dumps(drifted),
        text=True,
        capture_output=True,
        check=False,
    )
    assert rejected.returncode != 0
    missing_log_name = json.loads(json.dumps(fixture))
    del missing_log_name[0]["logName"]
    rejected_identity = subprocess.run(
        args,
        input=json.dumps(missing_log_name),
        text=True,
        capture_output=True,
        check=False,
    )
    assert rejected_identity.returncode != 0


def test_startup_completion_evidence_is_exact_bound_and_secret_safe(
    tmp_path: pathlib.Path,
) -> None:
    text = _read(CUTOVER)
    section = _section(
        text, "Shared primitive F: complete bootstrap and soak evidence"
    )
    for required in (
        "collect_startup_completion_evidence() (",
        'case "$MODE" in sid_lease|legacy_feed)',
        ".startup_contract.profile_digests[$mode]",
        "STARTUP_RESULT_LIMIT=$((EXPECTED_SLOT_COUNT + 1))",
        "STARTUP_INGESTION_GRACE_SECONDS=120",
        "STARTUP_STABILITY_GAP_SECONDS=30",
        'jsonPayload.event_type="startup_pacing_complete"',
        "($entry.logName | nonempty_string)",
        '$payload.message == "Startup pacing complete"',
        "($payload | keys | sort) == (expected_payload_keys | sort)",
        '($payload.process_id | type == "number" and',
        "($payload.hostname | nonempty_string)",
        ".expected_numeric_instance_id == .live_numeric_instance_id",
        "(.expected_numeric_instance_id | tostring)",
        "$payload.profile_digest == $digest",
        "$payload.selected_domains == $contract.selected_domains",
        'test "$OBSERVED_SLOT_KEYS" = "$EXPECTED_SLOT_KEYS"',
        "([.[].jsonPayload.worker_id] | unique | length) == $expected",
        'START_PROOF_SHA256=$(sha256sum "$START_PROOF"',
        "start_proof_sha256:$start_proof_sha256",
        '"$START_PROOF" >"$HASHES"',
        'cmp -s "$SAFE_TEMP_DIR/first.canonical.json"',
        "collection_complete:true",
        "collect_startup_completion_evidence sid_lease",
    ):
        assert required in section
    assert section.count('gcloud logging read "$STARTUP_FILTER"') == 2
    assert "$payload.process_id == $slot.main_pid" not in section
    assert "$payload.hostname == $slot.instance_name" not in section
    assert section.index("collect_startup_completion_evidence sid_lease") < (
        section.index("scripts/operations/bcfy_calls_sid/bootstrap.jq")
    )

    marker = (
        '--argjson contract "$EXPECTED_CONTRACT" '
        '--slurpfile proof "$START_PROOF" \'\n'
    )
    start = section.index(marker) + len(marker)
    end = section.index('\' >"$output"', start)
    program = section[start:end]
    digest = "d" * 64
    contract = {
        "lease_admission_cycle_budget": 20,
        "max_feeds_per_worker": 800,
        "profile": "mixed-dormant",
        "profile_digests": {
            "legacy_feed": "e" * 64,
            "sid_lease": digest,
        },
        "selected_domains": ["feed", "sid"],
        "startup_jitter_max_sec": 2.0,
        "startup_stagger_max_sec": 60.0,
    }
    proof_path = tmp_path / "proof.json"
    proof_path.write_text(
        json.dumps(
            [
                {
                    "instance_name": "worker-a",
                    "expected_numeric_instance_id": "123",
                    "live_numeric_instance_id": "123",
                    "worker_index": 1,
                    "main_pid": 4321,
                }
            ]
        ),
        encoding="utf-8",
    )
    fixture = [
        {
            "insertId": "startup-id",
            "timestamp": "2026-07-14T12:00:00Z",
            "receiveTimestamp": "2026-07-14T12:00:01Z",
            "logName": "projects/public/logs/ingestion",
            "resource": {
                "type": "gce_instance",
                "labels": {
                    "instance_id": "123",
                    "private_zone": "must-not-survive",
                },
            },
            "jsonPayload": {
                "bcfy_calls_authority_mode": "sid_lease",
                "deterministic_delay_sec": 1.0,
                "event_type": "startup_pacing_complete",
                "hostname": "worker-a",
                "lease_admission_cycle_budget": 20,
                "max_feeds_per_worker": 800,
                "message": "Startup pacing complete",
                "process_id": 4321,
                "profile": "mixed-dormant",
                "profile_digest": digest,
                "random_delay_sec": 0.5,
                "selected_domains": ["feed", "sid"],
                "startup_jitter_max_sec": 2.0,
                "startup_stagger_max_sec": 60.0,
                "total_delay_sec": 1.5,
                "worker_id": "12345678-1234-4234-8234-123456789abc",
                "worker_index": 1,
            },
            "privateProviderField": "must-not-survive",
        }
    ]
    args = [
        "jq",
        "-cS",
        "--arg",
        "mode",
        "sid_lease",
        "--arg",
        "digest",
        digest,
        "--argjson",
        "contract",
        json.dumps(contract),
        "--slurpfile",
        "proof",
        str(proof_path),
        program,
    ]
    accepted = subprocess.run(
        args,
        input=json.dumps(fixture),
        text=True,
        capture_output=True,
        check=False,
    )
    assert accepted.returncode == 0, accepted.stderr
    projected = json.loads(accepted.stdout)
    assert projected[0]["jsonPayload"]["event_type"] == (
        "startup_pacing_complete"
    )
    assert "message" not in projected[0]["jsonPayload"]
    assert "must-not-survive" not in accepted.stdout

    for mutation in ("unexpected_key", "invalid_pid", "missing_log_name"):
        rejected_fixture = json.loads(json.dumps(fixture))
        if mutation == "unexpected_key":
            rejected_fixture[0]["jsonPayload"]["unexpected"] = True
        elif mutation == "invalid_pid":
            rejected_fixture[0]["jsonPayload"]["process_id"] = 0
        else:
            del rejected_fixture[0]["logName"]
        rejected = subprocess.run(
            args,
            input=json.dumps(rejected_fixture),
            text=True,
            capture_output=True,
            check=False,
        )
        assert rejected.returncode != 0, mutation


def test_control_capture_and_saved_plan_apply_are_bounded_and_exact() -> None:
    text = _read(CUTOVER)
    controls = _section(
        text, "Shared primitive 0: capture and compare durable cutover controls"
    )
    apply = _section(
        text, "Shared primitive 00: saved-plan ingestion-tuple reconcile"
    )
    for required in (
        "capture_cutover_controls() (",
        "name,selfLink,targetSize",
        "versions:",
        "instanceTemplate:",
        "updatePolicy:",
        "autoHealingPolicies:",
        "instanceLifecyclePolicy:",
        "status:",
        "autoscaler:(.autoscaler // null)",
        "numeric_instance_id:$id",
        "value_sha256",
        "hashlib.sha256(value.encode()).hexdigest()",
        "workflow-$workflow.json",
        "capture_no_active_workflow_runs() (",
        "nonterminal-workflow-runs.json",
        "controls.sha256",
        "compare_cutover_controls() (",
        "temp=$(mktemp -d)",
        "status_autoscaler:.status.autoscaler",
    ):
        assert required in controls
    assert "--json name,value --limit" not in controls
    assert "value:(if ($found" not in controls
    assert '>"$before/' not in controls
    assert '>"$after/' not in controls

    for required in (
        ".deployment_ref",
        "git/ref/tags/$DEPLOYMENT_REF",
        "commits/$DEPLOYMENT_REF",
        'test "$resolved_ref" = "$DEPLOYMENT_COMMIT"',
        "nonterminal-before-mutation.json",
        "INGESTION_MAINTENANCE_MODE",
        "INGESTION_CONTAINER_IMAGE",
        "BCFY_CALLS_AUTHORITY_MODE",
        "invocation-projection.json",
        "suppress_app_deploy:true",
        "--raw-field suppress_app_deploy=true",
        '--event workflow_dispatch --commit "$DEPLOYMENT_COMMIT"',
        "infra-before.json",
        "infra-new.json",
        "length == 1",
        ".headSha == $sha",
        '.workflowName == "Deploy Infrastructure"',
        'gh run watch "$RUN_ID"',
        "Trigger App Deploy Workflow",
        '["skipped"]',
        'gh run download "$RUN_ID"',
        '--name tfplan-prod --dir "$temp/plan"',
        "terraform_plan_sha256",
        "plan-pin-projection.json",
        'cmp -s "$temp/app-before.json" "$temp/app-after.json"',
    ):
        assert required in apply
    assert not re.search(r"(?m)^  gh (?:api|run|variable|workflow) ", apply)
    assert 'install -m 600 "${plan_files[0]}"' not in apply
    assert 'cp "${plan_files[0]}"' not in apply
    no_active = text[
        text.index("capture_no_active_workflow_runs() (") : text.index(
            "\n)\n\ncapture_cutover_controls() ("
        )
    ]
    for status in (
        "queued",
        "in_progress",
        "requested",
        "waiting",
        "action_required",
    ):
        assert status in no_active
    assert '--status "$status" --limit 1' in no_active
    assert "pending" not in no_active
    assert controls.count("capture_no_active_workflow_runs") >= 2


def test_rollback_uses_known_inert_template_and_two_stage_legacy_reconcile() -> (
    None
):
    text = _read(CUTOVER)
    rollback = _section(text, "ROLLBACK_NO_AUTHORITY")
    restored = _section(text, "LEGACY_RESTORED")
    for required in (
        "tuple-apply-sid-durable",
        "LAST_DURABLE_MODE=sid_lease",
        "tuple-apply-frozen",
        "LAST_DURABLE_MODE=legacy_feed",
        ".cutover_contract.authority_mode == $mode",
        ".cutover_contract.maintenance_mode == true",
        ".present == false",
        'defaultActionOnFailure == "DO_NOTHING"',
        'all(.[]; .current_action == "NONE")',
        "TARGET_MODE=legacy_feed",
        "Do not call",
    ):
        assert required in rollback
    assert "replacement template remains" not in rollback
    accepted_reconcile = rollback.index(
        "set_and_apply_ingestion_tuple sid_lease true"
    )
    capture = rollback.index("capture_cutover_controls rollback-entry")
    stage = rollback.index("\nTARGET_MODE=legacy_feed\n")
    assert accepted_reconcile < capture < stage
    assert not re.search(
        r"(?m)^\s*set_and_apply_ingestion_tuple ", rollback[stage:]
    )
    assert 'test -s "$ACCEPTED_PLAN" && test -s "$ACCEPTED_CONTROLS"' in (
        rollback
    )
    assert rollback.index("tuple-apply-rollback-frozen") < rollback.index(
        "tuple-apply-sid-durable"
    )
    legacy_start = restored.index("start_same_frozen_slots legacy_feed")
    durable_legacy = restored.index(
        "set_and_apply_ingestion_tuple legacy_feed true"
    )
    restore_controls = restored.index(
        "set_and_apply_ingestion_tuple\nlegacy_feed false"
    )
    assert legacy_start < durable_legacy < restore_controls
    assert "same live legacy PIDs" in restored
    assert "template-driven replacement or\naction blocks" in restored


def test_pitr_gate_zero_reflects_numeric_job_pins_without_claiming_disable() -> (
    None
):
    text = _read(PITR)
    for job in ("bcfy_calls_sid_operation", "schema_migration"):
        row = next(line for line in text.splitlines() if f"`{job}`" in line)
        assert "numeric Secret Manager version is pinned" in row
        assert "no durable execution-disable sentinel" in row
        assert "secret reference is `latest`" not in row


def test_evidence_template_mirrors_load_bearing_artifacts_and_pitr_gate_zero() -> (
    None
):
    text = _read(EVIDENCE)
    for required in (
        "signed input manifest",
        "full MIG URI",
        "full autoscaler URI",
        "full operation Job URI",
        "saved Terraform/config proof that schema/operation Jobs share the immutable image digest",
        "operation Job identity/generation and match-only bounded attestation",
        "no endpoint/network/secret values",
        "four object generations/content hashes",
        "ControlPID",
        "unit-cgroup process count",
        "legacy T+90 host-local slot snapshot",
        "legacy exact force completion at/before T+100",
        "legacy T+120-or-earlier final complete slot proof",
        "first/second reducer-safe projected-population object IDs",
        "identity-set hashes",
        "receiveTimestamp min/max",
        "Gate 0 status",
        "eight-consumer URI/template/invoker inventory",
        "zero uncatalogued database consumers",
        "INTERNAL_AUTOMATION` pg_cron",
        "immutable numeric MIG worker-secret binding",
        "backup/deletion-protection/labels/pooling/Query-Insights/IaC parity-delta matrix",
        "restored admin surface attestation before any client probe",
        "migration `019` scheduling fence",
        "collector MIG raw-password Gate 0 blocker cleared",
        "controlled Job before/after full URI/UID/generation/template hashes (source Job only)",
        "validated short Job/execution IDs, canonical execution URI, v1 metadata/status projection",
        "signed canonical 004 comparator artifact/hash/exit-zero result",
        "byte-equal signed manifest digest",
        "UNSAFE_TO_VERIFY",
    ):
        assert required in text
    assert re.search(r"exact override\s+argument array", text)


def test_live_job_attestation_is_scoped_to_the_operation_job() -> None:
    cutover = _read(CUTOVER)
    evidence = _read(EVIDENCE)
    combined = f"{cutover}\n{evidence}"
    assert cutover.count('gcloud run jobs describe "$OP_JOB"') == 1
    assert "live operation-Job identity/IAM/generation" in cutover
    assert re.search(
        r"saved Terraform/config proof that both SQL Jobs use\s+those pins",
        cutover,
    )
    assert (
        "saved Terraform/config proof that schema/operation Jobs share"
        in evidence
    )
    for unsupported_claim in (
        "both Job generations",
        "schema-migration and operation-Job identity/IAM/generation",
        "schema migration Job identity/generation and match-only bounded attestation",
    ):
        assert unsupported_claim not in combined


def test_operation_logs_are_projected_before_retention() -> None:
    text = _read(CUTOVER)
    section = text[
        text.index("collect_operation_log_evidence() (") : text.index(
            "write_unsafe_to_verify()"
        )
    ]
    assert "safe_temp=$(mktemp -d)" in section
    assert "chmod 700" in section
    assert "project_operation_log_read" in section
    assert "run.googleapis.com%2Fstdout" in section
    assert "unsafe or schema-drifted SQL stdout entry" in section
    assert "logs-first.projected.json" in section
    assert "logs-second.projected.json" in section
    assert ">$execution_dir/logs-first.json" not in section
    assert ">$execution_dir/logs-second.json" not in section
    assert "logs-first.canonical.json" not in text
    assert "logs-second.canonical.json" not in text


def test_operation_log_projector_rejects_secret_bearing_stdout() -> None:
    text = _read(CUTOVER)
    begin = text.index("project_operation_log_read() {")
    section = text[
        begin : text.index("cloud_timeout 30 gcloud logging read", begin)
    ]
    marker = 'jq -cS --arg stdout_log "$stdout_log" \'\n'
    start = section.index(marker) + len(marker)
    end = section.index('\' >"$output"', start)
    program = section[start:end]
    log_name = "projects/public/logs/run.googleapis.com%2Fstdout"
    safe = [
        {
            "insertId": "a",
            "timestamp": "2026-07-14T12:00:00Z",
            "severity": "INFO",
            "logName": log_name,
            "textPayload": " sid_count | feed_count 19 | 154 ",
        }
    ]
    result = subprocess.run(
        ["jq", "-cS", "--arg", "stdout_log", log_name, program],
        input=json.dumps(safe),
        text=True,
        capture_output=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == safe

    unsafe = [{**safe[0], "textPayload": "https://signed.invalid/?token=x"}]
    result = subprocess.run(
        ["jq", "-cS", "--arg", "stdout_log", log_name, program],
        input=json.dumps(unsafe),
        text=True,
        capture_output=True,
        check=False,
    )
    assert result.returncode != 0


def test_public_reducers_are_bound_to_clean_signed_checkout() -> None:
    text = _read(CUTOVER)
    for required in (
        "PUBLIC_REPO_ROOT",
        'git -C "$PUBLIC_REPO_ROOT" rev-parse HEAD',
        "status --porcelain",
        "--untracked-files=all",
        "public_runtime_sha256.slo_contract",
        "public_runtime_sha256.bootstrap_reducer",
        "public_runtime_sha256.soak_reducer",
        'PYTHONPATH="$PUBLIC_REPO_ROOT"',
        "python3 -P",
        "contract.__file__",
        '-f "$PUBLIC_REPO_ROOT/scripts/operations/bcfy_calls_sid/bootstrap.jq"',
        '-f "$PUBLIC_REPO_ROOT/scripts/operations/bcfy_calls_sid/soak.jq"',
    ):
        assert required in text


def test_saved_plan_distinguishes_optional_override_from_rendered_endpoint() -> (
    None
):
    text = _read(CUTOVER)
    section = text[
        text.index("set_and_apply_ingestion_tuple() (") : text.index(
            "## Shared primitive A"
        )
    ]
    for required in (
        "ENDPOINT_OVERRIDE_SHA256",
        "PLANNED_ENDPOINT_SHA256",
        "operation_job_db_host",
        "active_alloydb_primary_instance_ip_override_sha256 ==",
        "$override_sha",
        "planned_active_alloydb_primary_instance_ip_sha256 ==",
        "$planned_sha",
    ):
        assert required in section
    assert "--arg endpoint_sha" not in section


def test_safe_python_path_ignores_shadow_slo_contract(
    tmp_path: pathlib.Path,
) -> None:
    shadow = tmp_path / "backend/pipeline/ingestion"
    shadow.mkdir(parents=True)
    for package in (shadow.parents[1], shadow.parent, shadow):
        (package / "__init__.py").write_text("", encoding="utf-8")
    (shadow / "slo_contract.py").write_text(
        "raise RuntimeError('shadow module imported')\n", encoding="utf-8"
    )
    result = subprocess.run(
        [
            "python3",
            "-P",
            "-c",
            (
                "from backend.pipeline.ingestion import slo_contract; "
                "print(slo_contract.__file__)"
            ),
        ],
        cwd=tmp_path,
        env={**os.environ, "PYTHONPATH": str(ROOT)},
        text=True,
        capture_output=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert (
        pathlib.Path(result.stdout.strip()).resolve()
        == (ROOT / "backend/pipeline/ingestion/slo_contract.py").resolve()
    )


def test_live_template_runtime_agreement_precedes_control_restoration() -> None:
    text = _read(CUTOVER)
    primitive = text[
        text.index("capture_live_template_runtime_agreement() (") : text.index(
            "## Shared primitive F"
        )
    ]
    for required in (
        "instance-templates describe",
        "/global/instanceTemplates/",
        "--global --format=json",
        'metadata", {}).get("items"',
        "template-projection.json",
        "alloydb_host_sha256",
        "planned_active_alloydb_primary_instance_ip_sha256",
        "target-may-change",
        "baseline-runtime-identity.json",
        "current-runtime-identity.json",
    ):
        assert required in primitive
    assert "user-data" in primitive
    assert 'install -m 600 "$temp/template-projection.json"' in primitive

    sid = _section(text, "SID_DURABLE")
    accepted = _section(text, "ACCEPTED")
    assert "capture_live_template_runtime_agreement sid-durable" in sid
    assert sid.index("capture_cutover_controls sid-durable") < sid.index(
        "capture_live_template_runtime_agreement sid-durable"
    )
    assert "set_and_apply_ingestion_tuple sid_lease false" in accepted

    legacy = _section(text, "LEGACY_RESTORED")
    code = legacy[legacy.index("```bash") :]
    assert code.index("collect_startup_completion_evidence legacy_feed") < (
        code.index("set_and_apply_ingestion_tuple legacy_feed true")
    )
    assert code.index(
        "capture_live_template_runtime_agreement legacy-durable"
    ) < (code.index("set_and_apply_ingestion_tuple legacy_feed false"))


def test_live_template_projector_retains_only_endpoint_hash() -> None:
    text = _read(CUTOVER)
    begin = text.index("capture_live_template_runtime_agreement() (")
    section = text[begin : text.index("## Shared primitive F", begin)]
    marker = "    python3 -c '\n"
    start = section.index(marker) + len(marker)
    end = section.index('\' >"$temp/template-projection.json"', start)
    program = section[start:end]
    digest = "d" * 64
    image = f"us-docker.pkg.dev/public/repository/worker@sha256:{digest}"
    template_name = "ingestion-prod-reviewed"
    template_uri = (
        "https://www.googleapis.com/compute/v1/projects/public/global/"
        f"instanceTemplates/{template_name}"
    )
    endpoint = "10.20.30.40"
    user_data = f"""#cloud-config
write_files:
- path: /etc/container-env/icecast-collector-prod.env
  permissions: \"0600\"
  content: |
    WORKER_PROFILE=mixed-dormant
    BCFY_CALLS_AUTHORITY_MODE=sid_lease
    ALLOYDB_HOST={endpoint}
- path: /etc/systemd/system/icecast-collector-prod@.service
  permissions: \"0644\"
  content: |
    ExecStartPre=/usr/bin/docker pull {image}
    ExecStart=/usr/bin/docker run {image}
"""
    source = {
        "name": template_name,
        "selfLink": template_uri,
        "properties": {
            "metadata": {"items": [{"key": "user-data", "value": user_data}]}
        },
    }
    result = subprocess.run(
        ["python3", "-c", program],
        input=json.dumps(source),
        env={
            **os.environ,
            "EXPECTED_TEMPLATE_URI": template_uri,
            "EXPECTED_TEMPLATE_NAME": template_name,
            "EXPECTED_MODE": "sid_lease",
            "EXPECTED_IMAGE": image,
            "EXPECTED_SERVICE": "icecast-collector-prod",
        },
        text=True,
        capture_output=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    projection = json.loads(result.stdout)
    assert endpoint not in result.stdout
    assert (
        projection["alloydb_host_sha256"]
        == hashlib.sha256(endpoint.encode()).hexdigest()
    )
    assert projection["template_uri"] == template_uri


def test_all_cutover_bash_fences_are_syntax_valid() -> None:
    text = _read(CUTOVER)
    blocks = re.findall(r"(?ms)^```bash\n(.*?)^```$", text)
    assert len(blocks) >= 10
    for index, block in enumerate(blocks, start=1):
        result = subprocess.run(
            ["bash", "-n"],
            input=block,
            text=True,
            capture_output=True,
            check=False,
        )
        assert result.returncode == 0, f"bash fence {index}: {result.stderr}"
