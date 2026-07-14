"""Static safety contract for the Phase 7 operator runbooks.

These tests intentionally validate the load-bearing state-machine vocabulary and
the accepted evidence boundary.  They do not claim that a production cutover or
an AlloyDB restore has been executed.
"""

from __future__ import annotations

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
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


def _read(path: Path) -> str:
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
        "--default-action-on-vm-failure=do-nothing",
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
    assert text.index("Execute `002_activate.sql`") > text.index("## NO_AUTHORITY\n")
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
    assert not re.search(r"(?i)delete\s+from\s+(?:public\.)?ingestion_leases", text)
    assert not re.search(r"(?i)truncate\s+(?:table\s+)?(?:public\.)?ingestion_leases", text)


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
        "raw-log object ID",
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
    ):
        assert required in text
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
    ):
        assert injection in combined
    assert combined.count("Independent review") >= 2
    assert "no unmitigated HIGH" in combined


def test_accepted_contract_amendments_are_explicit_but_pending() -> None:
    requirements = _read(REQUIREMENTS)
    roadmap = _read(ROADMAP)
    state = _read(STATE)
    validation = _read(VALIDATION)
    combined = "\n".join((requirements, roadmap, state, validation))

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
        assert required in combined

    for requirement in ("POLL-07", "OPER-06", "ROLL-07", "ROLL-08"):
        assert re.search(rf"- \[ \] \*\*{requirement}\*\*", requirements)
        assert re.search(rf"\| {requirement} \| Phase 7 \| Pending \|", requirements)


def test_documents_do_not_preclaim_live_actions() -> None:
    combined = "\n".join(
        (_read(CUTOVER), _read(PITR), _read(EVIDENCE), _read(VALIDATION))
    )
    assert "NO PRODUCTION CUTOVER PERFORMED" in combined
    assert "PITR REVIEWED — NOT EXERCISED" in combined
    for false_claim in (
        "production cutover passed",
        "production soak passed",
        "live pitr passed",
        "pitr exercise passed",
    ):
        assert false_claim not in combined.lower()
