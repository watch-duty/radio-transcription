"""Static contracts for the manual Broadcastify Calls SID handoff."""

from __future__ import annotations

import pathlib
import re
import unittest

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[4]
_ALLOYDB_ROOT = _REPO_ROOT / "terraform/modules/alloydb"
_OPERATIONS_ROOT = _ALLOYDB_ROOT / "sql/operations/bcfy_calls_sid"


def _sql_without_comments(path: pathlib.Path) -> str:
    """Return normalized lowercase SQL with line comments removed."""
    uncommented = "\n".join(
        line
        for line in path.read_text().splitlines()
        if not line.lstrip().startswith("--")
    )
    return " ".join(uncommented.split()).lower()


class TestBcfyCallsSidCutoverSqlContract(unittest.TestCase):
    """Pins the fail-closed boundary of the three manual operations."""

    def setUp(self) -> None:
        self.preseed = _sql_without_comments(
            _OPERATIONS_ROOT / "001_preseed.sql"
        )
        self.activate = _sql_without_comments(
            _OPERATIONS_ROOT / "002_activate.sql"
        )
        self.rollback = _sql_without_comments(
            _OPERATIONS_ROOT / "003_rollback.sql"
        )

    def test_operations_are_outside_automatic_migration_glob(self) -> None:
        self.assertEqual(
            sorted(path.name for path in _OPERATIONS_ROOT.glob("*.sql")),
            [
                "001_preseed.sql",
                "002_activate.sql",
                "003_rollback.sql",
            ],
        )

        main_tf = (_ALLOYDB_ROOT / "main.tf").read_text()
        self.assertIn(
            'fileset("${path.module}/sql/ingestion", "*.sql")',
            main_tf,
        )
        self.assertNotIn(
            'fileset("${path.module}/sql/operations",',
            main_tf,
        )

    def test_every_operation_is_bounded_and_transactional(self) -> None:
        for operation in (self.preseed, self.activate, self.rollback):
            with self.subTest(operation=operation[:40]):
                self.assertIn("begin;", operation)
                self.assertIn("commit;", operation)
                self.assertIn("set local lock_timeout", operation)
                self.assertIn("set local statement_timeout", operation)
                self.assertIn("$preflight$", operation)
                self.assertIn("$postflight$", operation)

        combined = f"{self.preseed} {self.activate} {self.rollback}"
        for forbidden in (
            "reviewed_sid_count",
            "manifest_digest",
            "sha256",
            "live_probe",
            "split_part",
        ):
            self.assertNotIn(forbidden, combined)
        self.assertNotRegex(combined, r"\b19\b|\b154\b")

    def test_all_operations_fail_closed_on_membership_shape(self) -> None:
        for operation in (self.preseed, self.activate, self.rollback):
            with self.subTest(operation=operation[:40]):
                self.assertIn(
                    "left join public.feed_properties as properties",
                    operation,
                )
                self.assertIn(
                    "properties.source_feed_id !~ '^[0-9]+-[0-9]+$'",
                    operation,
                )
                self.assertIn(
                    "properties.bcfy_calls_sid || '-' || "
                    "properties.bcfy_calls_group_id",
                    operation,
                )
                self.assertIn(
                    "properties.bcfy_calls_is_trunked is distinct from true",
                    operation,
                )
                self.assertRegex(
                    operation,
                    re.compile(
                        r"group by sid, group_id having count\(\*\) > 1"
                    ),
                )

    def test_preseed_only_inserts_dormant_parent_identities(self) -> None:
        self.assertIn("insert into public.ingestion_leases", self.preseed)
        self.assertIn(
            "on conflict (source_type, lease_key) do nothing", self.preseed
        )
        self.assertIn("'deactivated'::public.feed_status", self.preseed)
        self.assertNotIn("update public.feeds", self.preseed)
        self.assertNotIn("update public.ingestion_leases", self.preseed)
        self.assertNotIn("delete from", self.preseed)

    def test_activation_is_confirmed_locked_and_fence_dominating(self) -> None:
        self.assertIn("process_absence_confirmed", self.activate)
        self.assertIn("<> 'confirmed'", self.activate)
        self.assertRegex(
            self.activate,
            re.compile(
                r"lock table public\.ingestion_leases.*"
                r"lock table public\.feeds.*"
                r"lock table public\.feed_properties"
            ),
        )
        self.assertIn("worker_id is not null", self.activate)
        self.assertIn("9223372036854775807", self.activate)
        self.assertIn("greatest(", self.activate)
        self.assertIn("maximum_child_fence + 1", self.activate)
        self.assertIn(
            "membership_revision = leases.membership_revision + 1",
            self.activate,
        )
        for status in ("unclaimed", "active", "failing", "deactivated"):
            self.assertIn(f"'{status}'", self.activate)
        self.assertIn("else feeds.status", self.activate)

    def test_rollback_advances_parent_then_all_child_fences(self) -> None:
        self.assertIn("process_absence_confirmed", self.rollback)
        self.assertIn("<> 'confirmed'", self.rollback)
        self.assertRegex(
            self.rollback,
            re.compile(
                r"lock table public\.ingestion_leases.*"
                r"lock table public\.feeds.*"
                r"lock table public\.feed_properties"
            ),
        )

        parent_update = self.rollback.index(
            "update public.ingestion_leases as leases"
        )
        child_update = self.rollback.index("update public.feeds as feeds")
        self.assertLess(parent_update, child_update)
        preflight = self.rollback[:parent_update]
        self.assertNotIn("leases.worker_id is not null", preflight)
        self.assertNotIn("leases.last_heartbeat is not null", preflight)
        parent_mutation = self.rollback[parent_update:child_update]
        self.assertIn("worker_id = null", parent_mutation)
        self.assertIn("last_heartbeat = null", parent_mutation)
        self.assertIn(
            "fencing_token = leases.fencing_token + 1",
            self.rollback,
        )
        self.assertIn(
            "membership_revision = leases.membership_revision + 1",
            self.rollback,
        )
        self.assertIn("new_parent_fence + 1", self.rollback)
        self.assertIn("greatest(", self.rollback)
        self.assertIn("'active'::public.feed_status", self.rollback)
        self.assertIn("'unclaimed'::public.feed_status", self.rollback)
        self.assertIn("'deactivated'::public.feed_status", self.rollback)
        self.assertIn("9223372036854775805", self.rollback)
        self.assertIn("9223372036854775807", self.rollback)


if __name__ == "__main__":
    unittest.main()
