"""Contracts for the manual Broadcastify Calls SID handoff SQL."""

from __future__ import annotations

import pathlib
import unittest

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[4]
_OPERATIONS = (
    _REPO_ROOT / "terraform/modules/alloydb/sql/operations/bcfy_calls_sid"
)


def _normalized_sql(path: pathlib.Path) -> str:
    """Return an operation without comments or insignificant whitespace."""
    return " ".join(
        line
        for line in path.read_text().splitlines()
        if not line.lstrip().startswith("--")
    ).lower()


class TestBcfyCallsSidCutoverSqlContract(unittest.TestCase):
    """Protect reviewed-manifest, all-Calls SID authority handoffs."""

    def test_review_manifest_is_read_only_and_reports_both_handoff_sets(
        self,
    ) -> None:
        """Fail if operators cannot derive the reviewed SQL inputs safely."""
        sql = _normalized_sql(_OPERATIONS / "000_review_manifest.sql")

        self.assertIn(
            "begin transaction isolation level repeatable read read only;",
            sql,
        )
        self.assertIn("full_calls_membership", sql)
        self.assertIn("eligible_calls_membership", sql)
        self.assertIn("manifest_scope", sql)
        self.assertIn("full", sql)
        self.assertIn("eligible", sql)
        for mutation in (
            "insert into public.",
            "update public.",
            "delete from public.",
            "truncate public.",
        ):
            self.assertNotIn(mutation, sql)

    def test_backfill_requires_a_reviewed_full_calls_manifest(self) -> None:
        """Fail if a future backfill can infer or mutate an unreviewed set."""
        sql = _normalized_sql(_OPERATIONS / "000_backfill_membership.sql")

        for variable in (
            "reviewed_total_feed_count",
            "reviewed_total_sid_count",
            "reviewed_total_manifest_digest",
            "backfill_confirmed",
        ):
            self.assertIn(variable, sql)
        self.assertIn("begin;", sql)
        self.assertIn("commit;", sql)
        self.assertIn("set local lock_timeout", sql)
        self.assertIn("set local statement_timeout", sql)
        self.assertIn("split_part(properties.source_feed_id", sql)
        self.assertIn("bcfy_calls_is_trunked = true", sql)
        self.assertIn("reviewed full calls membership changed", sql)

    def test_authority_operations_are_bound_to_reviewed_eligible_manifest(
        self,
    ) -> None:
        """Fail if stale fixed cardinalities can activate a changed fleet."""
        for operation in (
            "001_preseed.sql",
            "002_activate.sql",
            "003_rollback_children.sql",
            "004_verify.sql",
        ):
            with self.subTest(operation=operation):
                sql = _normalized_sql(_OPERATIONS / operation)
                self.assertIn("reviewed_eligible_feed_count", sql)
                self.assertIn("reviewed_eligible_sid_count", sql)
                self.assertIn("reviewed_eligible_manifest_digest", sql)
                self.assertNotIn("expected 19 sids/154 feeds", sql)
                self.assertNotIn("<> 19", sql)
                self.assertNotIn("<> 154", sql)

    def test_only_review_inputs_can_parse_legacy_source_ids(self) -> None:
        """Fail if authority handoff starts deriving membership implicitly."""
        for operation in (
            "001_preseed.sql",
            "002_activate.sql",
            "003_rollback_children.sql",
            "004_verify.sql",
        ):
            with self.subTest(operation=operation):
                self.assertNotIn(
                    "split_part",
                    _normalized_sql(_OPERATIONS / operation),
                )

    def test_verify_uses_no_persistent_table_mutations(self) -> None:
        """Fail if verification becomes writable beyond its temporary workspace."""
        sql = _normalized_sql(_OPERATIONS / "004_verify.sql")

        self.assertNotIn("read only", sql)
        for mutation in (
            "insert into public.",
            "update public.",
            "delete from public.",
            "truncate public.",
        ):
            self.assertNotIn(mutation, sql)


if __name__ == "__main__":
    unittest.main()
