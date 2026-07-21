from __future__ import annotations

import dataclasses
import inspect
import typing
import unittest

from backend.pipeline.ingestion import grant_control, worker_profiles


class TestWorkerProfile(unittest.TestCase):
    """Behavioral tests for immutable worker topology and capacity."""

    def test_values_are_deeply_immutable_and_minimal(self) -> None:
        profile = worker_profiles.MIXED_DORMANT_PROFILE
        mutable_profile = typing.cast("typing.Any", profile)
        mutable_allocation = typing.cast("typing.Any", profile.allocations[0])
        mutable_presets = typing.cast(
            "typing.Any",
            worker_profiles.WORKER_PROFILE_PRESETS,
        )

        with self.assertRaises(dataclasses.FrozenInstanceError):
            mutable_profile.name = "changed"
        with self.assertRaises(dataclasses.FrozenInstanceError):
            mutable_allocation.owned_cap = 1
        with self.assertRaises(TypeError):
            mutable_presets["changed"] = profile

        self.assertIsInstance(profile.allocations, tuple)
        self.assertEqual(
            tuple(field.name for field in dataclasses.fields(profile)),
            ("name", "allocations"),
        )

    def test_presets_have_exact_domains_caps_budgets_and_claim_flags(
        self,
    ) -> None:
        legacy = worker_profiles.LEGACY_PROFILE
        mixed = worker_profiles.MIXED_DORMANT_PROFILE
        sid_only = worker_profiles.SID_DORMANT_PROFILE

        self.assertEqual(
            set(worker_profiles.WORKER_PROFILE_PRESETS),
            {"legacy", "mixed-dormant", "sid-dormant"},
        )
        self.assertEqual(
            [allocation.domain_id for allocation in legacy.allocations],
            [grant_control.DomainId.FEED],
        )
        self.assertEqual(
            [allocation.domain_id for allocation in mixed.allocations],
            [grant_control.DomainId.FEED, grant_control.DomainId.SID],
        )
        self.assertEqual(
            [allocation.domain_id for allocation in sid_only.allocations],
            [grant_control.DomainId.SID],
        )
        self.assertTrue(legacy.allocations[0].claims_enabled)
        self.assertTrue(mixed.allocations[0].claims_enabled)
        for profile in worker_profiles.WORKER_PROFILE_PRESETS.values():
            for allocation in profile.allocations:
                if allocation.domain_id is grant_control.DomainId.SID:
                    self.assertEqual(allocation.owned_cap, 32)
                    self.assertEqual(allocation.claims_per_cycle, 2)
                    self.assertFalse(allocation.claims_enabled)

    def test_bcfy_calls_authority_mode_is_closed(self) -> None:
        self.assertEqual(
            {mode.value for mode in worker_profiles.BcfyCallsAuthorityMode},
            {"legacy_feed", "sid_lease"},
        )

    def test_authority_derivation_overwrites_profile_claim_flags(self) -> None:
        feed, sid = worker_profiles.MIXED_DORMANT_PROFILE.allocations
        tampered = dataclasses.replace(
            worker_profiles.MIXED_DORMANT_PROFILE,
            allocations=(
                dataclasses.replace(feed, claims_enabled=False),
                dataclasses.replace(sid, claims_enabled=True),
            ),
        )

        legacy = worker_profiles.derive_bcfy_calls_authority(
            tampered,
            worker_profiles.BcfyCallsAuthorityMode.LEGACY_FEED,
        )
        sid_lease = worker_profiles.derive_bcfy_calls_authority(
            tampered,
            worker_profiles.BcfyCallsAuthorityMode.SID_LEASE,
        )

        self.assertEqual(
            tuple(
                allocation.claims_enabled for allocation in legacy.allocations
            ),
            (True, False),
        )
        self.assertEqual(
            tuple(
                allocation.claims_enabled
                for allocation in sid_lease.allocations
            ),
            (True, True),
        )
        self.assertEqual(
            tuple(
                (allocation.owned_cap, allocation.claims_per_cycle)
                for allocation in sid_lease.allocations
            ),
            ((800, 20), (32, 2)),
        )

    def test_authority_derivation_rejects_absent_domain(self) -> None:
        cases = (
            (
                worker_profiles.LEGACY_PROFILE,
                worker_profiles.BcfyCallsAuthorityMode.SID_LEASE,
            ),
            (
                worker_profiles.SID_DORMANT_PROFILE,
                worker_profiles.BcfyCallsAuthorityMode.LEGACY_FEED,
            ),
        )

        for profile, mode in cases:
            with self.subTest(profile=profile.name, mode=mode.value):
                with self.assertRaisesRegex(ValueError, "requires.*domain"):
                    worker_profiles.derive_bcfy_calls_authority(profile, mode)

    def test_sid_budget_is_one_total_cycle_allocation(self) -> None:
        sid_allocations = [
            allocation
            for allocation in worker_profiles.MIXED_DORMANT_PROFILE.allocations
            if allocation.domain_id is grant_control.DomainId.SID
        ]

        self.assertEqual(len(sid_allocations), 1)
        self.assertEqual(sid_allocations[0].owned_cap, 32)
        self.assertEqual(sid_allocations[0].claims_per_cycle, 2)

    def test_absent_selector_defaults_only_to_legacy(self) -> None:
        self.assertEqual(
            worker_profiles.resolve_worker_profile(None),
            worker_profiles.LEGACY_PROFILE,
        )
        for selector in ("", " ", "\t"):
            with self.subTest(selector=selector):
                with self.assertRaisesRegex(ValueError, "must not be blank"):
                    worker_profiles.resolve_worker_profile(selector)

    def test_selector_rejects_unknown_preset(self) -> None:
        for selector in ("external.module:profile", " legacy "):
            with self.subTest(selector=selector):
                with self.assertRaisesRegex(
                    ValueError,
                    "Unknown WORKER_PROFILE",
                ):
                    worker_profiles.resolve_worker_profile(selector)

    def test_selector_applies_explicit_domain_capacities(self) -> None:
        profile = worker_profiles.resolve_worker_profile(
            "mixed-dormant",
            feed_owned_cap=123,
            feed_claims_per_cycle=7,
            sid_owned_cap=31,
            sid_claims_per_cycle=1,
        )

        self.assertEqual(
            profile.allocations,
            (
                worker_profiles.DomainAllocation(
                    domain_id=grant_control.DomainId.FEED,
                    owned_cap=123,
                    claims_per_cycle=7,
                    claims_enabled=True,
                ),
                worker_profiles.DomainAllocation(
                    domain_id=grant_control.DomainId.SID,
                    owned_cap=31,
                    claims_per_cycle=1,
                    claims_enabled=False,
                ),
            ),
        )

    def test_profile_name_and_structure_validation(self) -> None:
        baseline = worker_profiles.LEGACY_PROFILE
        invalid_profiles = {
            "empty_name": dataclasses.replace(baseline, name=" "),
            "empty_allocations": dataclasses.replace(
                baseline,
                allocations=(),
            ),
        }

        for case, profile in invalid_profiles.items():
            with self.subTest(case=case):
                with self.assertRaises((TypeError, ValueError)):
                    worker_profiles.validate_worker_profile(profile)

    def test_profile_rejects_duplicate_domains(self) -> None:
        baseline = worker_profiles.LEGACY_PROFILE
        feed = baseline.allocations[0]
        duplicate = dataclasses.replace(
            baseline,
            allocations=(feed, feed),
        )
        with self.assertRaisesRegex(ValueError, "Duplicate.*domain"):
            worker_profiles.validate_worker_profile(duplicate)

    def test_profile_rejects_invalid_caps_and_budgets(self) -> None:
        baseline = worker_profiles.LEGACY_PROFILE
        feed = baseline.allocations[0]
        invalid_allocations = {
            "zero_cap": dataclasses.replace(feed, owned_cap=0),
            "negative_cap": dataclasses.replace(feed, owned_cap=-1),
            "bool_cap": dataclasses.replace(feed, owned_cap=True),
            "zero_enabled_budget": dataclasses.replace(
                feed,
                claims_per_cycle=0,
            ),
            "zero_disabled_budget": dataclasses.replace(
                feed,
                claims_per_cycle=0,
                claims_enabled=False,
            ),
            "negative_budget": dataclasses.replace(
                feed,
                claims_per_cycle=-1,
            ),
            "bool_budget": dataclasses.replace(
                feed,
                claims_per_cycle=True,
            ),
            "budget_above_cap": dataclasses.replace(
                feed,
                owned_cap=2,
                claims_per_cycle=3,
            ),
        }

        for case, allocation in invalid_allocations.items():
            with self.subTest(case=case):
                profile = dataclasses.replace(
                    baseline,
                    allocations=(allocation,),
                )
                with self.assertRaises((TypeError, ValueError)):
                    worker_profiles.validate_worker_profile(profile)

    def test_profile_module_has_no_deployment_execution_surface(self) -> None:
        source = inspect.getsource(worker_profiles).lower()

        for forbidden in (
            "terraform",
            "gcloud",
            "autoscal",
            "load test",
            "connection pool",
            "migration command",
        ):
            with self.subTest(forbidden=forbidden):
                self.assertNotIn(forbidden, source)
        self.assertFalse(
            worker_profiles.MIXED_DORMANT_PROFILE.allocations[1].claims_enabled
        )


if __name__ == "__main__":
    unittest.main()
