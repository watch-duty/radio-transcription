from __future__ import annotations

import ast
import dataclasses
import inspect
import re
import types
import typing
import unittest

from backend.pipeline.ingestion import grant_control, worker_profiles


def _catalog_with(
    **entries: worker_profiles.DomainCatalogEntry,
) -> typing.Mapping[
    grant_control.DomainId,
    worker_profiles.DomainCatalogEntry,
]:
    catalog = dict(worker_profiles.DOMAIN_CATALOG)
    for domain_name, entry in entries.items():
        catalog[grant_control.DomainId(domain_name)] = entry
    return types.MappingProxyType(catalog)


class TestWorkerProfile(unittest.TestCase):
    """Tests for the closed immutable worker-profile model."""

    def test_values_are_deeply_immutable(self) -> None:
        profile = worker_profiles.MIXED_DORMANT_PROFILE
        feed_entry = worker_profiles.DOMAIN_CATALOG[grant_control.DomainId.FEED]
        mutable_profile = typing.cast("typing.Any", profile)
        mutable_allocation = typing.cast("typing.Any", profile.allocations[0])
        mutable_entry = typing.cast("typing.Any", feed_entry)
        mutable_catalog = typing.cast(
            "typing.Any",
            worker_profiles.DOMAIN_CATALOG,
        )
        mutable_presets = typing.cast(
            "typing.Any",
            worker_profiles.WORKER_PROFILE_PRESETS,
        )

        with self.assertRaises(dataclasses.FrozenInstanceError):
            mutable_profile.name = "changed"
        with self.assertRaises(dataclasses.FrozenInstanceError):
            mutable_allocation.owned_cap = 1
        with self.assertRaises(dataclasses.FrozenInstanceError):
            mutable_entry.logical_authority = "changed"
        with self.assertRaises(TypeError):
            mutable_catalog[grant_control.DomainId.FEED] = feed_entry
        with self.assertRaises(TypeError):
            mutable_presets["changed"] = profile
        self.assertIsInstance(profile.allocations, tuple)
        self.assertIsInstance(
            feed_entry.compatible_resource_classes,
            frozenset,
        )

    def test_catalog_has_exact_canonical_domain_coverage(self) -> None:
        self.assertEqual(
            set(worker_profiles.DOMAIN_CATALOG),
            set(grant_control.DomainId),
        )
        self.assertEqual(
            set(worker_profiles.AuthorityKind),
            {
                worker_profiles.AuthorityKind.FEED,
                worker_profiles.AuthorityKind.SID_LEASE,
            },
        )
        self.assertEqual(
            set(worker_profiles.ResourceClass),
            {
                worker_profiles.ResourceClass.SHARED,
                worker_profiles.ResourceClass.CONTINUOUS,
                worker_profiles.ResourceClass.DISCRETE,
            },
        )
        self.assertEqual(
            {kind.value for kind in worker_profiles.AuthorityKind},
            {"feed", "sid_lease"},
        )
        self.assertEqual(
            {resource.value for resource in worker_profiles.ResourceClass},
            {"shared", "continuous", "discrete"},
        )
        self.assertEqual(
            set(worker_profiles.WORKER_PROFILE_PRESETS),
            {"legacy", "mixed-dormant", "sid-dormant"},
        )
        for domain_id, entry in worker_profiles.DOMAIN_CATALOG.items():
            with self.subTest(domain_id=domain_id.value):
                self.assertIs(entry.domain_id, domain_id)

        feed = worker_profiles.DOMAIN_CATALOG[grant_control.DomainId.FEED]
        sid = worker_profiles.DOMAIN_CATALOG[grant_control.DomainId.SID]
        self.assertEqual(feed.logical_authority, "feed")
        self.assertIs(feed.authority_kind, worker_profiles.AuthorityKind.FEED)
        self.assertEqual(feed.required_config_group, "feed")
        self.assertEqual(
            feed.compatible_resource_classes,
            frozenset(
                (
                    worker_profiles.ResourceClass.SHARED,
                    worker_profiles.ResourceClass.CONTINUOUS,
                )
            ),
        )
        self.assertEqual(sid.logical_authority, "ingestion_lease")
        self.assertIs(
            sid.authority_kind,
            worker_profiles.AuthorityKind.SID_LEASE,
        )
        self.assertEqual(sid.required_config_group, "sid")
        self.assertEqual(
            sid.compatible_resource_classes,
            frozenset(
                (
                    worker_profiles.ResourceClass.SHARED,
                    worker_profiles.ResourceClass.DISCRETE,
                )
            ),
        )

    def test_catalog_contains_no_dynamic_execution_surface(self) -> None:
        tree = ast.parse(inspect.getsource(worker_profiles))
        imported_roots = {
            node.names[0].name.split(".", maxsplit=1)[0]
            for node in ast.walk(tree)
            if isinstance(node, ast.Import)
        }
        called_names = {
            node.func.id
            for node in ast.walk(tree)
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
        }

        self.assertTrue(imported_roots.isdisjoint({"importlib", "subprocess"}))
        self.assertTrue(called_names.isdisjoint({"eval", "exec", "__import__"}))
        for entry in worker_profiles.DOMAIN_CATALOG.values():
            self.assertIsInstance(entry.logical_authority, str)
            self.assertIsInstance(entry.required_config_group, str)
            self.assertFalse(callable(entry.logical_authority))
            self.assertFalse(callable(entry.required_config_group))

    def test_presets_have_exact_domains_caps_budgets_and_claim_flags(
        self,
    ) -> None:
        legacy = worker_profiles.LEGACY_PROFILE
        mixed = worker_profiles.MIXED_DORMANT_PROFILE
        sid_only = worker_profiles.SID_DORMANT_PROFILE

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
            set(worker_profiles.BcfyCallsAuthorityMode),
            {
                worker_profiles.BcfyCallsAuthorityMode.LEGACY_FEED,
                worker_profiles.BcfyCallsAuthorityMode.SID_LEASE,
            },
        )
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
            dataclasses.replace(
                tampered,
                allocations=(
                    dataclasses.replace(feed, claims_enabled=False),
                    dataclasses.replace(sid, claims_enabled=False),
                ),
            ),
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

    def test_authority_derivation_rejects_incompatible_topologies(self) -> None:
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

    def test_authority_derivation_revalidates_process_envelope(self) -> None:
        profile = dataclasses.replace(
            worker_profiles.MIXED_DORMANT_PROFILE,
            process_owned_cap=800,
        )

        with self.assertRaisesRegex(ValueError, "exceed.*envelope"):
            worker_profiles.derive_bcfy_calls_authority(
                profile,
                worker_profiles.BcfyCallsAuthorityMode.SID_LEASE,
            )

    def test_sid_budget_is_one_total_cycle_allocation(self) -> None:
        profile = worker_profiles.MIXED_DORMANT_PROFILE
        sid_allocations = [
            allocation
            for allocation in profile.allocations
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
                    ValueError, "Unknown WORKER_PROFILE"
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

        self.assertEqual(profile.process_owned_cap, 154)
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

    def test_digest_is_lowercase_sha256_and_order_independent(self) -> None:
        profile = worker_profiles.MIXED_DORMANT_PROFILE
        equivalent = dataclasses.replace(
            profile,
            allocations=tuple(reversed(profile.allocations)),
        )

        digest = worker_profiles.profile_digest(profile)
        self.assertEqual(digest, worker_profiles.profile_digest(equivalent))
        self.assertIsNot(profile, equivalent)
        self.assertIsNotNone(re.fullmatch(r"[0-9a-f]{64}", digest))

    def test_digest_changes_for_every_canonical_field(self) -> None:
        baseline = worker_profiles.LEGACY_PROFILE
        feed = baseline.allocations[0]
        mutations = {
            "name": dataclasses.replace(baseline, name="legacy-v2"),
            "version": dataclasses.replace(baseline, version=2),
            "resource_class": dataclasses.replace(
                baseline,
                resource_class=worker_profiles.ResourceClass.CONTINUOUS,
            ),
            "process_owned_cap": dataclasses.replace(
                baseline,
                process_owned_cap=801,
            ),
            "domain_id": dataclasses.replace(
                baseline,
                allocations=(
                    dataclasses.replace(
                        feed,
                        domain_id=grant_control.DomainId.SID,
                    ),
                ),
            ),
            "owned_cap": dataclasses.replace(
                baseline,
                process_owned_cap=801,
                allocations=(dataclasses.replace(feed, owned_cap=801),),
            ),
            "claims_per_cycle": dataclasses.replace(
                baseline,
                allocations=(dataclasses.replace(feed, claims_per_cycle=19),),
            ),
            "claims_enabled": dataclasses.replace(
                baseline,
                allocations=(dataclasses.replace(feed, claims_enabled=False),),
            ),
        }
        baseline_digest = worker_profiles.profile_digest(baseline)

        for field_name, profile in mutations.items():
            with self.subTest(field_name=field_name):
                self.assertNotEqual(
                    baseline_digest,
                    worker_profiles.profile_digest(profile),
                )

    def test_digest_is_observability_identity_not_authority(self) -> None:
        source = inspect.getsource(worker_profiles.profile_digest).lower()

        self.assertNotIn("authorize", source)
        self.assertNotIn("credential", source)
        self.assertNotIn("grant", source)

    def test_profile_name_version_and_structure_validation(self) -> None:
        baseline = worker_profiles.LEGACY_PROFILE
        invalid_profiles = {
            "empty_name": dataclasses.replace(baseline, name=" "),
            "zero_version": dataclasses.replace(baseline, version=0),
            "bool_version": dataclasses.replace(baseline, version=True),
            "zero_envelope": dataclasses.replace(
                baseline,
                process_owned_cap=0,
            ),
            "empty_allocations": dataclasses.replace(
                baseline,
                allocations=(),
            ),
        }

        for case, profile in invalid_profiles.items():
            with self.subTest(case=case):
                with self.assertRaises((TypeError, ValueError)):
                    worker_profiles.validate_worker_profile(profile)

    def test_profile_rejects_missing_and_duplicate_domains(self) -> None:
        baseline = worker_profiles.LEGACY_PROFILE
        feed = baseline.allocations[0]
        duplicate = dataclasses.replace(
            baseline,
            process_owned_cap=1600,
            allocations=(feed, feed),
        )
        with self.assertRaisesRegex(ValueError, "Duplicate.*domain"):
            worker_profiles.validate_worker_profile(duplicate)

        missing_catalog_domain = types.MappingProxyType(
            {
                grant_control.DomainId.SID: worker_profiles.DOMAIN_CATALOG[
                    grant_control.DomainId.SID
                ],
            }
        )
        with self.assertRaisesRegex((TypeError, ValueError), "Unknown.*domain"):
            worker_profiles.validate_worker_profile(
                baseline,
                missing_catalog_domain,
            )

    def test_profile_rejects_duplicate_logical_authority(self) -> None:
        mixed = worker_profiles.MIXED_DORMANT_PROFILE
        sid_entry = worker_profiles.DOMAIN_CATALOG[grant_control.DomainId.SID]
        catalog = _catalog_with(
            sid=dataclasses.replace(
                sid_entry,
                logical_authority="feed",
            )
        )

        with self.assertRaisesRegex(ValueError, "Duplicate.*authority"):
            worker_profiles.validate_worker_profile(mixed, catalog)

    def test_catalog_entry_metadata_is_validated_before_use(self) -> None:
        baseline = worker_profiles.LEGACY_PROFILE
        feed_entry = worker_profiles.DOMAIN_CATALOG[grant_control.DomainId.FEED]
        invalid_entries = {
            "key_mismatch": dataclasses.replace(
                feed_entry,
                domain_id=grant_control.DomainId.SID,
            ),
            "logical_authority": dataclasses.replace(
                feed_entry,
                logical_authority="",
            ),
            "empty_resource_classes": dataclasses.replace(
                feed_entry,
                compatible_resource_classes=frozenset(),
            ),
            "required_config_group": dataclasses.replace(
                feed_entry,
                required_config_group=" ",
            ),
        }

        for case, entry in invalid_entries.items():
            with self.subTest(case=case):
                catalog = types.MappingProxyType(
                    {grant_control.DomainId.FEED: entry}
                )
                with self.assertRaises((TypeError, ValueError)):
                    worker_profiles.validate_worker_profile(baseline, catalog)

    def test_profile_rejects_resource_class_incompatibility(self) -> None:
        feed_profile = dataclasses.replace(
            worker_profiles.LEGACY_PROFILE,
            resource_class=worker_profiles.ResourceClass.DISCRETE,
        )
        sid_profile = dataclasses.replace(
            worker_profiles.SID_DORMANT_PROFILE,
            resource_class=worker_profiles.ResourceClass.CONTINUOUS,
        )

        for profile in (feed_profile, sid_profile):
            with self.subTest(profile=profile.name):
                with self.assertRaisesRegex(ValueError, "incompatible"):
                    worker_profiles.validate_worker_profile(profile)

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

    def test_enabled_caps_must_fit_process_envelope(self) -> None:
        baseline = worker_profiles.LEGACY_PROFILE
        invalid = dataclasses.replace(baseline, process_owned_cap=799)

        with self.assertRaisesRegex(ValueError, "exceed.*envelope"):
            worker_profiles.validate_worker_profile(invalid)

    def test_smaller_envelope_is_valid_when_enabled_caps_fit(self) -> None:
        mixed = worker_profiles.MIXED_DORMANT_PROFILE
        feed, sid = mixed.allocations
        profile = dataclasses.replace(
            mixed,
            process_owned_cap=3,
            allocations=(
                dataclasses.replace(
                    feed,
                    owned_cap=3,
                    claims_per_cycle=1,
                ),
                sid,
            ),
        )

        self.assertIs(
            worker_profiles.validate_worker_profile(profile),
            profile,
        )

    def test_resource_and_migration_constraints_remain_data_only(self) -> None:
        """D-46/47, D-51/52, and D-53/54/55 stay non-action seams."""
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
