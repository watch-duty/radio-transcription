# Broadcastify Calls SID-Only Application Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> superpowers:subagent-driven-development (recommended) or
> superpowers:executing-plans to implement this plan task-by-task. Steps use
> checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove the application-level legacy Broadcastify Calls Feed-authority
switch so Calls can only be claimed through durable SID leases.

**Architecture:** Retain the mixed Feed-and-SID supervisor, but make its topology
unconditional: non-Calls sources use Feed grants and Calls uses SID leases.
Separate Feed-claim capacity metadata from general collector metadata so
`CAP_BCFY_CALLS` is no longer parsed, then preserve the public health authority
field as constant operational evidence.

**Tech Stack:** Python 3.13, dataclasses, asyncio, aiohttp, pytest/unittest,
Ruff, ty/pyright, mise.

## Global Constraints

- Do not implement any GOO-768 admin mutation, read-model, or Reporter UI work.
- Do not change database schemas or delete durable SID lease rows.
- Do not change SID admission capacity, work concurrency, failure policy,
  fencing, heartbeat, or page-settlement behavior.
- Keep `/healthz.bcfy_calls_authority_mode` with the exact value `sid_lease`.
- Keep Feed-domain claims enabled for `bcfy_feeds`, `openmhz`, and
  `fire_notifications`.
- Ignore any supplied `BCFY_CALLS_AUTHORITY_MODE` or `CAP_BCFY_CALLS` value
  rather than retaining a hidden rollback path.
- Run only focused low-resource tests locally; leave resource-heavy integration
  and E2E lanes to GitHub Actions.

---

### Task 1: Separate Feed-claim capacity from collector metadata

**Files:**

- Modify: `backend/pipeline/ingestion/source_runtime_specs.py`
- Modify: `backend/pipeline/ingestion/settings.py`
- Modify: `backend/pipeline/ingestion/main.py`
- Modify: `backend/pipeline/storage/feed_store.py`
- Modify:
  `backend/pipeline/ingestion/tests/test_source_runtime_specs.py`
- Modify: `backend/pipeline/ingestion/tests/test_settings.py`
- Modify: `backend/pipeline/ingestion/tests/test_grant_controls.py`

**Interfaces:**

- Produces:
  `source_runtime_specs.feed_claimable_source_specs() -> dict[SourceType, SourceRuntimeSpec]`
- Produces:
  `source_runtime_specs.default_feed_claim_caps() -> dict[SourceType, int]`
- Produces:
  `CollectorSettings.feed_claim_caps: Mapping[SourceType, int]`
- Removes: `CollectorSettings.caps`

- [ ] **Step 1: Write failing source-registry tests**

Replace the claimable-source assertions with Feed-authority-specific
assertions:

```python
def test_feed_claimable_specs_exclude_non_feed_authorities(self) -> None:
    specs = source_runtime_specs.feed_claimable_source_specs()

    self.assertEqual(
        set(specs),
        {
            feed_store.SourceType.BCFY_FEEDS,
            feed_store.SourceType.OPENMHZ,
            feed_store.SourceType.FIRE_NOTIFICATIONS,
        },
    )
    self.assertNotIn(feed_store.SourceType.BCFY_CALLS, specs)
    self.assertNotIn(feed_store.SourceType.ECHO, specs)


def test_default_feed_claim_caps_match_feed_claimable_specs(self) -> None:
    caps = source_runtime_specs.default_feed_claim_caps()

    self.assertEqual(
        caps,
        {
            feed_store.SourceType.BCFY_FEEDS: 240,
            feed_store.SourceType.OPENMHZ: 900,
            feed_store.SourceType.FIRE_NOTIFICATIONS: 600,
        },
    )
```

- [ ] **Step 2: Write failing settings tests for removed Calls capacity**

Update settings tests to use `settings.feed_claim_caps` and add:

```python
def test_legacy_calls_cap_environment_is_ignored(self) -> None:
    env = {
        **_required_env(),
        "CAP_BCFY_CALLS": "999",
    }

    with patch.dict("os.environ", env, clear=True):
        settings = CollectorSettings()

    self.assertNotIn(SourceType.BCFY_CALLS, settings.feed_claim_caps)
    self.assertEqual(
        set(settings.feed_claim_caps),
        {
            SourceType.BCFY_FEEDS,
            SourceType.OPENMHZ,
            SourceType.FIRE_NOTIFICATIONS,
        },
    )
```

Change existing cap assertions so `CAP_BCFY_FEEDS`, `CAP_OPENMHZ`, and
`CAP_FIRE_NOTIFICATIONS` still override their exact entries.

- [ ] **Step 3: Run the new tests and confirm the legacy contract fails**

Run:

```bash
safe-run -- uv run python -m pytest \
  backend/pipeline/ingestion/tests/test_source_runtime_specs.py \
  backend/pipeline/ingestion/tests/test_settings.py -q
```

Expected: failures because the new Feed-claim functions do not exist,
`CollectorSettings.caps` still includes Calls, and `CAP_BCFY_CALLS` is still
parsed.

- [ ] **Step 4: Implement Feed-authority metadata**

Rename `SourceRuntimeSpec.claimable` to `feed_claimable`, rename
`default_cap` to `default_feed_cap`, and configure Calls as collector metadata
without Feed authority:

```python
feed_store.SourceType.BCFY_CALLS: SourceRuntimeSpec(
    source_type=feed_store.SourceType.BCFY_CALLS,
    topic_kind=TopicKind.SEGMENTED,
    feed_claimable=False,
    url_base_env="BCFY_CALLS_URL_BASE",
    url_base_default=constants.BCFY_CALLS_URL_BASE,
),
```

The three Feed-authority sources retain `feed_claimable=True` and their current
caps. Echo retains `feed_claimable=False`.

Replace the old helper functions with:

```python
def feed_claimable_source_specs() -> dict[
    feed_store.SourceType,
    SourceRuntimeSpec,
]:
    return {
        source_type: spec
        for source_type, spec in SOURCE_RUNTIME_SPECS.items()
        if spec.feed_claimable
    }


def default_feed_claim_caps() -> dict[feed_store.SourceType, int]:
    caps: dict[feed_store.SourceType, int] = {}
    for source_type, spec in feed_claimable_source_specs().items():
        if spec.default_feed_cap is None:
            msg = f"Feed-claimable source type {source_type.value} has no cap"
            raise ValueError(msg)
        caps[source_type] = spec.default_feed_cap
    return caps
```

- [ ] **Step 5: Make settings load only Feed claim caps**

Rename `_DEFAULT_CAPS` to `_DEFAULT_FEED_CLAIM_CAPS`, rename
`_load_caps_from_env()` to `_load_feed_claim_caps_from_env()`, and source it
from `default_feed_claim_caps()`.

Replace the mutable/general settings fields with:

```python
feed_claim_caps: typing.Mapping[SourceType, int] = field(
    default_factory=lambda: types.MappingProxyType(
        _load_feed_claim_caps_from_env()
    ),
)
```

Delete the derived `feed_claim_caps` assignment from `__post_init__`. No code
path may load `CAP_BCFY_CALLS`.

- [ ] **Step 6: Update registry and storage documentation**

In `main.py`, compare `set(settings.feed_claim_caps)` with
`set(source_runtime_specs.feed_claimable_source_specs())`. Keep the independent
topic-path startup check over every registered collector. Update the invariant
message to say “Feed-claim caps registry.”

Update `FeedStore` documentation to state that `CollectorRuntime` passes
`settings.feed_claim_caps`, and that the explicit list excludes sources owned
through non-Feed authority.

Update `test_grant_controls.py` fixtures from `default_caps()` to
`default_feed_claim_caps()`.

- [ ] **Step 7: Run focused registry, settings, and grant-control tests**

Run:

```bash
safe-run -- uv run python -m pytest \
  backend/pipeline/ingestion/tests/test_source_runtime_specs.py \
  backend/pipeline/ingestion/tests/test_settings.py \
  backend/pipeline/ingestion/tests/test_grant_controls.py -q
```

Expected: all selected tests and subtests pass.

- [ ] **Step 8: Commit the Feed-cap separation**

```bash
git add \
  backend/pipeline/ingestion/source_runtime_specs.py \
  backend/pipeline/ingestion/settings.py \
  backend/pipeline/ingestion/main.py \
  backend/pipeline/storage/feed_store.py \
  backend/pipeline/ingestion/tests/test_source_runtime_specs.py \
  backend/pipeline/ingestion/tests/test_settings.py \
  backend/pipeline/ingestion/tests/test_grant_controls.py
git commit -m "refactor(ingestion): separate Calls from Feed claim caps"
```

---

### Task 2: Remove authority-mode selection from worker profiles

**Files:**

- Modify: `backend/pipeline/ingestion/worker_profiles.py`
- Modify: `backend/pipeline/ingestion/settings.py`
- Modify: `backend/pipeline/ingestion/tests/test_worker_profiles.py`
- Modify: `backend/pipeline/ingestion/tests/test_settings.py`

**Interfaces:**

- Produces:
  `worker_profiles.build_mixed_worker_profile(*, feed_owned_cap: int, feed_claims_per_cycle: int, sid_owned_cap: int, sid_claims_per_cycle: int) -> WorkerProfile`
- Produces: `worker_profiles.MIXED_PROFILE`
- Keeps: `worker_profiles.LEGACY_PROFILE` for generic Feed-domain tests.
- Removes: `BcfyCallsAuthorityMode`, `derive_bcfy_calls_authority`,
  `MIXED_DORMANT_PROFILE`, `SID_DORMANT_PROFILE`,
  `WORKER_PROFILE_PRESETS`, and `resolve_worker_profile`.

- [ ] **Step 1: Write failing fixed-topology tests**

Replace mode/preset tests with:

```python
def test_mixed_profile_enables_feed_and_sid_claims(self) -> None:
    profile = worker_profiles.build_mixed_worker_profile(
        feed_owned_cap=123,
        feed_claims_per_cycle=7,
        sid_owned_cap=31,
        sid_claims_per_cycle=1,
    )

    self.assertEqual(profile.name, "mixed")
    self.assertEqual(
        tuple(
            (allocation.domain_id, allocation.claims_enabled)
            for allocation in profile.allocations
        ),
        (
            (grant_control.DomainId.FEED, True),
            (grant_control.DomainId.SID, True),
        ),
    )
    self.assertEqual(
        tuple(
            (allocation.owned_cap, allocation.claims_per_cycle)
            for allocation in profile.allocations
        ),
        ((123, 7), (31, 1)),
    )
```

Retain validation coverage for empty profiles, duplicate domains, non-positive
capacities, claim budget greater than owned capacity, deep immutability, and
the generic `LEGACY_PROFILE`.

- [ ] **Step 2: Update the settings test to require unconditional SID claims**

The default settings assertion must require:

```python
self.assertEqual(settings.worker_profile.name, "mixed")
sid = worker_profiles.allocation_for_domain(
    settings.worker_profile,
    grant_control.DomainId.SID,
)
self.assertIsNotNone(sid)
assert sid is not None
self.assertTrue(sid.claims_enabled)
```

Add an environment compatibility assertion:

```python
def test_legacy_authority_environment_is_ignored(self) -> None:
    env = {
        **_required_env(),
        "BCFY_CALLS_AUTHORITY_MODE": "legacy_feed",
    }

    with patch.dict("os.environ", env, clear=True):
        settings = CollectorSettings()

    sid = worker_profiles.allocation_for_domain(
        settings.worker_profile,
        grant_control.DomainId.SID,
    )
    self.assertIsNotNone(sid)
    assert sid is not None
    self.assertTrue(sid.claims_enabled)
```

- [ ] **Step 3: Run the worker-profile/settings tests and confirm failure**

Run:

```bash
safe-run -- uv run python -m pytest \
  backend/pipeline/ingestion/tests/test_worker_profiles.py \
  backend/pipeline/ingestion/tests/test_settings.py -q
```

Expected: failures because `build_mixed_worker_profile` and `MIXED_PROFILE` do
not exist and the legacy authority variable still selects Feed ownership.

- [ ] **Step 4: Implement the fixed mixed profile**

Delete the Calls authority enum and derivation code. Replace the dormant mixed
profile with:

```python
MIXED_PROFILE = WorkerProfile(
    name="mixed",
    allocations=(
        LEGACY_PROFILE.allocations[0],
        DomainAllocation(
            domain_id=grant_control.DomainId.SID,
            owned_cap=32,
            claims_per_cycle=2,
            claims_enabled=True,
        ),
    ),
)
```

Add:

```python
def build_mixed_worker_profile(
    *,
    feed_owned_cap: int = 800,
    feed_claims_per_cycle: int = 20,
    sid_owned_cap: int = 32,
    sid_claims_per_cycle: int = 2,
) -> WorkerProfile:
    feed, sid = MIXED_PROFILE.allocations
    return validate_worker_profile(
        dataclasses.replace(
            MIXED_PROFILE,
            allocations=(
                dataclasses.replace(
                    feed,
                    owned_cap=feed_owned_cap,
                    claims_per_cycle=feed_claims_per_cycle,
                ),
                dataclasses.replace(
                    sid,
                    owned_cap=sid_owned_cap,
                    claims_per_cycle=sid_claims_per_cycle,
                ),
            ),
        )
    )
```

Remove unused `enum`, `types`, and `typing` imports after removing the preset
mapping.

- [ ] **Step 5: Remove settings authority selection**

Delete `_load_bcfy_calls_authority_mode()` and the
`CollectorSettings.bcfy_calls_authority_mode` field. In `__post_init__`, build
the profile directly:

```python
profile = worker_profiles.build_mixed_worker_profile(
    feed_owned_cap=self.max_feeds_per_worker,
    feed_claims_per_cycle=self.lease_admission_cycle_budget,
    sid_owned_cap=self.max_sids_per_worker,
    sid_claims_per_cycle=self.sid_lease_admission_cycle_budget,
)
object.__setattr__(self, "worker_profile", profile)
```

- [ ] **Step 6: Run the worker-profile/settings tests**

Run:

```bash
safe-run -- uv run python -m pytest \
  backend/pipeline/ingestion/tests/test_worker_profiles.py \
  backend/pipeline/ingestion/tests/test_settings.py -q
```

Expected: all selected tests and subtests pass.

- [ ] **Step 7: Commit unconditional mixed authority**

```bash
git add \
  backend/pipeline/ingestion/worker_profiles.py \
  backend/pipeline/ingestion/settings.py \
  backend/pipeline/ingestion/tests/test_worker_profiles.py \
  backend/pipeline/ingestion/tests/test_settings.py
git commit -m "refactor(ingestion): make Calls SID authority unconditional"
```

---

### Task 3: Make runtime observability report constant SID authority

**Files:**

- Modify: `backend/pipeline/ingestion/health_server.py`
- Modify: `backend/pipeline/ingestion/collector_runtime.py`
- Modify: `backend/pipeline/ingestion/tests/test_health_server.py`
- Modify: `backend/pipeline/ingestion/tests/test_collector_runtime.py`

**Interfaces:**

- Produces:
  `health_server.BCFY_CALLS_AUTHORITY_MODE: Final[str] = "sid_lease"`
- Removes: `HealthState.bcfy_calls_authority_mode`
- Keeps: `/healthz` JSON key `bcfy_calls_authority_mode`.

- [ ] **Step 1: Write failing health contract tests**

Remove the authority argument from every `HealthState` fixture. Assert the
response always contains:

```python
self.assertEqual(body["bcfy_calls_authority_mode"], "sid_lease")
```

Delete test mutations that assign another authority value. Add:

```python
def test_calls_authority_is_sid_only(self) -> None:
    self.assertEqual(
        health_server.BCFY_CALLS_AUTHORITY_MODE,
        "sid_lease",
    )
    self.assertNotIn(
        "bcfy_calls_authority_mode",
        {field.name for field in dataclasses.fields(HealthState)},
    )
```

- [ ] **Step 2: Simplify runtime composition tests**

Remove `BcfyCallsAuthorityMode` parameters from `_settings()`, `_runtime()`, and
`TestSupervisorComposition._compose()`. Delete
`test_legacy_mode_keeps_calls_in_feed_claim_store`. Rename the remaining claim
test to `test_calls_are_excluded_from_feed_claim_store`.

Keep the composition assertion that registrations are ordered:

```python
[
    grant_control.DomainId.FEED,
    grant_control.DomainId.SID,
]
```

- [ ] **Step 3: Run health/runtime tests and confirm failure**

Run:

```bash
safe-run -- uv run python -m pytest \
  backend/pipeline/ingestion/tests/test_health_server.py \
  backend/pipeline/ingestion/tests/test_collector_runtime.py -q
```

Expected: failures because `HealthState` still requires the removed field and
runtime fixtures still reference the deleted settings mode.

- [ ] **Step 4: Implement constant authority telemetry**

In `health_server.py`, add:

```python
BCFY_CALLS_AUTHORITY_MODE: typing.Final[str] = "sid_lease"
```

Remove `bcfy_calls_authority_mode` from `HealthState` and return the constant
from `_response_payload()`.

In `CollectorRuntime.__init__`, construct `HealthState` without an authority
argument. In startup telemetry, use:

```python
"bcfy_calls_authority_mode": health_server.BCFY_CALLS_AUTHORITY_MODE,
```

- [ ] **Step 5: Run health/runtime tests**

Run:

```bash
safe-run -- uv run python -m pytest \
  backend/pipeline/ingestion/tests/test_health_server.py \
  backend/pipeline/ingestion/tests/test_collector_runtime.py -q
```

Expected: all selected tests and subtests pass.

- [ ] **Step 6: Commit constant SID observability**

```bash
git add \
  backend/pipeline/ingestion/health_server.py \
  backend/pipeline/ingestion/collector_runtime.py \
  backend/pipeline/ingestion/tests/test_health_server.py \
  backend/pipeline/ingestion/tests/test_collector_runtime.py
git commit -m "refactor(ingestion): report constant Calls SID authority"
```

---

### Task 4: Validate the complete application cleanup

**Files:**

- Verify all files changed in Tasks 1-3.
- Update if required:
  `docs/superpowers/specs/2026-07-27-bcfy-calls-sid-only-cleanup-design.md`

**Interfaces:**

- Consumes all interfaces produced by Tasks 1-3.
- Produces one draft application PR ready for CI and review.

- [ ] **Step 1: Scan for removed authority controls**

Run:

```bash
rg -n \
  'BCFY_CALLS_AUTHORITY_MODE|BcfyCallsAuthorityMode|legacy_feed|CAP_BCFY_CALLS|bcfy_calls_authority_mode' \
  backend
```

Expected: the only `bcfy_calls_authority_mode` references are the constant
health/startup telemetry contract and its tests. There are no legacy mode,
environment, or Calls Feed-cap references.

- [ ] **Step 2: Format the repository through the project task**

Run:

```bash
safe-run -- mise run format
```

Inspect `git status --short` and revert no user changes. If formatting touches
unrelated files, retain only files in this plan.

- [ ] **Step 3: Run the complete focused unit suite**

Run:

```bash
safe-run -- uv run python -m pytest \
  backend/pipeline/ingestion/tests/test_source_runtime_specs.py \
  backend/pipeline/ingestion/tests/test_settings.py \
  backend/pipeline/ingestion/tests/test_worker_profiles.py \
  backend/pipeline/ingestion/tests/test_grant_controls.py \
  backend/pipeline/ingestion/tests/test_health_server.py \
  backend/pipeline/ingestion/tests/test_collector_runtime.py -q
```

Expected: all selected tests and subtests pass.

- [ ] **Step 4: Run Python lint and type checks**

Run:

```bash
safe-run -- mise run lint:python
```

Expected: Ruff, formatting, typing, notebook, and dead-code checks pass. If a
pre-existing unrelated failure appears, capture its exact output in the draft
PR and run the narrow equivalent over the changed files.

- [ ] **Step 5: Verify diff integrity**

Run:

```bash
git diff --check origin/main...HEAD
git status --short --branch
git diff --stat origin/main...HEAD
```

Expected: no whitespace errors, no uncommitted implementation changes, and
only the approved design, plan, application code, and focused tests differ from
`origin/main`.

- [ ] **Step 6: Commit any formatting-only adjustments**

If Task 2-4 formatting changed intended files after the prior commits:

```bash
git add \
  backend/pipeline/ingestion \
  backend/pipeline/storage/feed_store.py
git commit -m "style(ingestion): format SID-only cleanup"
```

Skip this commit when no formatting changes remain.

- [ ] **Step 7: Push and open the draft PR**

Push:

```bash
git push -u origin agent/bcfy-calls-sid-only-cleanup
```

Create a draft PR targeting `main` with title:

```text
[ENG-ONLY] refactor(ingestion): make Broadcastify Calls SID-only
```

The body must state the deployment PR dependency, the irreversible removal of
legacy configuration, preserved health response compatibility, focused test
counts, and that GOO-768 is out of scope.
