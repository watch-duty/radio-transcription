# PR 924 Eval-Only Preparation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make separate eval-only rounds preparable through the CLI and label normalized inference artifacts with the evaluated model family.

**Architecture:** Add a preparation-specific config loader that validates either a complete training-manifest pair or a target-bearing eval-only config. Keep the existing training preparation path intact, dispatch eval-only runs to a dedicated artifact record/helper that publishes `config.json` last, and centralize publisher-model versus endpoint-family selection in evaluation.

**Tech Stack:** Python 3.13, `unittest`, `pytest`, fake in-memory GCS fixtures, Ruff 0.15.12, `ty`, and Vulture.

## Global Constraints

- Preserve the immutable one-`[eval.model]`-target-per-round contract.
- Do not rewrite a training round's target after tuning.
- Eval-only preparation uploads only `run_config.toml`, canonical `eval.jsonl`, and `config.json`, in that order.
- Publish `config.json` last so it remains the durable completion marker.
- Keep current training preparation, preflight, tuning, artifact paths, and resume behavior unchanged.
- Continue requiring the existing `[sft]` table for eval-only TOML configs.
- Publisher targets use their own model family; endpoint targets use `sft.base_model`.
- Follow `.github/instructions/PYTHON_STYLE.instructions.md`, including module-only imports, 80-character lines, and complete public API documentation.
- Follow red-green-refactor: every production behavior change requires a test observed failing first.
- Preserve all pre-existing untracked inference manifests, `model/research/`, and `results/`.
- Do not push until the user explicitly requests it.

---

### Task 1: Validate preparation modes in the config boundary

**Files:**
- Modify: `model/src/gemini_sft/config.py:170-335`
- Test: `model/tests/gemini_sft/test_config.py:80-180`

**Interfaces:**
- Consumes: existing TOML parsing through `_load_run_config`.
- Produces: `load_prepare_run_config(path: str | pathlib.Path) -> RunConfig`; eval-only records omit nonexistent training artifact URIs.

- [ ] **Step 1: Add failing preparation-mode tests**

Add these methods to `TestRunConfig` in
`model/tests/gemini_sft/test_config.py`:

```python
def test_prepare_config_accepts_complete_training_pair(self) -> None:
    cfg = config_module.load_prepare_run_config(
        self._write_config(self._valid_toml())
    )

    self.assertIsNotNone(cfg.train_manifest_uri)
    self.assertIsNotNone(cfg.validation_manifest_uri)
    self.assertIn("gemini_train_uri", cfg.to_record_dict())

def test_prepare_config_accepts_eval_only_target(self) -> None:
    body = self._without_manifest_lines(
        self._valid_toml(
            eval_section=self._eval_model_section(
                "checkpoint_6",
                "projects/p/locations/us-central1/endpoints/123",
            )
        ),
        "train_manifest_uri",
        "validation_manifest_uri",
    )

    cfg = config_module.load_prepare_run_config(self._write_config(body))
    record = cfg.to_record_dict()

    self.assertIsNone(cfg.train_manifest_uri)
    self.assertIsNone(cfg.validation_manifest_uri)
    self.assertEqual(cfg.eval_model.label, "checkpoint_6")
    for key in (
        "canonical_train_uri",
        "canonical_validation_uri",
        "gemini_train_uri",
        "gemini_validation_uri",
    ):
        self.assertNotIn(key, record)

def test_prepare_config_rejects_partial_training_pair(self) -> None:
    body = self._without_manifest_lines(
        self._valid_toml(eval_section=self._eval_model_section()),
        "validation_manifest_uri",
    )

    with self.assertRaisesRegex(
        RunConfigError,
        "both train_manifest_uri and validation_manifest_uri",
    ):
        config_module.load_prepare_run_config(self._write_config(body))

def test_prepare_config_requires_target_without_training_pair(self) -> None:
    body = self._without_manifest_lines(
        self._valid_toml(),
        "train_manifest_uri",
        "validation_manifest_uri",
    )

    with self.assertRaisesRegex(
        RunConfigError,
        r"eval-only prepare.*\[eval\.model\]",
    ):
        config_module.load_prepare_run_config(self._write_config(body))
```

- [ ] **Step 2: Run the preparation-mode tests and verify RED**

Run:

```bash
cd model
safe-run -- uv run --extra dev pytest \
  tests/gemini_sft/test_config.py::TestRunConfig::test_prepare_config_accepts_complete_training_pair \
  tests/gemini_sft/test_config.py::TestRunConfig::test_prepare_config_accepts_eval_only_target \
  tests/gemini_sft/test_config.py::TestRunConfig::test_prepare_config_rejects_partial_training_pair \
  tests/gemini_sft/test_config.py::TestRunConfig::test_prepare_config_requires_target_without_training_pair \
  -q
```

Expected: failures because `load_prepare_run_config` does not exist.

- [ ] **Step 3: Decouple config requirements and add the preparation loader**

Change the public loaders and internal signature in
`model/src/gemini_sft/config.py` to:

```python
def load_run_config(path: str | pathlib.Path) -> RunConfig:
    """Load a training TOML config with both training manifests required.

    Args:
        path: Local path to the operator TOML config.

    Returns:
        A validated training run config.

    Raises:
        RunConfigError: If the TOML or required training contract is invalid.
    """
    return _load_run_config(
        path,
        require_training_manifests=True,
        require_eval_model=False,
    )


def load_eval_run_config(path: str | pathlib.Path) -> RunConfig:
    """Load an eval TOML config with one explicit model target required.

    Args:
        path: Local path to the operator TOML config.

    Returns:
        A validated evaluation run config.

    Raises:
        RunConfigError: If the TOML or required eval target is invalid.
    """
    return _load_run_config(
        path,
        require_training_manifests=False,
        require_eval_model=True,
    )


def load_prepare_run_config(path: str | pathlib.Path) -> RunConfig:
    """Load a complete training config or target-bearing eval-only config.

    Args:
        path: Local path to the operator TOML config.

    Returns:
        A validated training or eval-only run config.

    Raises:
        RunConfigError: If only one training manifest is configured or an
            eval-only config lacks `[eval.model]`.
    """
    run_cfg = _load_run_config(
        path,
        require_training_manifests=False,
        require_eval_model=False,
    )
    has_train = run_cfg.train_manifest_uri is not None
    has_validation = run_cfg.validation_manifest_uri is not None
    if has_train != has_validation:
        msg = (
            "prepare configs must define both train_manifest_uri and "
            "validation_manifest_uri, or neither"
        )
        raise RunConfigError(msg)
    if not has_train and run_cfg.eval_model is None:
        msg = "eval-only prepare configs require one [eval.model] target"
        raise RunConfigError(msg)
    return run_cfg


def _load_run_config(
    path: str | pathlib.Path,
    *,
    require_training_manifests: bool,
    require_eval_model: bool,
) -> RunConfig:
```

Inside `_load_run_config`, replace the coupled target requirement with:

```python
eval_model = _eval_model_target(
    eval_table,
    required=require_eval_model,
)
```

Keep required training-manifest parsing controlled only by
`require_training_manifests`.

In `RunConfig.to_record_dict`, remove these keys from the unconditional base
record:

```python
"canonical_train_uri"
"canonical_validation_uri"
"gemini_train_uri"
"gemini_validation_uri"
```

Then add them only for a complete training pair:

```python
if (
    self.train_manifest_uri is not None
    and self.validation_manifest_uri is not None
):
    record.update(
        {
            "canonical_train_uri": self.paths.canonical_train_uri,
            "canonical_validation_uri": (
                self.paths.canonical_validation_uri
            ),
            "gemini_train_uri": self.paths.gemini_train_uri,
            "gemini_validation_uri": self.paths.gemini_validation_uri,
        }
    )
```

Replace `RunConfig.to_record_dict`'s docstring with:

```python
"""Build the JSON-compatible durable config record.

Returns:
    Resolved run state. Training-only artifact URIs are omitted when the
    config has no training manifest pair.
"""
```

- [ ] **Step 4: Run config tests and verify GREEN**

Run:

```bash
cd model
safe-run -- uv run --extra dev pytest tests/gemini_sft/test_config.py -q
```

Expected: all config tests pass.

- [ ] **Step 5: Commit the preparation config contract**

```bash
git add model/src/gemini_sft/config.py \
  model/tests/gemini_sft/test_config.py
git commit -m "feat(gemini-sft): validate preparation modes"
```

### Task 2: Publish eval-only durable preparation state

**Files:**
- Modify: `model/src/gemini_sft/artifacts.py:20-75`
- Modify: `model/src/gemini_sft/prepare.py:25-295`
- Test: `model/tests/gemini_sft/test_workflow.py:45-420`

**Interfaces:**
- Consumes: `config.load_prepare_run_config` from Task 1 and strict `artifacts.load_canonical_rows`.
- Produces: `artifacts.PreparedEvalArtifacts`; `prepare_run(...)` returns training or eval-only artifacts and durable config state.

- [ ] **Step 1: Add the eval-only config helper and failing workflow tests**

Add this helper beside `_config_text` in
`model/tests/gemini_sft/test_workflow.py`:

```python
def _eval_only_config_text(
    *,
    round_id: str = "round-a",
    eval_label: str = "base",
    eval_model: str = "gemini-3.1-flash-lite",
) -> str:
    body = _config_text(
        round_id=round_id,
        eval_label=eval_label,
        eval_model=eval_model,
    )
    excluded = ("train_manifest_uri =", "validation_manifest_uri =")
    return "\n".join(
        line for line in body.splitlines() if not line.startswith(excluded)
    )
```

Add these tests to `TestPrepareRun`:

```python
def test_prepare_cli_publishes_only_eval_artifacts_for_eval_only_round(
    self,
) -> None:
    with tempfile.TemporaryDirectory() as tmp_s:
        tmp = pathlib.Path(tmp_s)
        storage = fake_gcs.FakeStorageClient()
        storage.put(
            "gs://source/manifests/eval.jsonl",
            _manifest([_row("gs://audio/eval.flac", "eval transcript")]),
        )
        cfg_path = tmp / "run.toml"
        cfg_path.write_text(_eval_only_config_text(), encoding="utf-8")
        run_cfg = config_module.load_prepare_run_config(cfg_path)

        with (
            unittest.mock.patch.object(
                prepare.storage,
                "Client",
                return_value=storage,
            ),
            unittest.mock.patch.object(
                prepare,
                "RESULTS_DIR",
                tmp / "results",
            ),
            unittest.mock.patch.object(
                prepare.preflight,
                "run_preflight",
            ) as run_preflight,
            unittest.mock.patch.object(
                prepare,
                "write_gemini_jsonl",
            ) as write_gemini,
        ):
            result = prepare.prepare(argparse.Namespace(config=str(cfg_path)))

        self.assertEqual(result, 0)
        run_preflight.assert_not_called()
        write_gemini.assert_not_called()
        self.assertEqual(
            storage.uploads,
            [
                run_cfg.paths.run_config_uri,
                run_cfg.paths.canonical_eval_uri,
                run_cfg.paths.config_uri,
            ],
        )
        durable = json.loads(storage.get(run_cfg.paths.config_uri))
        self.assertEqual(durable["status"], "eval_prepared")
        self.assertEqual(durable["canonical_eval_rows"], 1)
        self.assertNotIn("gemini_train_uri", durable)
        self.assertFalse(
            (tmp / "results" / "round-a" / "preflight").exists()
        )

def test_eval_only_prepare_rejects_invalid_manifests_before_upload(
    self,
) -> None:
    cases = {
        "malformed JSONL": "{bad json}\n",
        "empty manifest": "",
        "invalid canonical row": _manifest(
            [_row("local/eval.mp3", "invalid audio URI")]
        ),
    }
    for name, content in cases.items():
        with self.subTest(name=name), tempfile.TemporaryDirectory() as tmp_s:
            tmp = pathlib.Path(tmp_s)
            storage = fake_gcs.FakeStorageClient()
            storage.put("gs://source/manifests/eval.jsonl", content)
            cfg_path = tmp / "run.toml"
            cfg_path.write_text(_eval_only_config_text(), encoding="utf-8")

            with (
                unittest.mock.patch.object(
                    prepare.storage,
                    "Client",
                    return_value=storage,
                ),
                unittest.mock.patch.object(
                    prepare,
                    "RESULTS_DIR",
                    tmp / "results",
                ),
            ):
                result = prepare.prepare(
                    argparse.Namespace(config=str(cfg_path))
                )

            self.assertEqual(result, 1)
            self.assertEqual(storage.uploads, [])

def test_tune_rejects_eval_only_config_before_provider_submission(
    self,
) -> None:
    with tempfile.TemporaryDirectory() as tmp_s:
        tmp = pathlib.Path(tmp_s)
        cfg_path = tmp / "run.toml"
        cfg_path.write_text(_eval_only_config_text(), encoding="utf-8")

        with unittest.mock.patch.object(
            tune_module,
            "submit_tuning_job",
        ) as submit:
            result = tune_module.tune(
                argparse.Namespace(config=str(cfg_path), confirm=True)
            )

        self.assertEqual(result, 1)
        submit.assert_not_called()
```

- [ ] **Step 2: Run eval-only preparation tests and verify RED**

Run:

```bash
cd model
safe-run -- uv run --extra dev --extra vertex pytest \
  tests/gemini_sft/test_workflow.py::TestPrepareRun::test_prepare_cli_publishes_only_eval_artifacts_for_eval_only_round \
  tests/gemini_sft/test_workflow.py::TestPrepareRun::test_eval_only_prepare_rejects_invalid_manifests_before_upload \
  tests/gemini_sft/test_workflow.py::TestPrepareRun::test_tune_rejects_eval_only_config_before_provider_submission \
  -q
```

Expected: preparation tests fail because `prepare` still calls the training
loader/path. The tuning rejection test may already pass and guards the
unchanged boundary.

- [ ] **Step 3: Add the eval-only artifact record**

Add this class after `PreparedRunArtifacts` in
`model/src/gemini_sft/artifacts.py`:

```python
@dataclasses.dataclass(frozen=True)
class PreparedEvalArtifacts:
    """Local paths and count produced by preparing an eval-only round.

    Attributes:
        run_config_path: Local copy of the operator TOML.
        canonical_eval_path: Local validated canonical eval manifest.
        canonical_eval_rows: Number of validated canonical eval rows.
    """

    run_config_path: pathlib.Path
    canonical_eval_path: pathlib.Path
    canonical_eval_rows: int
```

- [ ] **Step 4: Dispatch preparation and implement the eval-only path**

In `prepare`, replace the config load with:

```python
run_cfg = config_lib.load_prepare_run_config(args.config)
```

Replace `prepare`'s docstring with:

```python
"""Prepare and publish one training or eval-only run.

Args:
    args: Parsed CLI namespace containing the config path.

Returns:
    Zero when preparation succeeds; one for a validation or I/O failure.
"""
```

After `prepare_run` returns, branch the completion log and exit status:

```python
if isinstance(artifacts, artifacts_lib.PreparedEvalArtifacts):
    logger.info(
        "Prepared eval-only round with %s eval rows.",
        artifacts.canonical_eval_rows,
    )
    return 0 if config.get("status") == "eval_prepared" else 1
logger.info(
    "Prepared %s train rows, %s validation rows, and %s eval rows.",
    artifacts.canonical_train_rows,
    artifacts.canonical_validation_rows,
    artifacts.canonical_eval_rows,
)
return 0 if config.get("status") == "preflight_passed" else 1
```

Change `prepare_run`'s return annotation and add an early dispatch:

```python
def prepare_run(
    *,
    run_cfg: config_lib.RunConfig,
    storage_client: storage.Client,
    results_dir: pathlib.Path,
) -> tuple[
    artifacts_lib.PreparedRunArtifacts
    | artifacts_lib.PreparedEvalArtifacts,
    dict[str, typing.Any],
]:
    """Prepare and publish one validated training or eval-only run.

    Args:
        run_cfg: Validated preparation config.
        storage_client: Client used for source and durable GCS artifacts.
        results_dir: Local root for the run mirror.

    Returns:
        Prepared local artifacts and the durable config record.

    Raises:
        OSError: If local or GCS artifacts cannot be read or written.
        TypeError: If strict manifest parsing finds a non-object row.
        ValueError: If canonical validation or preparation invariants fail.
    """
    if (
        run_cfg.train_manifest_uri is None
        and run_cfg.validation_manifest_uri is None
    ):
        return _prepare_eval_run(
            run_cfg=run_cfg,
            storage_client=storage_client,
            results_dir=results_dir,
        )
```

The existing training body follows this dispatch unchanged.

Add these private helpers before `prepare_artifacts`:

```python
def _prepare_eval_run(
    *,
    run_cfg: config_lib.RunConfig,
    storage_client: storage.Client,
    results_dir: pathlib.Path,
) -> tuple[artifacts_lib.PreparedEvalArtifacts, dict[str, typing.Any]]:
    run_dir = artifacts_lib.local_run_dir(results_dir, run_cfg.round_id)
    artifacts = _prepare_eval_artifacts(run_cfg, storage_client, run_dir)
    config = {
        **run_cfg.to_record_dict(),
        "canonical_eval_rows": artifacts.canonical_eval_rows,
        "status": "eval_prepared",
    }
    for local_path, gcs_uri in (
        (artifacts.run_config_path, run_cfg.paths.run_config_uri),
        (artifacts.canonical_eval_path, run_cfg.paths.canonical_eval_uri),
    ):
        gcs_utils.upload_local_file(storage_client, local_path, gcs_uri)
    config = artifacts_lib.write_and_upload_config(
        results_dir=results_dir,
        run_cfg=run_cfg,
        storage_client=storage_client,
        config=config,
    )
    return artifacts, config


def _prepare_eval_artifacts(
    run_cfg: config_lib.RunConfig,
    storage_client: storage.Client,
    run_dir: pathlib.Path,
) -> artifacts_lib.PreparedEvalArtifacts:
    if run_cfg.eval_model is None:
        msg = "eval-only prepare requires one [eval.model] target"
        raise ValueError(msg)
    canonical_dir = run_dir / "manifests" / "canonical"
    canonical_dir.mkdir(parents=True, exist_ok=True)
    run_config_path = run_dir / "run_config.toml"
    run_config_path.write_text(run_cfg.raw_toml, encoding="utf-8")
    canonical_eval_path = canonical_dir / "eval.jsonl"
    gcs_utils.download_gcs_uri(
        storage_client,
        run_cfg.eval_manifest_uri,
        canonical_eval_path,
    )
    _, eval_rows = artifacts_lib.load_canonical_rows(
        canonical_eval_path,
        "eval",
    )
    return artifacts_lib.PreparedEvalArtifacts(
        run_config_path=run_config_path,
        canonical_eval_path=canonical_eval_path,
        canonical_eval_rows=len(eval_rows),
    )
```

- [ ] **Step 5: Run preparation and training regression tests**

Run:

```bash
cd model
safe-run -- uv run --extra dev --extra vertex pytest \
  tests/gemini_sft/test_config.py \
  tests/gemini_sft/test_workflow.py::TestPrepareRun \
  -q
```

Expected: all selected tests pass. Existing training preparation tests must
still assert the full artifact set and `preflight_passed` status.

- [ ] **Step 6: Commit eval-only preparation**

```bash
git add model/src/gemini_sft/artifacts.py \
  model/src/gemini_sft/prepare.py \
  model/tests/gemini_sft/test_workflow.py
git commit -m "feat(gemini-sft): prepare eval-only rounds"
```

### Task 3: Use the evaluated model family and consume prepared eval state

**Files:**
- Modify: `model/src/gemini_sft/evaluate.py:105-155`
- Test: `model/tests/gemini_sft/test_workflow.py:1140-2560`

**Interfaces:**
- Consumes: eval-only `config.json` from Task 2 and `config.EvalModelTarget`.
- Produces: `_eval_model_family_id(target, base_model) -> str`; normalized manifests use the evaluated publisher family or endpoint base family.

- [ ] **Step 1: Add failing family-selection and end-to-end tests**

Add these methods to `TestEvaluateRun`:

```python
def test_eval_model_family_uses_publisher_target_model(self) -> None:
    target = config_module.EvalModelTarget(
        label="base",
        model="gemini-2.5-flash",
    )

    self.assertEqual(
        evaluate_module._eval_model_family_id(
            target,
            "gemini-3.1-flash-lite",
        ),
        "gemini-2.5-flash",
    )

def test_eval_model_family_uses_base_model_for_endpoint(self) -> None:
    target = config_module.EvalModelTarget(
        label="checkpoint_6",
        model="projects/p/locations/us-central1/endpoints/123",
    )

    self.assertEqual(
        evaluate_module._eval_model_family_id(
            target,
            "gemini-3.1-flash-lite",
        ),
        "gemini-3.1-flash-lite",
    )

def test_eval_consumes_eval_only_prepared_state(self) -> None:
    with tempfile.TemporaryDirectory() as tmp_s:
        tmp = pathlib.Path(tmp_s)
        storage = fake_gcs.FakeStorageClient()
        storage.put(
            "gs://source/manifests/eval.jsonl",
            _manifest([_row("gs://audio/eval.flac", "eval transcript")]),
        )
        cfg_path = tmp / "run.toml"
        cfg_path.write_text(
            _eval_only_config_text(eval_model="gemini-2.5-flash"),
            encoding="utf-8",
        )
        run_cfg = config_module.load_prepare_run_config(cfg_path)
        with (
            unittest.mock.patch.object(
                prepare.storage,
                "Client",
                return_value=storage,
            ),
            unittest.mock.patch.object(
                prepare,
                "RESULTS_DIR",
                tmp / "results",
            ),
        ):
            self.assertEqual(
                prepare.prepare(argparse.Namespace(config=str(cfg_path))),
                0,
            )
        durable = json.loads(storage.get(run_cfg.paths.config_uri))
        predictions = _batch_prediction_map(
            {"gs://audio/eval.flac": "eval transcript"}
        )
        with (
            _patched_eval_scoring(),
            unittest.mock.patch.object(
                evaluate_module,
                "RESULTS_DIR",
                tmp / "results",
            ),
            unittest.mock.patch.object(
                evaluate_module,
                "batch_infer",
                return_value=predictions,
            ),
        ):
            result = evaluate_module.evaluate_run(
                argparse.Namespace(config=str(cfg_path)),
                run_cfg,
                storage,
                durable,
            )

    self.assertEqual(result, 0)
    normalized_uri = (
        "gs://test-bucket/inference_manifests/echo/eval/"
        "gemini_2_5_flash/round-a/base.jsonl"
    )
    self.assertTrue(storage.has(normalized_uri))
    normalized = json.loads(storage.get(normalized_uri).strip())
    self.assertEqual(
        normalized["pred_text_gemini_2_5_flash"],
        "eval transcript",
    )
```

- [ ] **Step 2: Run the family/eval-only tests and verify RED**

Run:

```bash
cd model
safe-run -- uv run --extra dev --extra scoring --extra vertex pytest \
  tests/gemini_sft/test_workflow.py::TestEvaluateRun::test_eval_model_family_uses_publisher_target_model \
  tests/gemini_sft/test_workflow.py::TestEvaluateRun::test_eval_model_family_uses_base_model_for_endpoint \
  tests/gemini_sft/test_workflow.py::TestEvaluateRun::test_eval_consumes_eval_only_prepared_state \
  -q
```

Expected: helper tests fail because `_eval_model_family_id` does not exist;
the integration test uses the base-model slug instead of the target-model
slug.

- [ ] **Step 3: Centralize model-family selection**

Add this helper before `evaluate_run` in
`model/src/gemini_sft/evaluate.py`:

```python
def _eval_model_family_id(
    target: config_lib.EvalModelTarget,
    base_model: str,
) -> str:
    """Return the publisher family represented by one eval target.

    Args:
        target: Durable evaluated model or endpoint target.
        base_model: Publisher model used to create endpoint targets.

    Returns:
        The target model for publisher targets, or ``base_model`` for endpoints.
    """
    if "/endpoints/" in target.model:
        return base_model
    return target.model
```

Replace the existing model-family computation in `evaluate_run` with:

```python
model_family_slug = inference_manifest.model_family_slug_from_model_id(
    _eval_model_family_id(target, base_model)
)
```

- [ ] **Step 4: Run evaluation workflow tests and verify GREEN**

Run:

```bash
cd model
safe-run -- uv run --extra dev --extra scoring --extra vertex pytest \
  tests/gemini_sft/test_workflow.py -q
```

Expected: all workflow tests pass, including publisher and endpoint target
family selection.

- [ ] **Step 5: Commit evaluated-family selection**

```bash
git add model/src/gemini_sft/evaluate.py \
  model/tests/gemini_sft/test_workflow.py
git commit -m "fix(gemini-sft): label eval output by target family"
```

### Task 4: Final verification and scope gate

**Files:**
- Verify: all files changed since `origin/main`.

**Interfaces:**
- Consumes: completed Tasks 1-3.
- Produces: a locally clean PR branch ready to push and resend.

- [ ] **Step 1: Run the exact PR test directories**

```bash
cd model
safe-run -- uv run --extra dev --extra scoring --extra vertex pytest \
  tests/common/tests tests/gemini_sft -q
```

Expected: every test and subtest passes.

- [ ] **Step 2: Run root quality gates**

From the repository root, run each command and require exit code zero:

```bash
safe-run -- uv run ruff check
safe-run -- uv run ruff format --check
safe-run -- uv run ty check
safe-run -- uv run vulture
git diff --check origin/main...HEAD
```

`ty` may print the nine existing unrelated unused-ignore warnings in backend
segmentation tests, but it must exit zero and report no diagnostics in PR
files.

- [ ] **Step 3: Inspect final scope**

```bash
git status --short --branch
git diff --stat origin/main...HEAD
git log --oneline origin/gsd/gemini-sft-workflow-code..HEAD
```

Expected: no tracked working-tree changes; the pre-existing untracked
manifests, `model/research/`, and `results/` remain untouched. Do not push.
