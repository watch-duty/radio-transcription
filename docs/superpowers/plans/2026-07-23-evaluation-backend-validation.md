# Evaluation Backend Validation Consolidation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the positive-context backend requirement single-source while preserving fail-fast config validation, direct resolver safety, and all current behavior.

**Architecture:** Add one pure context/backend contract helper to `common.gemini.context`, backed by the same private count validator as the existing context contract. Keep validation at both existing seams: config loading translates shared errors to `RunConfigError`, while target backend resolution consumes the helper's backend decision.

**Tech Stack:** Python 3.11+, `unittest` through pytest, uv, Ruff

## Global Constraints

- Preserve existing backend selection, exceptions, and error timing.
- Config loading must continue rejecting positive-context batch before durable publication.
- Direct calls to `resolve_target_backend` must remain safe.
- Keep durable count/mode validation in `evaluate_run`, request identity construction, and online inference.
- Do not change the config schema or add backward compatibility.
- Do not change publisher-model, endpoint, or location routing.
- Do not add a new module for this single rule.
- Do not change anything under `model/colabs`.
- Run only targeted low-resource unit tests; do not run local E2E or broad integration lanes.
- Do not reply to or resolve GitHub review threads without separate authorization.

---

### Task 1: Establish the Shared Context/Backend Contract

**Files:**
- Modify: `model/src/common/gemini/context.py:348-373`
- Test: `model/tests/common/tests/test_gemini_context.py:118`

**Interfaces:**
- Consumes: Existing `prior_context_count: int` and already-parsed `configured_backend: str | None`.
- Produces: `resolve_evaluation_backend_for_context(prior_context_count: int, configured_backend: str | None) -> str | None`.
- Preserves: `validate_evaluation_context_contract(prior_context_count: int, history_mode: str) -> str`.

- [ ] **Step 1: Write failing tests for the shared backend contract**

Add these methods to `TestEvaluationContextBoundary`:

```python
def test_zero_context_preserves_configured_backend(self) -> None:
    for backend in (None, "batch", "online"):
        with self.subTest(backend=backend):
            self.assertEqual(
                context.resolve_evaluation_backend_for_context(0, backend),
                backend,
            )

def test_positive_context_requires_online_backend(self) -> None:
    for backend in (None, "online"):
        with self.subTest(backend=backend):
            self.assertEqual(
                context.resolve_evaluation_backend_for_context(1, backend),
                "online",
            )

def test_positive_context_rejects_batch_backend(self) -> None:
    with self.assertRaisesRegex(
        ValueError,
        "predicted-history evaluation requires the online backend",
    ):
        context.resolve_evaluation_backend_for_context(1, "batch")

def test_backend_contract_rejects_invalid_context_count(self) -> None:
    for count, error, message in (
        (True, TypeError, "must be an integer"),
        (-1, ValueError, "must be non-negative"),
    ):
        with (
            self.subTest(count=count),
            self.assertRaisesRegex(error, message),
        ):
            context.resolve_evaluation_backend_for_context(count, None)
```

- [ ] **Step 2: Run the new tests and verify they fail**

Run:

```bash
safe-run -- uv run --project model --extra dev --extra scoring --extra vertex \
  pytest \
  model/tests/common/tests/test_gemini_context.py::TestEvaluationContextBoundary \
  -q -n 0
```

Expected: FAIL because `common.gemini.context` has no attribute
`resolve_evaluation_backend_for_context`.

- [ ] **Step 3: Implement the shared count validator and backend contract**

Add the private count validator immediately before
`validate_evaluation_context_contract`:

```python
def _validate_prior_context_count(prior_context_count: int) -> None:
    """Validate one structural prediction-history window size.

    Args:
        prior_context_count: Maximum structural prediction-history window.

    Raises:
        TypeError: If the context count is not an integer.
        ValueError: If the context count is negative.
    """
    if isinstance(prior_context_count, bool) or not isinstance(
        prior_context_count,
        int,
    ):
        msg = "prior_context_count must be an integer"
        raise TypeError(msg)
    if prior_context_count < 0:
        msg = "prior_context_count must be non-negative"
        raise ValueError(msg)
```

Replace the inline count checks in `validate_evaluation_context_contract`:

```python
def validate_evaluation_context_contract(
    prior_context_count: int,
    history_mode: str,
) -> str:
    """Validate the evaluation window and its request-shape contract.

    Args:
        prior_context_count: Maximum structural prediction-history window.
        history_mode: Configured representation for prediction history.

    Returns:
        The normalized history mode.

    Raises:
        TypeError: If the context count is not an integer.
        ValueError: If the count is negative or the mode is unsupported.
    """
    _validate_prior_context_count(prior_context_count)
    return validate_history_mode(history_mode)
```

Add the new public helper immediately after that function:

```python
def resolve_evaluation_backend_for_context(
    prior_context_count: int,
    configured_backend: str | None,
) -> str | None:
    """Resolve any backend selection imposed by evaluation context.

    Args:
        prior_context_count: Maximum structural prediction-history window.
        configured_backend: Already-parsed explicit backend, or ``None``.

    Returns:
        The configured backend for stateless evaluation, ``"online"`` for
        positive context, or ``None`` when target shape must choose.

    Raises:
        TypeError: If the context count is not an integer.
        ValueError: If the count is negative or positive context is combined
            with explicit batch execution.
    """
    _validate_prior_context_count(prior_context_count)
    if prior_context_count == 0:
        return configured_backend
    if configured_backend == "batch":
        msg = (
            "predicted-history evaluation requires the online backend; "
            "batch cannot construct causal prior predictions"
        )
        raise ValueError(msg)
    return "online"
```

- [ ] **Step 4: Run the context tests and verify they pass**

Run:

```bash
safe-run -- uv run --project model --extra dev --extra scoring --extra vertex \
  pytest model/tests/common/tests/test_gemini_context.py -q -n 0
```

Expected: PASS, including the new backend-contract cases and all existing
context/scheduling behavior.

- [ ] **Step 5: Run static checks for Task 1**

Run:

```bash
uv run ruff check \
  model/src/common/gemini/context.py \
  model/tests/common/tests/test_gemini_context.py
uv run ruff format --check \
  model/src/common/gemini/context.py \
  model/tests/common/tests/test_gemini_context.py
git diff --check
```

Expected: all commands exit zero.

- [ ] **Step 6: Commit the shared contract**

```bash
git add \
  model/src/common/gemini/context.py \
  model/tests/common/tests/test_gemini_context.py
git commit -m "refactor(gemini-eval): centralize context backend contract"
```

### Task 2: Migrate Both Validation Seams

**Files:**
- Modify: `model/src/gemini_sft/config.py:443-476`
- Modify: `model/src/gemini_sft/target_execution.py:110-156`
- Test: `model/tests/common/tests/test_drift_guard.py:71`
- Test: `model/tests/gemini_sft/test_config.py:416-439`
- Test: `model/tests/gemini_sft/test_target_execution.py:61-135`

**Interfaces:**
- Consumes: `context.resolve_evaluation_backend_for_context(prior_context_count, configured_backend) -> str | None` from Task 1.
- Produces: Config and runtime routing paths that share one compatibility rule and canonical error message.
- Preserves: `resolve_target_backend(target, execution, *, prior_context_count=0) -> str`.

- [ ] **Step 1: Write a failing drift guard for single-source ownership**

Add this method to `TestDriftGuard`:

```python
def test_eval_backend_rule_uses_shared_context_contract(self) -> None:
    """Context/backend compatibility must have one source implementation."""
    config_calls = _python_calls(_SRC_DIR / "gemini_sft" / "config.py")
    target_calls = _python_calls(
        _SRC_DIR / "gemini_sft" / "target_execution.py"
    )

    expected_call = ("context", "resolve_evaluation_backend_for_context")
    self.assertIn(expected_call, config_calls)
    self.assertIn(expected_call, target_calls)

    message = (
        "predicted-history evaluation requires the online backend; "
        "batch cannot construct causal prior predictions"
    )
    owners = []
    for path in _SRC_DIR.rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        if any(
            isinstance(node, ast.Constant) and node.value == message
            for node in ast.walk(tree)
        ):
            owners.append(path.relative_to(_SRC_DIR))
    owners.sort()
    self.assertEqual(
        owners,
        [pathlib.Path("common/gemini/context.py")],
    )
```

- [ ] **Step 2: Run the drift guard and verify it fails**

Run:

```bash
safe-run -- uv run --project model --extra dev --extra scoring --extra vertex \
  pytest \
  model/tests/common/tests/test_drift_guard.py::TestDriftGuard::test_eval_backend_rule_uses_shared_context_contract \
  -q -n 0
```

Expected: FAIL because config and target execution do not call the new helper,
and the canonical error message still has multiple source owners.

- [ ] **Step 3: Migrate config loading to the shared rule**

Replace `_validated_evaluation_context`'s implementation body with:

```python
try:
    mode = context.validate_evaluation_context_contract(
        prior_context_count,
        prior_context_mode,
    )
    context.resolve_evaluation_backend_for_context(
        prior_context_count,
        eval_execution.backend,
    )
except (TypeError, ValueError) as exc:
    raise RunConfigError(str(exc)) from None
return mode
```

This preserves early `RunConfigError` translation and removes the duplicated
backend comparison and message.

- [ ] **Step 4: Migrate target backend resolution to the shared rule**

Replace the count checks, batch incompatibility check, and explicit-backend
branches in `resolve_target_backend` with:

```python
backend = context.resolve_evaluation_backend_for_context(
    prior_context_count,
    execution.backend,
)
if backend is not None:
    return backend
if target.is_endpoint:
    return "online"
return "batch"
```

Keep the existing function interface and docstring exception contract.

- [ ] **Step 5: Run the drift and functional contract tests**

Run:

```bash
safe-run -- uv run --project model --extra dev --extra scoring --extra vertex \
  pytest \
  model/tests/common/tests/test_drift_guard.py::TestDriftGuard::test_eval_backend_rule_uses_shared_context_contract \
  model/tests/gemini_sft/test_config.py::TestRunConfig::test_prepare_rejects_batch_backend_with_prior_context \
  model/tests/gemini_sft/test_target_execution.py::TestTargetBackendResolver \
  -q -n 0
```

Expected: PASS. Config loading still raises `RunConfigError`, direct resolver
calls still validate context/backend compatibility, and the rule has one source
owner.

- [ ] **Step 6: Run the affected low-resource suite**

Run:

```bash
safe-run -- uv run --project model --extra dev --extra scoring --extra vertex \
  pytest \
  model/tests/common/tests/test_gemini_context.py \
  model/tests/common/tests/test_drift_guard.py \
  model/tests/gemini_sft/test_config.py \
  model/tests/gemini_sft/test_target_execution.py \
  model/tests/gemini_sft/test_workflow.py::TestPrepareRun \
  -q -n 0
```

Expected: PASS with no failures.

- [ ] **Step 7: Run static and whitespace checks**

Run:

```bash
uv run ruff check \
  model/src/common/gemini/context.py \
  model/src/gemini_sft/config.py \
  model/src/gemini_sft/target_execution.py \
  model/tests/common/tests/test_gemini_context.py \
  model/tests/common/tests/test_drift_guard.py
uv run ruff format --check \
  model/src/common/gemini/context.py \
  model/src/gemini_sft/config.py \
  model/src/gemini_sft/target_execution.py \
  model/tests/common/tests/test_gemini_context.py \
  model/tests/common/tests/test_drift_guard.py
git diff --check
git diff --name-only \
  refactor/gemini-eval-causal-contract -- model/colabs
```

Expected: Ruff and `git diff --check` exit zero; the `model/colabs` command
prints nothing.

- [ ] **Step 8: Commit the call-site migration**

```bash
git add \
  model/src/gemini_sft/config.py \
  model/src/gemini_sft/target_execution.py \
  model/tests/common/tests/test_drift_guard.py
git commit -m "refactor(gemini-eval): share backend compatibility rule"
```
