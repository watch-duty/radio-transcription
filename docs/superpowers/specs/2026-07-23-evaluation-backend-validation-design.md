# Evaluation Backend Validation Consolidation

Status: Approved

## Context

Positive-context evaluation requires online execution because each request can
depend on predictions produced by earlier causal waves. The current branch
enforces the incompatible `prior_context_count > 0` plus explicit `batch`
combination in both TOML config loading and target backend resolution.

Those checks protect different seams:

- Config loading must fail before durable artifacts or provider work.
- Backend resolution is a public interface used directly by tests and must
  remain safe when called outside config loading.

The validation timing is intentional, but the business rule and error message
are duplicated. Changing the rule currently requires coordinated edits across
modules.

## Goals

- Make the context/backend compatibility rule single-source.
- Preserve fail-fast config validation.
- Preserve direct-call safety in backend resolution.
- Preserve existing backend selection, exceptions, and error timing.
- Keep count and history-mode validation at untrusted durable and request
  seams.

## Non-Goals

- Changing the config schema or supporting legacy durable config records.
- Changing publisher-model, endpoint, or location routing.
- Removing validation from either config loading or backend resolution.
- Adding another module solely for this rule.
- Replying to or resolving GitHub review threads.

## Options Considered

### Shared helper in `common.gemini.context`

Add one pure helper that validates the context/backend combination and returns
the configured or context-required backend. Config loading and backend
resolution both call it.

This keeps the rule near the existing evaluation-context contract, avoids an
import cycle, preserves both validation seams, and gives maintainers one place
to change the rule.

### Move backend resolution into `gemini_sft.config`

This would reduce one call layer, but it would make the configuration parser
own runtime routing policy. It weakens module cohesion and still needs a
separate path for configs without an evaluation target.

### Add an evaluation-contract module

A dedicated module would provide a clean seam but would be shallow: one small
rule behind a new file and import surface. The added navigation cost is not
justified.

## Design

Add this public helper to `common.gemini.context`:

```python
def resolve_evaluation_backend_for_context(
    prior_context_count: int,
    configured_backend: str | None,
) -> str | None:
    ...
```

Its contract is:

- Validate `prior_context_count` with the same type and nonnegative rules used
  by `validate_evaluation_context_contract`.
- With zero context, return `configured_backend` unchanged.
- With positive context and explicit `batch`, raise the existing `ValueError`.
- With positive context and `None` or `online`, return `online`.

`configured_backend` is expected to be the already-parsed `None`, `batch`, or
`online` value. The helper does not add a new backend-value validation rule;
config parsing continues to own that concern so this refactor remains
behavior-preserving.

Extract the existing count checks into a private helper inside
`common.gemini.context` so both public context-contract functions share the
primitive validation without exporting another interface.

`gemini_sft.config._validated_evaluation_context` will call the shared backend
helper and continue translating `TypeError` or `ValueError` into
`RunConfigError`.

`gemini_sft.target_execution.resolve_target_backend` will use the returned
backend. When the helper returns `None`, the existing target-shape default
remains: endpoints use online and publisher IDs use batch.

`gemini_sft.evaluate.evaluate_run`, request identity construction, and online
inference retain their existing count/mode validation because they consume
durable or independently callable input at different seams.

## Error Handling

The helper owns the canonical incompatibility message:

```text
predicted-history evaluation requires the online backend; batch cannot
construct causal prior predictions
```

Config loading preserves `RunConfigError`; direct backend resolution preserves
`TypeError` and `ValueError`. No provider call or durable publication moves
earlier in the flow.

## Testing

- Add focused unit coverage for zero-context pass-through, positive-context
  online selection, and positive-context batch rejection at the shared helper.
- Retain the config-loading test proving invalid TOML fails early.
- Retain backend-resolver tests proving direct-call behavior and target-shape
  defaults.
- Run the context, config, target-execution, and affected workflow unit tests,
  plus Ruff formatting and whitespace checks.
