# Stack Research

**Domain:** Brownfield Gemini SFT/evaluation operator workflow for Watch Duty radio-transcription ASR research

**Researched:** 2026-06-28

**Confidence:** HIGH for repo-local stack decisions; MEDIUM for Google GenAI/Vertex API details that may change outside the repo.

## Executive Recommendation

Use the existing `model/` Python package and `gemini-sft` console script as the
workflow surface. Do not build a new app, service, notebook workflow,
or orchestration layer. The right stack is a config-driven Python CLI running
inside the lightweight ASR Docker runtime, with GCS as the durable state store
and Vertex AI/Gemini accessed through the existing `google-genai` optional
extra.

The operator should run from explicit TOML configs, but the durable record
after `prepare` must remain `gs://<bucket>/sft/runs/<round-id>/config.json`.
Local `results/<round-id>/` should stay a cache/report mirror only. This
matches the current repository contract and prevents a new operator from
depending on one workstation's local files.

Console reporting should be generated from the same in-memory metric dicts
that write `wer_summary.json` and normalized inference manifests. Add report
rendering under `gemini_sft.records` or a nearby package module, not as a
notebook-only post-processing script. Keep output Markdown/plain text so it is
useful in terminals, PRs, and run notes without adding a terminal UI dependency.

## Recommended Stack

### Core Technologies

| Technology | Version | Purpose | Why Recommended | Confidence |
|------------|---------|---------|-----------------|------------|
| Python model package | Python >=3.11 from `model/pyproject.toml` | Owns ASR model helpers and the `gemini-sft` console script | The SFT/eval workflow is already packaged as `radio-transcription-model`; using this boundary avoids the root backend Python 3.13/runtime dependency set and keeps ASR research code isolated. | HIGH |
| `gemini-sft` CLI | `model/src/gemini_sft/cli.py` | Operator entry point for `prepare`, `tune`, and `eval` | Existing CLI already handles config parsing, GCS run state, tune resume, and eval scoring. Extend this surface instead of adding another command family. | HIGH |
| TOML + stdlib `tomllib` | Python stdlib | Human-authored run configs | Current config parser validates TOML with dataclasses and clear errors. It is enough for this workflow and avoids adding Pydantic/Typer just for config shape. | HIGH |
| GCS | `google-cloud-storage>=2.19`; model lock currently `3.10.1` | Durable manifests, run state, model inputs, Vertex batch outputs, reports, and normalized inference manifests | The repo already treats GCS run prefixes as authoritative and local files as mirrors. This is the right durability model for long Vertex jobs and operator handoff. | HIGH |
| Vertex AI / Gemini | `google-genai>=2.10,<3`; model lock currently `2.10.0` | Gemini SFT, batch inference, and online checkpoint/endpoint scoring | Existing `common.gemini.vertex` wraps `genai.Client(vertexai=True, project, location)`, tuning, batch jobs, polling, and response parsing. Context7 docs confirm these are current SDK surfaces for Vertex AI tuning and batch inference. | MEDIUM-HIGH |
| Lightweight ASR Docker runtime | `asr-eval-docker-compose.yml` service `notebooks-cpu` | Default local operator runtime | The entrypoint installs `/workspace/model[scoring,vertex]` editable before command execution, giving the operator live package code plus scoring and Vertex dependencies without NeMo/GPU overhead. | HIGH |
| Google Cloud ADC + mounted gcloud config | `gcloud auth application-default login` before container start | Auth for GCS and Vertex from local or VM runs | `asr-eval-docker-compose.yml` mounts `${HOME}/.config/gcloud` and sets `GOOGLE_APPLICATION_CREDENTIALS`; ASR docs explicitly require ADC before running containers. | HIGH |

### Existing Packages And Helpers To Use

| Package/Helper | Purpose | Use It For | Why |
|----------------|---------|------------|-----|
| `gemini_sft.config` | TOML validation and derived GCS paths | Add `models`-style eval target config here | Keeps all operator config validation in one package-owned place and preserves `config.json` resume semantics. |
| `gemini_sft.prepare` | Canonical manifest copy, Gemini JSONL generation, preflight | Keep train/validation/eval split preparation here | It already prevents train/eval leakage and writes the GCS artifact contract. |
| `gemini_sft.tune` | Paid Vertex tuning submission/resume | Continue to gate paid tuning with explicit `--confirm` | Existing state machine persists the job name before polling, which is the right failure mode for long-running jobs. |
| `gemini_sft.evaluate` | Batch inference and scoring | Extend for multiple model targets and dataset breakdowns | It already loads durable config, builds prior context dynamically, uploads normalized inference manifests, and writes WER summaries. |
| `gemini_sft.records` | Markdown/JSON report writers and ledger | Add console summary rendering, exact-empty metrics, total reference words, and dataset breakdown tables here | Reporting belongs beside existing summary writers so console output, local mirrors, and GCS reports cannot drift. |
| `common.gemini.prompts` | Canonical prompt and keyword set | All SFT, eval, notebook, and checkpoint scoring prompt use | Existing drift-guard tests expect shared prompt imports. |
| `common.gemini.context` | Dynamic prior-context construction | SFT JSONL and eval request histories | Preserves same-source grouping, source-order sorting, usable previous transcripts only, and text-only prior modes. |
| `common.gemini.vertex` | Vertex request construction, tuning, batch parsing | All Gemini request shapes and Vertex SDK calls | Keeps snake/camel-case request parsing and safety/generation config consistent across batch and online paths. |
| `common.gemini.batch` | Batch input upload, batch job reuse, prediction loading | Base model and batch-compatible model eval | It already rejects duplicate audio URIs and treats missing predictions as deletions instead of shrinking denominators. |
| `common.scoring` | WER/CER, keyword metrics, hallucination/empty-unintelligible rate, bootstrap, duration buckets | All report metrics | The normalizer is domain-specific and version-sensitive; do not duplicate scoring math in scripts. |
| `common.inference_manifest` | Normalized inference manifest upload | Scorer-ready GCS outputs | Keeps comparison artifacts under `inference_manifests/<dataset>/<model>/<run>/...` and rejects mixed prediction fields. |
| `common.gcs_utils` | GCS URI parsing, JSON/JSONL download/upload helpers | GCS artifact IO | Reuses the repo's tested GCS boundary instead of ad hoc Storage client calls. |
| `model/scripts/sft/score_gemini_sft_checkpoints_online.py` | Current online checkpoint scorer | Treat as a migration/reference source, not the long-term workflow surface | It has useful checkpoint discovery, online concurrency, resumable prediction JSONL, and summary columns, but the operator should not need a separate mental model forever. |

## Local And Cloud Tooling

### Default Operator Runtime

Run Gemini SFT/eval through the lightweight ASR runtime:

```bash
gcloud auth application-default login

docker compose -f asr-eval-docker-compose.yml run --rm notebooks-cpu \
  bash -lc 'gemini-sft --help'
```

Use `notebooks-cpu` by default. Use `notebooks` only when a paired notebook or
other local model work needs GPU access. Do not use `nemo-cli-cpu` or
`nemo-cli-gpu` for Gemini SFT/eval unless the work also requires NeMo/Canary.
Gemini tuning and inference run in Vertex; local GPU does not accelerate those
jobs.

Local fallback is acceptable for contributors who know their Python environment:

```bash
python3 -m pip install -e "model[scoring,vertex]"
gemini-sft --help
```

For onboarding, prefer Docker because it avoids root/model environment
confusion. The root lockfile and model lockfile currently carry different
`google-genai` versions; the SFT workflow should run from the model package
environment installed by the ASR container.

### Cloud Layout

Use one GCS bucket per run environment and keep each experiment under one
round-owned prefix:

```text
gs://<bucket>/sft/runs/<round-id>/
  run_config.toml
  config.json
  status.json
  manifests/canonical/
  model_inputs/gemini/
  preflight/report.json
  tuning/status.json
  evals/

gs://<bucket>/inference_manifests/<dataset-slug>/<model-family>/<round-id>/
  base.jsonl
  tuned.jsonl
  checkpoint_<id>.jsonl
```

The operator TOML names `gcp.project`, `gcp.bucket`, and `gcp.location`.
Operators should run in the same Google Cloud project/environment as the target
manifest bucket unless IAM has been explicitly arranged. This follows
`ASR_CONTRIBUTING.md` and avoids cross-project bucket access surprises.

Vertex should be used only at the model workflow boundary:

- `prepare`: local/container CPU work plus GCS reads/writes, no paid Vertex job.
- `tune`: explicit paid Vertex SFT job, gated by `--confirm`.
- `eval`: Vertex batch inference for batch-compatible targets, followed by
  local scoring from downloaded outputs.
- checkpoint/endpoint online scoring: `models.generate_content` with bounded
  concurrency when batch inference cannot handle the target resource.

The production pipeline stack - Pub/Sub, AlloyDB, Redis, frontend APIs, local
fake GCS, and full Docker Compose - is not required for this operator workflow.
Only real GCS, Vertex AI, ADC, and the model package are in scope.

## Config Shape Recommendation

Keep the existing TOML fields for SFT preparation and tuning. For eval
onboarding, add a `models`-style section that can represent base models, tuned
endpoints, and checkpoint endpoints through one list of target resources.

Recommended direction:

```toml
round_id = "YYYY-MM-DD-short-description"
dataset = "prior-context-count8-v2"
inference_dataset_slug = "echo/eval"
eval_manifest_uri = "gs://bucket/path/manifests/canonical/eval.jsonl"

[gcp]
project = "watch-duty-project"
bucket = "wd-transcription-data"
location = "us-central1"

[context]
prior_turn_count = 8
prior_context_mode = "text_turns"

[[models]]
label = "base"
resource = "gemini-3.1-flash-lite"
mode = "batch"

[[models]]
label = "ckpt_7"
resource = "projects/.../locations/us/endpoints/..."
checkpoint_id = "7"
mode = "online"
```

Use explicit `mode` only if auto-detection would hide behavior from the
operator. The implementation can still choose a sensible default: base model
IDs go through batch inference; checkpoint endpoints go through online scoring.
If Vertex batch support for tuned/checkpoint resources changes, this is the
section to revisit.

Run masked and unmasked evals as separate configs or separate
`inference_dataset_slug` values. Do not add an eval-sibling abstraction unless
the same operator truly needs one command to generate both.

## Console Reporting

Generate console reports from `gemini_sft.evaluate` after scoring, using the
same `metrics` dict passed to `write_wer_summary`. The console should print a
compact plain-text or Markdown table and the durable GCS artifact URIs.

Required report fields:

| Field | Source | Notes |
|-------|--------|-------|
| WER, CER | `common.scoring.compute_wer`, `compute_cer` | Already present for base/tuned. |
| Keyword accuracy | `common.scoring.keyword_metrics` with `GEMINI_TRANSCRIBE_KEYWORDS` | Report weighted overall plus per-keyword table in Markdown/JSON. |
| Hallucination/empty-unintelligible rate | `common.scoring.hallucination_rate` | Keep this name distinct from exact empty response rate. |
| Exact empty response rate | New small helper or shared scorer helper | Count hypotheses where `not hyp.strip()`. Existing checkpoint scorer already does this. |
| Insertions, deletions, substitutions | `compute_wer` result | Report counts and/or percentages, but also include total reference words so percentages are interpretable. |
| Total reference word count | `hits + substitutions + deletions` from `compute_wer` | Add once per model score panel. |
| Dataset breakdowns | Group canonical eval source rows by dataset metadata | Produce bcfy_calls, bcfy_feeds, echo, and fire_notifications without reading local `results/` as truth. |
| Artifact URIs | Batch output URI and normalized inference manifest URI | Existing metrics already carry these for base/tuned. Extend for checkpoints. |

Do not add `rich`, `tabulate`, pandas, a dashboard, or a frontend for this.
Markdown tables from stdlib string formatting are enough, are easy to test, and
match the current `records.py` output style.

## What NOT To Add

| Avoid | Why | Use Instead | Confidence |
|-------|-----|-------------|------------|
| New workflow engine such as Airflow, Prefect, Dagster, or Celery | The workflow is a small operator-driven sequence with paid cloud jobs and durable GCS state; a scheduler adds state duplication and onboarding burden. | `gemini-sft prepare/tune/eval` plus GCS `config.json`. | HIGH |
| Notebook-first orchestration | The project goal is to remove hidden notebook/thread context for new operators. | Packaged CLI and shared helpers. | HIGH |
| New CLI framework such as Typer/Click | `argparse` already works, tests target the current CLI, and this workflow does not need nested plugin UX. | Extend `gemini_sft.cli` with focused subcommands/flags. | HIGH |
| Pydantic for the first eval config iteration | Current dataclass/TOML parser gives explicit domain errors and no extra dependency. | Keep `gemini_sft.config`; add helper validators. | MEDIUM-HIGH |
| Rich/Textual/dashboard reporting | Adds dependency and rendering complexity without solving the operator problem. | Plain text/Markdown report renderer in `gemini_sft.records`. | HIGH |
| Local `results/` as source of truth | Local files are easy to delete, not shared across operators, and are explicitly a cache in repo docs. | GCS run prefix plus normalized inference manifests. | HIGH |
| Full production Docker Compose/fake GCS for SFT/eval | SFT/eval needs real GCS and Vertex, not Pub/Sub, AlloyDB, Redis, or local fake GCS. | `asr-eval-docker-compose.yml` lightweight runtime with ADC. | HIGH |
| NeMo containers for Gemini work | Heavy dependency/runtime overhead unrelated to Vertex Gemini jobs. | `notebooks-cpu` or `notebooks`. | HIGH |
| Committing real run TOMLs, raw prediction JSONL, or `results/` | Repo conventions explicitly forbid committing local experiment state unless requested. | Commit examples/placeholders and publish durable artifacts to GCS. | HIGH |
| W&B/MLflow/DVC experiment tracking | The repo already has a GCS run ledger/artifact convention; adding an external tracker splits provenance. | Improve GCS summaries and ledger first. | MEDIUM-HIGH |

## Installation And Verification

Operator smoke test:

```bash
gcloud auth application-default login
docker compose -f asr-eval-docker-compose.yml run --rm notebooks-cpu \
  bash -lc 'gemini-sft --help'
```

Model package verification from `model/`:

```bash
safe-run -- uv run --extra dev --extra scoring --extra vertex pytest tests/
```

Targeted checks for reporting/config changes:

```bash
safe-run -- uv run --extra dev --extra scoring --extra vertex \
  pytest tests/gemini_sft tests/common/tests/test_gemini_batch.py \
  tests/common/tests/test_gemini_vertex.py tests/common/tests/test_scoring.py
```

Unit tests must keep GCS and Vertex mocked. They should not submit tuning jobs,
run Vertex batch inference, execute notebooks, or perform full end-to-end evals.

## Version Compatibility Notes

| Package/Tool | Current Constraint | Compatibility Note | Confidence |
|--------------|--------------------|--------------------|------------|
| `google-genai` | `model/pyproject.toml`: `>=2.10,<3`; `model/uv.lock`: `2.10.0` | Use the model environment for SFT/eval. The root lockfile has a different SDK version, so root `uv run` is not the onboarding path for operators. | HIGH |
| `google-cloud-storage` | `model/pyproject.toml`: `>=2.19`; `model/uv.lock`: `3.10.1` | Core model dependency because GCS helpers are shared throughout model workflows. | HIGH |
| `jiwer` | `>=3.1,<4`; lock `3.1.0` | Required for WER/CER; keep scoring behavior covered by golden tests before upgrades. | HIGH |
| `nemo_text_processing` | `==1.1.0` | Version-pinned because inverse text normalization can silently change WER. Keep behind `[scoring]`. | HIGH |
| `pytest` | `>=9.0,<10`; lock `9.0.3` | Existing model tests rely on mocked cloud boundaries and should remain the safety net for stack changes. | HIGH |
| Docker ASR runtime | PyTorch image plus `model/notebook_docker/entrypoint.sh` | The image is heavier than the CLI strictly needs, but it is already the documented ASR runtime and installs model extras correctly. Avoid a second Gemini-only image unless startup cost becomes a real blocker. | MEDIUM-HIGH |

## External Docs To Verify Later

These areas are outside the repo and may change; verify against current
official docs before implementing or operating a paid run:

| Area | Why Verify | Current Research Status |
|------|------------|-------------------------|
| Google GenAI Python SDK exact pinned version | Context7 fetched current `/googleapis/python-genai` docs, not a version-specific `2.10.0` reference. Re-check before upgrading SDK calls or changing tuning/batch request shapes. | MEDIUM |
| Gemini model availability and region support | Model IDs, tuned endpoint behavior, and batch locations are service-side and can change. The repo currently special-cases `gemini-3.1-flash-lite` batch location to `us`. | MEDIUM |
| Batch support for tuned/checkpoint endpoint resources | The repo has both batch eval and a separate online checkpoint scorer. Before collapsing all targets into one evaluator, verify which resource forms `client.batches.create` accepts today. | MEDIUM |
| Vertex tuning pricing, quotas, and allowed adapter sizes | Paid tuning needs current pricing/quota confirmation before operator runbooks hard-code expectations. | MEDIUM |
| GCS/ADC/IAM setup for cross-project buckets | ASR docs recommend same-project VM and bucket. If operators need cross-project runs, verify IAM and ADC impersonation steps. | MEDIUM |

## Recommendation Summary

1. Keep the stack as `model/` Python package + `gemini-sft` CLI +
   lightweight ASR Docker runtime.
2. Use GCS run prefixes as the durable workflow database; never promote local
   `results/` to source of truth.
3. Use Vertex AI through `google-genai` inside `common.gemini.vertex`; keep
   request construction, prompts, safety settings, and prior context shared.
4. Add a small config/model-target abstraction inside `gemini_sft.config` and
   `gemini_sft.evaluate`; do not add a workflow engine or new CLI framework.
5. Generate console reports from package-owned metric/rendering helpers using
   stdlib Markdown/plain text, with exact empty response rate, total reference
   word count, dataset breakdowns, and artifact URIs.

## Sources

- `.planning/PROJECT.md` - milestone scope and active requirements.
- `.planning/codebase/STACK.md` - repo runtime platforms, model package, and
  dependency boundaries.
- `.planning/codebase/ARCHITECTURE.md` - model/SFT component ownership.
- `.planning/codebase/CONVENTIONS.md` - config, manifest, prompt, GCS state,
  and git hygiene conventions.
- `.planning/codebase/INTEGRATIONS.md` - Google Cloud, ASR runtime, and Gemini
  SFT integration points.
- `.planning/codebase/TESTING.md` - model test command and mocked cloud
  boundary policy.
- `.planning/codebase/CONCERNS.md` - checkpoint scorer gap, empty response
  terminology, prior-context semantics, and cloud-cost boundaries.
- `model/scripts/sft/README.md` - current `gemini-sft` operator contract.
- `ASR_CONTRIBUTING.md` - lightweight ASR runtime and ADC setup.
- `model/pyproject.toml`, `model/uv.lock` - model package dependencies and
  locked versions.
- `asr-eval-docker-compose.yml`, `model/notebook_docker/entrypoint.sh` -
  container runtime and editable model extra installation.
- `model/src/common/gemini/*`, `model/src/gemini_sft/*`,
  `model/src/common/scoring.py`, `model/src/common/inference_manifest.py` -
  existing implementation boundaries.
- Context7 `/googleapis/python-genai` - fetched 2026-06-28 for Vertex AI
  client initialization, `client.tunings.tune`, `types.TuningDataset`,
  `types.TuningValidationDataset`, `types.CreateTuningJobConfig`,
  `client.batches.create`, and tuned endpoint `models.generate_content`.
- Official source surfaced by Context7:
  `https://github.com/googleapis/python-genai/blob/main/README.md`
- Official source surfaced by Context7:
  `https://github.com/googleapis/python-genai/blob/main/docs/_sources/index.rst.txt`

---
*Stack research for: Gemini SFT/evaluation workflow onboarding*
*Researched: 2026-06-28*
