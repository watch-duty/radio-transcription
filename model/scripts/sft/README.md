# Watch Duty Gemini SFT CLI

Gemini supervised fine-tuning is exposed as the packaged `gemini-sft` command
from the `radio-transcription-model` distribution under `model/`.

## Runtime

Use the lightweight ASR Docker service from the repo root. It mounts the repo at
`/workspace` and installs `/workspace/model[scoring,vertex]` in editable mode on
container startup.

```bash
docker compose -f asr-eval-docker-compose.yml run --rm notebooks-cpu \
  bash -lc 'gemini-sft --help'
```

## Operator Docs

- [Operator runbook](docs/runbook.md)
- [Config examples](docs/configs.md)
- [Metric glossary](docs/metrics.md)
- [Artifact reference](docs/artifacts.md)
- [Artifact hygiene](docs/hygiene.md)

## Command Summary

```bash
gemini-sft prepare --config /path/to/run.toml
gemini-sft tune --config /path/to/run.toml --confirm
gemini-sft eval --config /path/to/run.toml
```

`prepare` creates either a training round or an eval-only round. `tune`
submits or resumes a paid Vertex tuning job. `eval` can spend money through
Vertex batch inference or online endpoint prediction and evaluates exactly the
one target recorded for the prepared round. See the runbook for command order,
recovery behavior, GCS artifacts, report inspection, checkpoint endpoint evals,
masked/unmasked evals, and artifact hygiene.

## Standalone Scripts

- [`build_validation_manifest_from_eval.py`](build_validation_manifest_from_eval.py) -
  builds a canonical `validation.jsonl` by sampling `eval.jsonl` and
  relabeling `split`. See the runbook's
  [Build A Validation Manifest](docs/runbook.md#build-a-validation-manifest)
  section for why this exists.

## Verification Boundary

Unit tests mock GCS and Vertex boundaries. They must not submit paid Vertex
tuning, run Vertex batch inference, execute notebooks, or run end-to-end evals.
