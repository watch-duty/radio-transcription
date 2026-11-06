# Watch Duty Gemini SFT CLI

This directory is the operator entry point for Gemini supervised fine-tuning
and evaluation. The implementation is always authoritative; the documentation
explains workflow and intent without duplicating schemas or state machines.

## Runtime

Run the packaged CLI from the repository's lightweight ASR container:

```bash
docker compose -f asr-eval-docker-compose.yml run --rm notebooks-cpu \
  bash -lc 'gemini-sft --help'
```

Use command help for the current command surface:

```bash
gemini-sft prepare --help
gemini-sft tune --help
gemini-sft eval --help
```

## Code Ownership

| Behavior | Source of truth |
| --- | --- |
| Commands and flags | [`cli.py`](../../src/gemini_sft/cli.py) |
| TOML fields, defaults, and validation | [`config.py`](../../src/gemini_sft/config.py) and [`run_config.example.toml`](run_config.example.toml) |
| Manifest preparation and preflight | [`prepare.py`](../../src/gemini_sft/prepare.py), [`preflight.py`](../../src/gemini_sft/preflight.py), and [`records.py`](../../src/gemini_sft/records.py) |
| Tuning submission and recovery | [`tune.py`](../../src/gemini_sft/tune.py) |
| Evaluation orchestration | [`evaluate.py`](../../src/gemini_sft/evaluate.py) |
| Provider execution, retries, and reuse | [`target_execution.py`](../../src/gemini_sft/target_execution.py) |
| Report schema and metric calculations | [`reporting.py`](../../src/gemini_sft/reporting.py) |
| Durable and local artifact locations | [`artifacts.py`](../../src/gemini_sft/artifacts.py) |

If documentation and code disagree, follow code and correct the documentation.
Avoid copying implementation constants or exhaustive field lists into Markdown.

## Operator Workflow

Copy [`run_config.example.toml`](run_config.example.toml) outside version
control, replace its placeholders, and use a unique `round_id`.

```bash
gemini-sft prepare --config /path/to/run.toml
gemini-sft tune --config /path/to/run.toml --confirm
gemini-sft eval --config /path/to/run.toml
```

`prepare` performs no tuning submission. `tune` and `eval` can create paid
Vertex work. Read the [operator runbook](docs/runbook.md) before either command.

## Documentation

- [Operator runbook](docs/runbook.md)
- [Configuration guidance](docs/configs.md)
- [Evaluation methodology](docs/evaluation-methodology.md)
- [Metric interpretation](docs/metrics.md)
- [Artifact ownership](docs/artifacts.md)
- [Artifact hygiene](docs/hygiene.md)

## Supporting Script

[`build_validation_manifest_from_eval.py`](build_validation_manifest_from_eval.py)
creates a validation manifest using the repository's current validation-set
convention. Its command help is authoritative.

## Verification Boundary

Unit tests mock GCS and Vertex boundaries. They must not submit tuning, run
provider inference, or execute end-to-end evaluations.
