# Workspace Instructions

> [!IMPORTANT]
> Always read and parse the following project style guides before making code changes or reviewing code in this repository:
> - Python Style: [PYTHON_STYLE.instructions.md](../.github/instructions/PYTHON_STYLE.instructions.md)
> - JS/TS Style: [JS_TS_STYLE.instructions.md](../.github/instructions/JS_TS_STYLE.instructions.md)

Refer to these guidelines to ensure consistency with project standards.

---

## Agent Action Standards

Prefer the project's **mise** task runner for standard formatting, linting, and
generation tasks. For test execution, follow the Python Test Safety section
below; keep local validation narrow unless the user explicitly asks for a
larger resource-heavy run.

1. **Code Formatting & Linting**:
   - Run `mise run format` locally to automatically format all modified files (Python, Terraform, Frontend) before submitting.
   - Run `mise run lint` locally to execute all code quality checks, formatting linters, and strict type checks.

2. **Protobuf Generation**:
   - Run `mise run generate:protos` to regenerate Python Protobuf/gRPC bindings after making changes to any `.proto` schemas.

3. **Python Test Safety**:
   - Treat test scope as a resource-safety decision. Before running a broad test command, inspect `.mise.toml` or the exact pytest command so you know whether it starts Docker, testcontainers, emulators, or the local service stack.
   - If the user asks to "run tests" without specifying scope, choose the narrowest relevant low-resource check. Prefer `mise run test` only when the current task definition shows it is scoped to safe backend/unit tests; otherwise use an explicit targeted command such as `uv run python -m pytest backend/ -q --ignore-glob='**/test_*_integration.py'`.
   - Do not proactively run local E2E, API, component, Docker, testcontainers, or full integration-stack commands such as `mise run test:e2e`, `mise run test:e2e:local`, `mise run test:component`, `mise run test:api`, `docker compose ... integration-tests`, `uv run pytest integration_tests/`, or unscoped `uv run pytest` unless the user explicitly asks and confirms the machine is prepared.
   - Prefer GitHub Actions for E2E/resource-stack validation: push the branch, check `gh pr checks <pr-number> --repo watch-duty/radio-transcription`, and inspect failures with `gh run view <run-id> --repo watch-duty/radio-transcription --log`.
   - If the user explicitly requests a large local test, run the narrowest path possible, keep Docker/testcontainers/API/E2E/component tests serial with `-n 0`, and avoid `-n auto` unless xdist fanout is specifically requested.
   - Reason: local E2E/resource-stack tests can start many containers and emulators, including AlloyDB Omni, Pub/Sub, GCS, Redis, and pipeline services. These runs have previously exhausted local machine resources; CI runners isolate those resource costs.

4. **Git Commits**:
   - Use descriptive semantic commit prefixes (e.g., `feat(transcription):`, `fix(pipeline):`, `style(transcription):`, `docs:`).
   - **Resource Limits & Sandboxes**: If committing inside a resource-restricted sandbox environment where local git hooks fail due to memory or process limits (exit codes `137` / `-9`), stage changes with `git add -u` and commit utilizing the `--no-verify` flag. The remote GitHub Action CI will perform final validation.

5. **Pull Request Title Standards**:
   - When creating or submitting a Pull Request on GitHub, you MUST prefix the PR title in brackets to satisfy the remote Linear check.
   - **Supported Prefix Formats**:
     - If the work corresponds to a tracked Linear ticket, prefix the PR title with the exact issue ID (e.g. `[GOO-123] feat: implementation...`).
     - For document updates, chores, metadata, or minor changes that do not warrant a ticket, you MUST prefix the PR title with `[ENG-ONLY]` or `[DEV-ONLY]` (e.g., `[ENG-ONLY] docs: update instructions`).
     - Failure to provide one of these prefixes will cause the remote GitHub Actions title validation check to fail immediately.
