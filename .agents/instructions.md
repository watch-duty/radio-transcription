# Workspace Instructions

> [!IMPORTANT]
> Always read and parse the following project style guides before making code changes or reviewing code in this repository:
> - Python Style: [PYTHON_STYLE.instructions.md](../.github/instructions/PYTHON_STYLE.instructions.md)
> - JS/TS Style: [JS_TS_STYLE.instructions.md](../.github/instructions/JS_TS_STYLE.instructions.md)

Refer to these guidelines to ensure consistency with project standards.

---

## Agent Action Standards

To ensure absolute consistency with Watch Duty's development guidelines, always utilize the project's **mise** task runner instead of running ad-hoc commands:

1. **Code Formatting & Linting**:
   - Run `mise run format` locally to automatically format all modified files (Python, Terraform, Frontend) before submitting.
   - Run `mise run lint` locally to execute all code quality checks, formatting linters, and strict type checks.

2. **Protobuf Generation**:
   - Run `mise run generate:protos` to regenerate Python Protobuf/gRPC bindings after making changes to any `.proto` schemas.

3. **Unit Tests**:
   - Run `mise run test` (or `mise run test:unit`) to verify the local Python unit test suite.

4. **Git Commits**:
   - Use descriptive semantic commit prefixes (e.g., `feat(transcription):`, `fix(pipeline):`, `style(transcription):`, `docs:`).
   - **Resource Limits & Sandboxes**: If committing inside a resource-restricted sandbox environment where local git hooks fail due to memory or process limits (exit codes `137` / `-9`), stage changes with `git add -u` and commit utilizing the `--no-verify` flag. The remote GitHub Action CI will perform final validation.