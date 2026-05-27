# Agent Instructions

Read and follow [.agents/instructions.md](.agents/instructions.md) before
making code changes or reviewing code in this repository.

Pay special attention to the Python test-safety guidance there. This repository
has resource-heavy Docker/testcontainers and E2E lanes; agents should prefer
targeted low-resource checks locally and GitHub Actions for full E2E validation.
