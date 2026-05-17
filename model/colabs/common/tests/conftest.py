"""pytest configuration for the common/ test suite.

Module-scope only — these are pure unit tests over in-memory data; no Docker,
no database, no env-var injection is needed (unlike the backend conftest).
"""
