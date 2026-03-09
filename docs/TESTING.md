# Chonk Reducer --- Testing Guide

This document describes how to test the Chonk Reducer project safely and
consistently.

It supplements: - docs/AGENT_RULES.md - README.md

Follow these rules when writing or modifying tests.

------------------------------------------------------------------------

# Running Tests

Install development dependencies:

    pip install -r requirements-dev.txt

Run the full test suite:

    pytest -q

All tests must pass before committing changes.

------------------------------------------------------------------------

# Continuous Integration

The GitHub CI pipeline automatically runs:

    pytest -q

Pull requests must pass CI before merging.

Developers should always run tests locally before pushing changes.

------------------------------------------------------------------------

# Test Design Goals

Tests should:

-   Run quickly
-   Not require real media files
-   Not require FFmpeg execution
-   Not depend on NAS storage
-   Not depend on Docker runtime
-   Be deterministic and repeatable

Prefer small focused unit tests over integration tests.

------------------------------------------------------------------------

# Filesystem Testing

Use pytest temporary directories rather than hardcoded paths.

Example:

    def test_example(tmp_path):
        test_file = tmp_path / "sample.txt"
        test_file.write_text("hello")
        assert test_file.exists()

This ensures tests remain isolated and do not affect real files.

------------------------------------------------------------------------

# Synology / NAS Constraint

Many Synology systems mount `/tmp` with the `noexec` flag.

This means tests that create executable files cannot run from `/tmp`.

When tests require executable scripts or shims:

-   Use pytest's `tmp_path` fixture
-   Ensure the file is created inside the project test directory or
    tmp_path
-   Explicitly mark executable permissions

Example:

    script = tmp_path / "shim.sh"
    script.write_text("#!/bin/sh\necho test")
    script.chmod(0o755)

Do not rely on `/tmp` directly.

------------------------------------------------------------------------

# Mocking External Tools

The system normally uses:

-   ffmpeg
-   ffprobe

Tests must not require these binaries.

Instead, mock subprocess calls or replace command execution with
controlled outputs.

Example using monkeypatch:

    monkeypatch.setattr(module, "run_ffmpeg", fake_function)

------------------------------------------------------------------------

# Database Testing

SQLite is used for runtime metrics.

Tests should:

-   Use temporary database files
-   Avoid touching real production databases
-   Verify schema migrations and data writes

Example:

    db_path = tmp_path / "test.db"

------------------------------------------------------------------------

# Environment Variables

Configuration is driven by environment variables.

Tests may override values using:

    monkeypatch.setenv("VARIABLE_NAME", "value")

This allows tests to simulate different runtime configurations.

------------------------------------------------------------------------

# Performance Rules

Tests must not:

-   Scan large directories
-   Load large files into memory
-   Execute real encodes

If large-file behavior must be tested, simulate it with small dummy
files.

------------------------------------------------------------------------

# Writing New Tests

When adding a feature:

1.  Add unit tests validating behavior
2.  Ensure edge cases are covered
3.  Ensure existing tests still pass

Example workflow:

    git checkout -b feature-branch
    pytest -q
    git commit -m "Add feature + tests"

------------------------------------------------------------------------

# Test Stability

Tests must remain stable across:

-   Linux environments
-   Docker containers
-   NAS environments

Avoid assumptions about:

-   filesystem layout
-   available system binaries
-   host OS configuration

------------------------------------------------------------------------

# Final Rule

Before completing any change:

1.  Run the full test suite
2.  Confirm pytest passes
3.  Ensure new behavior is covered by tests
