# Chonk Reducer -- Agent Rules

This document defines the rules that AI coding agents (Codex, Copilot,
etc.) must follow when working on this repository.

Agents must read and follow these rules **before making any changes**.

------------------------------------------------------------------------

# General Development Rules

1.  **Never break existing functionality.** If unsure, preserve current
    behavior.

2.  **Prefer small, clear changes** rather than large refactors.

3.  **Do not modify unrelated files.**

4.  **Explain the changes in a short summary** at the end of any
    implementation.

------------------------------------------------------------------------

# Testing Rules

5.  **Always update or add unit tests** for any new feature or code
    change.

6.  **Ensure the project passes tests before finishing.**

Run:

pytest -q

7.  **Never remove existing tests unless they are clearly invalid.**

8.  Any change to **encoding, scanning, or candidate logic** must
    include tests validating the behavior.

------------------------------------------------------------------------

# Documentation Rules

9.  **Always update `README.md` if behavior changes.**

This includes:

-   configuration
-   CLI usage
-   workflows
-   new features
-   environment variables

10. If a new feature is added, document:

-   usage
-   configuration options
-   example commands

------------------------------------------------------------------------

# Versioning Rules

11. **Always increment the application version when code changes.**

Update:

src/chonk_reducer/**init**.py compose.yaml

Follow **semantic versioning**:

patch -\> bug fixes minor -\> new features major -\> breaking changes

------------------------------------------------------------------------

# Dependency Rules

12. If new dependencies are required:

Update:

requirements.txt requirements-dev.txt

Dev tools (pytest plugins, etc.) belong in **requirements-dev.txt**.

------------------------------------------------------------------------

# Python Compatibility

13. **Maintain compatibility with Python 3.8.**

The NAS runtime environment uses Python 3.8.

Avoid features introduced after Python 3.8.

If timezone support is needed, use fallback logic:

try: from zoneinfo import ZoneInfo except ImportError: from
backports.zoneinfo import ZoneInfo

------------------------------------------------------------------------

# Performance & File Handling

14. **Avoid loading large files fully into memory.**

Log files and NDJSON files must be processed **line-by-line
(streaming)**.

Example:

for line in file: process(line)

------------------------------------------------------------------------

# Metrics and Statistics System

15. **SQLite is the metrics backend.**

The legacy `.chonkstats.ndjson` format is deprecated.

All metrics must be stored in:

/config/chonk.db

Tables:

runs encodes

16. **Do not reintroduce NDJSON as a primary metrics system.**

NDJSON exists only for **legacy migration**.

------------------------------------------------------------------------

# Migration Rules

17. Migration must be **safe and idempotent**.

Legacy stats files:

.chonkstats.ndjson

After migration they must be renamed to:

.chonkstats.ndjson.migrated

18. Migration must **not import duplicate records**.

------------------------------------------------------------------------

# Container & Runtime Architecture

19. Chonk runs inside Docker containers scheduled by NAS tasks.

Typical runtime flow:

git pull pytest docker compose build docker compose run

Changes must not break this workflow.

20. The container config directory must persist via Docker volume:

/config

Example mapping:

/volume1/docker/projects/nas-transcoder:/config

SQLite metrics must persist between runs.

------------------------------------------------------------------------

# Data Integrity Rules

21. Successful encodes are critical historical records and must always
    be preserved.

22. Skip events may be stored for operational visibility.

Skip pruning or aggregation may be added in future versions.

------------------------------------------------------------------------

# Final Implementation Requirements

Before finishing any task:

1.  Ensure tests pass.
2.  Ensure documentation is updated.
3.  Ensure version numbers are updated.
4.  Provide a **short implementation summary** describing the changes.

------------------------------------------------------------------------

# Purpose of These Rules

These rules ensure that:

-   production NAS workflows remain stable
-   metrics remain reliable
-   historical data is preserved
-   changes remain testable and maintainable

Agents must follow these rules to maintain the integrity of the Chonk
Reducer project.

