# Chonk Reducer --- Agent Rules

This document defines the rules that AI coding agents (Codex, Copilot,
ChatGPT, etc.) must follow when working on this repository.

Agents must read and follow these rules **before making any changes**.

The purpose of these rules is to protect production NAS environments,
ensure stable transcoding behavior, and maintain consistent
architecture.

------------------------------------------------------------------------

# Core Development Principles

1.  **Never break existing functionality.**
2.  **Prefer small, focused changes.**
3.  **Do not modify unrelated files.**
4.  **Preserve operator trust.**
5.  **Explain changes with a short summary.**

------------------------------------------------------------------------

# Testing Rules

6.  All code changes must include or update tests.
7.  Tests must pass before completing work.

Run: pytest -q

8.  Never remove tests unless clearly invalid.
9.  Changes affecting encoding, scanning, scheduling, candidate
    selection, or settings must include tests.
10. Add regression tests when fixing bugs.

------------------------------------------------------------------------

# Documentation Rules

11. README.md must always reflect the current behavior of the project.
12. New features must include documentation covering:

-   purpose
-   configuration
-   operator behavior
-   examples if helpful

13. Settings descriptions should be written so they can be reused for UI
    tooltips.

------------------------------------------------------------------------

# Versioning Rules

14. Always increment the application version when code changes.

Update: src/chonk_reducer/**init**.py compose.yaml (if applicable)

Semantic versioning:

patch -\> bug fixes\
minor -\> new features\
major -\> breaking changes

------------------------------------------------------------------------

# Dependency Rules

15. If new dependencies are required update: requirements.txt\
    requirements-dev.txt

16. Development-only tools belong in requirements-dev.txt.

------------------------------------------------------------------------

# Python Compatibility

17. Runtime targets Python 3.11+ (tested on 3.11--3.12).

Use standard timezone support:

from zoneinfo import ZoneInfo

------------------------------------------------------------------------

# Performance & File Handling

18. Never load large media files fully into memory.
19. Prefer streaming/line‑by‑line processing for logs and large files.
20. Long-running tasks must run in background workers, not the UI
    thread.

------------------------------------------------------------------------

# Metrics and Database System

21. SQLite is the system backend.

Database location:

/config/chonk.db

22. Database stores operational data including:

-   runs
-   run_files
-   settings
-   libraries
-   activity
-   job queue

23. NDJSON metrics are deprecated.

Legacy file:

.chonkstats.ndjson

exists only for migration purposes.

------------------------------------------------------------------------

# Migration Rules

24. Migrations must be safe and idempotent.
25. After migration rename legacy files to:

.chonkstats.ndjson.migrated

26. Migrations must never create duplicate records.

------------------------------------------------------------------------

# Configuration Architecture

Chonk uses three configuration layers.

## Docker Compose (Deployment Layer)

Compose should contain:

-   container runtime configuration
-   mounts
-   hardware devices
-   ports
-   environment bootstrap
-   secret keys

Examples:

CHONK_SECRET_KEY\
TZ\
WORK_ROOT\
STATS_PATH\
SERVICE_HOST\
SERVICE_PORT

Operational tuning settings must NOT live here.

------------------------------------------------------------------------

## Global Settings (Application Defaults)

Stored in SQLite.

Examples:

-   retry behavior
-   skip rules
-   logging retention
-   validation behavior
-   notification configuration

Editable via the UI.

------------------------------------------------------------------------

## Library Settings (Per‑Library Overrides)

Examples:

-   schedule
-   min size
-   max files per run
-   encode overrides

Override global defaults when present.

------------------------------------------------------------------------

# Scheduler Architecture

27. Chonk runs as a long‑running service.
28. Scheduler and dashboard must use the same timezone logic.
29. Dashboard "Next Run" must match the actual scheduled trigger.
30. Scheduler bugs are high severity.

------------------------------------------------------------------------

# Queue and Worker Model

31. Encoding work must run in a background queue.
32. Web UI must never block during encoding.
33. Dashboard must stay responsive while runs execute.

------------------------------------------------------------------------

# Encoding and File Safety

34. Never corrupt original media files.
35. Failed encodes must not overwrite originals.
36. Temporary files must be cleaned safely.
37. Successful encodes must preserve streams and metadata unless
    explicitly configured otherwise.

------------------------------------------------------------------------

# Notifications

38. Supported notifications may include:

-   Discord webhooks
-   generic webhooks

39. Secrets must be encrypted in the database.

Encryption requires:

CHONK_SECRET_KEY

40. Secrets must never be stored plaintext after encryption is enabled.

------------------------------------------------------------------------

# UI / Dashboard Rules

41. UI changes must not break operator workflows.
42. Dashboard must remain responsive during runs.
43. Settings pages should include tooltips or help text.
44. Avoid heavy frontend frameworks.

------------------------------------------------------------------------

# Container Runtime

45. Typical deployment workflow:

git pull\
pytest\
docker compose build\
docker compose up -d

46. Config directory must persist:

/config

47. Database and logs must survive container restarts.

------------------------------------------------------------------------

# Data Integrity Rules

48. Historical run records must never be lost.
49. Encode statistics must remain historically accurate.
50. Activity logs must remain reliable.

------------------------------------------------------------------------

# Final Implementation Requirements

Before finishing work an agent must:

1.  Ensure tests pass
2.  Ensure README is accurate
3.  Ensure version numbers are updated
4.  Provide a short implementation summary

------------------------------------------------------------------------

# Purpose

These rules ensure:

-   stable NAS deployments
-   predictable transcoding behavior
-   reliable metrics
-   maintainable architecture
