# Chonk Reducer --- Agent Rules

This document defines the rules that AI coding agents (Codex, Copilot,
ChatGPT, etc.) must follow when working on this repository.

Agents must read and follow these rules **before making any changes**.

The purpose of these rules is to protect production NAS environments,
ensure stable transcoding behavior, and maintain consistent
architecture.

------------------------------------------------------------------------

# Core Development Principles

1.  Never break existing functionality.
2.  Prefer small, focused changes.
3.  Do not modify unrelated files.
4.  Preserve operator trust.
5.  Provide a short implementation summary describing changes.

------------------------------------------------------------------------

# Testing Rules

6.  All code changes must include or update tests when appropriate.
7.  Tests must pass before completing work.

Run: pytest -q

8.  Never remove tests unless clearly invalid.
9.  Changes affecting encoding, scanning, scheduling, candidate
    selection, or settings behavior must include tests.
10. Add regression tests when fixing bugs.

------------------------------------------------------------------------

# Documentation Rules

11. README.md must reflect the **actual current behavior** of the
    project.
12. New features must include documentation covering:

-   purpose
-   configuration
-   operator behavior
-   examples if helpful

13. Settings descriptions should be written so they can later be reused
    for UI tooltips/help text.

------------------------------------------------------------------------

# Versioning Rules

14. Always increment the application version when code changes.

Update: src/chonk_reducer/**init**.py

compose.yaml may expose an APP_VERSION value used by the container.

Semantic versioning:

patch -\> bug fixes\
minor -\> new features\
major -\> breaking changes

Version strings should be consistent.\
Preferred canonical format inside Python code:

1.39.3

Container display versions may include a `v` prefix if required for UI
display.

------------------------------------------------------------------------

# Dependency Rules

15. If new dependencies are required update:

requirements.txt\
requirements-dev.txt

16. Development-only tools belong in requirements-dev.txt.

Avoid unnecessary dependencies.

------------------------------------------------------------------------

# Python Compatibility

17. Runtime targets **Python 3.11+**.

The codebase may contain compatibility fallbacks (for example
backports.zoneinfo) but these should not be required for normal
operation.

Standard timezone usage:

from zoneinfo import ZoneInfo

------------------------------------------------------------------------

# Performance & File Handling

18. Never load large media files fully into memory.
19. Prefer streaming / line-by-line processing for logs and large files.
20. Long-running work must run in background workers rather than
    blocking the web UI thread.

------------------------------------------------------------------------

# Metrics and Database System

21. SQLite is the primary backend for operational state and metrics.

Database location:

/config/chonk.db

22. Current operational tables include:

-   runs
-   encodes
-   settings
-   libraries
-   activity_events

Additional tables may be introduced through migrations when needed.

23. NDJSON metrics are deprecated.

Legacy file:

.chonkstats.ndjson

exists only for migration purposes.

Agents must not reintroduce NDJSON as a primary metrics system.

------------------------------------------------------------------------

# Migration Rules

24. Migrations must be safe and idempotent.

25. After migration legacy stats files must be renamed to:

.chonkstats.ndjson.migrated

26. Migration must never import duplicate records.

------------------------------------------------------------------------

# Configuration Architecture

Chonk uses a three-layer configuration model.

## Docker Compose (Deployment Layer)

Compose should contain deployment/runtime concerns only:

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

Operational tuning settings should not live in compose.

------------------------------------------------------------------------

## Global Settings (Application Defaults)

Stored in SQLite.

Used for system-wide operational behavior such as:

-   retry behavior
-   skip rules
-   validation behavior
-   log retention
-   notification configuration

These settings are editable through the UI.

------------------------------------------------------------------------

## Library Settings (Per-Library Behavior)

Each media library has its own configuration.

Examples:

-   schedule
-   min size
-   max files per run
-   encode overrides

Library settings override global defaults.

------------------------------------------------------------------------

# Scheduler Architecture

27. Chonk runs as a **long-running service**.

28. Library schedules are interpreted using the same timezone logic used
    by the dashboard.

29. Dashboard "Next Run" must match actual scheduler behavior.

30. Scheduler bugs are considered high severity.

------------------------------------------------------------------------

# Queue and Worker Model

31. The runtime uses an **in-memory job queue** with a background worker
    thread to process encoding jobs.

32. Queue state is not currently persisted in the database.

33. The web UI must remain responsive while jobs run.

------------------------------------------------------------------------

# Encoding and File Safety

34. Original media files must never be corrupted.
35. Failed encodes must not overwrite originals.
36. Temporary files must be cleaned up safely.
37. Successful encodes must preserve streams and metadata unless
    explicitly configured otherwise.

------------------------------------------------------------------------

# Notifications

38. Supported notifications may include:

-   Discord webhooks
-   generic webhooks

39. Secrets are stored encrypted in the database.

Encryption requires:

CHONK_SECRET_KEY

40. Secrets must never be stored in plaintext once encryption is
    enabled.

------------------------------------------------------------------------

# UI / Dashboard Rules

41. UI changes must not break operator workflows.
42. Dashboard must remain responsive during runs.
43. Settings pages should include help text or tooltips for clarity.
44. Avoid introducing heavy frontend frameworks.

------------------------------------------------------------------------

# Container Runtime

45. Typical deployment workflow:

git pull\
pytest\
docker compose build\
docker compose up -d

46. The config directory must persist:

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
2.  Ensure README reflects current behavior
3.  Ensure version numbers are updated
4.  Provide a short implementation summary

------------------------------------------------------------------------

# Purpose

These rules ensure:

-   stable NAS deployments
-   predictable transcoding behavior
-   reliable metrics
-   maintainable architecture
