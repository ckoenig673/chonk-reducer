
![License](https://img.shields.io/github/license/ckoenig673/chonk-reducer)
![Release](https://img.shields.io/github/v/release/ckoenig673/chonk-reducer)
![CI](https://github.com/ckoenig673/chonk-reducer/actions/workflows/ci.yml/badge.svg)
![Issues](https://img.shields.io/github/issues/ckoenig673/chonk-reducer)
![Last Commit](https://img.shields.io/github/last-commit/ckoenig673/chonk-reducer)

# Chonk Reducer

```
        ▄████▄   ██░ ██  ▒█████   ███▄    █  ██ ▄█▀
       ▒██▀ ▀█  ▓██░ ██▒▒██▒  ██▒ ██ ▀█   █  ██▄█▒
       ▒▓█    ▄ ▒██▀▀██░▒██░  ██▒▓██  ▀█ ██▒▓███▄░
       ▒▓▓▄ ▄██▒░▓█ ░██ ▒██   ██░▓██▒  ▐▌██▒▓██ █▄
       ▒ ▓███▀ ░░▓█▒░██▓░ ████▓▒░▒██░   ▓██░▒██▒ █▄
       ░ ░▒ ▒  ░ ▒ ░░▒░▒░ ▒░▒░▒░ ░ ▒░   ▒ ▒ ▒ ▒▒ ▓▒
         ░  ▒    ▒ ░▒░ ░  ░ ▒ ▒░ ░ ░░   ░ ▒░░ ░▒ ▒░
       ░         ░  ░░ ░░ ░ ░ ▒     ░   ░ ░ ░ ░░ ░
       ░ ░       ░  ░  ░    ░ ░           ░ ░  ░

                Reduce the Chonk. Respect the Bits.
```

**Current Version:** v1.25.0

Chonk Reducer is a **policy‑driven NAS media optimization pipeline** designed for **Synology + Docker environments**.

It safely re‑encodes oversized media files using **Intel QSV HEVC**, validates the results, atomically swaps files, and records operational statistics.

The goal is simple:

> **Reclaim disk space safely without breaking media libraries.**

---

# Quick Start

Clone the repo:

```bash
git clone https://github.com/ckoenig673/chonk-reducer
cd chonk-reducer
```

Run a container job manually:

```bash
docker compose run --rm movie-transcoder
```

Run health checks:

```bash
docker compose run --rm tv-transcoder healthcheck
docker compose run --rm movie-transcoder healthcheck
```

Most deployments schedule the job using **Synology DSM Task Scheduler**.

---

# Designed for Arr‑Based Media Environments

Chonk Reducer is built for NAS media stacks that use the **Arr ecosystem**.

Common automation tools:

- Radarr
- Sonarr
- Lidarr

These tools manage **downloading, importing, and organizing media libraries**.

Chonk Reducer runs **alongside them** by optimizing large media files **after they are imported**.

Typical workflow:

```
Downloader → Arr (Radarr/Sonarr/Lidarr) → Media Library
                                   ↓
                             Chonk Reducer
                       (optimize large files)
```

Because Chonk operates directly on files and preserves filenames, it **does not interfere with Arr library management**.

It simply reduces storage usage.

---

# Roadmap

Future improvements and planned features:

https://github.com/ckoenig673/chonk-reducer/issues?q=is%3Aissue+is%3Aopen+label%3Aroadmap

---

# What Chonk Does

Chonk Reducer runs a controlled pipeline:

• scans media folders  
• identifies oversized files  
• probes metadata with ffprobe  
• encodes using Intel QSV HEVC  
• validates output  
• enforces savings thresholds  
• atomically replaces the original file  
• stores metrics and run statistics  

Key behavior:

- Scans for `*.mkv` candidates
- Skips folders containing `.chonkignore`
- Supports `.chonkpause` to halt jobs instantly
- Encodes **in the same directory as the source file**
- Validates output via decode testing
- Requires minimum compression savings
- Backs up originals with timestamped `.bak`
- Writes `.optimized` marker to prevent reprocessing
- Retries transient failures
- Marks permanent failures with `.failed`
- Records detailed metrics in SQLite

---

# Run Summary Counters

Each run records stage counters:

- Candidates found
- Pre‑filtered
- Evaluated
- Processed
- Succeeded
- Skipped
- Failed

These counters are stored in the **SQLite stats database** for later analysis.

---

# High Level Architecture

```
DSM Task Scheduler
    → scripts/chonkreducer_task.sh
        → docker compose run --rm --no-deps <service>
            → python -m chonk_reducer
                → runner
                    → cleanup
                    → discovery
                    → probe
                    → encode
                    → validate
                    → swap
                    → metrics
```

Each run can run in one of two modes:

- **One-shot mode (existing):** stateless and isolated runs (DSM task friendly).
- **Service mode (new):** a long-running container that schedules movie/TV runs internally and exposes a health endpoint.

---

# Folder Markers

### .chonkignore

Exclude folders from processing.

Example:

```
/tv/Sports/.chonkignore
```

---

### .chonkpause

Immediately stop processing for a library.

Example:

```
/tv/.chonkpause
```

Optional:

```
/tv/.chonkpause.reason
```

---

### .optimized

Written after a successful encode swap.

Prevents future reprocessing.

---

### .failed

Written when encoding fails after retries.

Prevents repeated failures.

---


# Encode History Page

The service UI includes a **History** page at `/history`.

It shows completed encode jobs from SQLite, including:

- Library
- File name
- Original size
- New size
- Savings percent
- Savings amount
- Date/time

This gives operators quick visibility into what Chonk processed and how much storage was reclaimed.

---

# Docker Usage

Health check:

```bash
docker compose run --rm tv-transcoder healthcheck
docker compose run --rm movie-transcoder healthcheck
```

Manual run:

```bash
docker compose run --rm tv-transcoder
docker compose run --rm movie-transcoder
```

Synology DSM task example:

```bash
/bin/sh scripts/chonkreducer_task.sh tv-transcoder
/bin/sh scripts/chonkreducer_task.sh movie-transcoder
```

---

---

# Long-Running Service Mode (Arr-Style Foundation)

Chonk Reducer supports an optional long-running service mode for internal scheduling and operator workflows.

- Existing DSM Task Scheduler + one-shot container runs are still supported.
- Service mode now uses an Arr-style shell with persistent left navigation.
- Routes available in this foundation release:
  - `/dashboard`
  - `/runs`
  - `/runs/{run_id}`
  - `/history`
  - `/activity`
  - `/settings`
  - `/system`
- `/` renders the dashboard in the new shell.

Enable service mode:

```bash
docker compose up -d chonk-service
```

Health endpoint:

```bash
curl http://localhost:8080/health
```

Returns:

```json
{"status":"ok"}
```

Dashboard:

```bash
open http://localhost:8080/dashboard
```

The dashboard preserves existing operator controls and visibility:

- One manual run control per enabled library (`POST /libraries/{library_id}/run`)
- Legacy compatibility run routes remain available for default Movies/TV libraries
- Last run status is shown per enabled library
- Recent Runs table (from SQLite `runs`)
- Lifetime savings summary

Runs page:

```bash
open http://localhost:8080/runs
```

The Runs page is backed by the SQLite `runs` table and provides a recent run history view across libraries.

Runs now include links to a Run Detail page for each `run_id`, which shows per-run summary data from `runs` plus file-level entries from `encodes`.

Run Detail also surfaces the raw log file path when available, so operators can quickly jump from Activity → Run Detail → raw log file on disk.

It is intentionally minimal and currently focuses on operator history visibility (status, counts, duration, saved space, and raw log path visibility).

Detailed raw logs remain in log files and are unchanged (the UI is not a full log viewer).

Settings page:

```bash
open http://localhost:8080/settings
```

The settings page is backed by SQLite (`STATS_PATH`) and now has two sections:

- **Global Settings** (DB-backed key/value settings)
- **Libraries** (DB-backed library rows)

Global Settings now manage app-wide operator defaults (DB-backed), including:

- `min_file_age_minutes`
- `max_files`
- `min_savings_percent`
- `max_savings_percent`
- `retry_count`
- `retry_backoff_secs`
- `skip_codecs`
- `skip_resolution_tags`
- `skip_min_height`
- `validate_seconds`
- `log_retention_days`
- `bak_retention_days`

Saving settings writes values to SQLite immediately and applies them to service-driven runs.

Libraries are now persisted in a `libraries` table and support simple operator CRUD:

- create library
- edit library
- delete library
- enable/disable library

Library schedule editing supports two operator modes:

- **Simple schedule builder**: pick weekdays (`Su`, `M`, `T`, `W`, `Th`, `F`, `Sa`) and a time dropdown (15-minute increments), then Chonk generates the cron string for storage.
- **Advanced raw cron**: edit the raw cron expression directly for complex or custom schedules.

Cron remains the internal scheduler storage format. If a saved cron expression matches the simple weekly pattern (single time + weekday list), the UI opens in simple mode and pre-populates day/time controls. Unsupported or complex cron expressions automatically fall back to advanced mode and keep the raw cron value editable without rewriting.

Bootstrap model for the new config foundation:

1. Environment/compose values remain bootstrap defaults.
2. Missing global settings keys are initialized from env/default values.
3. If no `libraries` rows exist, default `Movies` and `TV` rows are initialized from current env/library values (including schedule bootstrap from legacy global schedule keys when present).

Runtime note: service scheduling and manual execution are now driven by enabled library rows in SQLite (`libraries`), not a fixed Movies/TV runtime model. Enabled libraries with blank schedules remain manual-run only until a schedule is configured.

Activity page:

```bash
open http://localhost:8080/activity
```

The Activity page is a lightweight operator-facing event feed stored in SQLite (`activity_events` table in `STATS_PATH`).

Activity entries that include a `run_id` render that value as a link to Run Detail (`/runs/{run_id}`).

It includes recent service events such as:

- service startup
- scheduler start
- schedule registration
- manual and scheduled run requests
- run start/completion
- busy overlap rejections

Raw detailed run logs are unchanged and still written to log files. The Activity page is intentionally a small recent-events view, not a full raw log replacement.

System page:

```bash
open http://localhost:8080/system
```

The System page provides lightweight operator visibility into the running service, including:

- service/runtime information (version, host/port, service mode)
- scheduler status
- configured schedules for enabled libraries
- next scheduled run times per enabled library when available from scheduler metadata
- SQLite database path and runtime/work path visibility

It is intentionally minimal and is not a full diagnostics framework.

Settings precedence / bootstrap model:

1. Environment/compose values are bootstrap defaults.
2. On first startup, missing global settings rows are initialized from those environment values.
3. On first startup, missing `libraries` rows are initialized from current fixed Movie/TV env/library defaults (and legacy global schedule keys when present).
4. After initialization, SQLite values are used by service-driven runs.

Scheduler notes:

- Library schedules are owned by DB-backed library rows (`libraries.schedule`).
- Global Settings do not include schedule fields.
- Enabled libraries with schedules are auto-registered with the scheduler.
- Enabled libraries with blank schedules are not auto-scheduled and are manual-run only.
- Disabled libraries are excluded from runtime scheduling and manual-run controls.
- `MOVIE_SCHEDULE` / `TV_SCHEDULE` remain optional legacy/bootstrap env defaults used only during first-time bootstrap.
- If schedules are changed in Libraries, restart the service for new cron schedules to be applied.

Service scheduler environment variables:

- `SERVICE_MODE=true|false` (default: false)
- `SERVICE_HOST` (default: `0.0.0.0`)
- `SERVICE_PORT` (default: `8080`)
Library-specific service overrides (optional):

- `MOVIE_MEDIA_ROOT`, `MOVIE_MIN_SIZE_GB`, `MOVIE_LOG_PREFIX`, `MOVIE_LIBRARY`
- `TV_MEDIA_ROOT`, `TV_MIN_SIZE_GB`, `TV_LOG_PREFIX`, `TV_LIBRARY`

If `SERVICE_MODE` is unset/false, Chonk keeps the existing one-shot behavior.

---


# Scheduling Model (Recommended)

Example balanced schedule:

| Library | Frequency |
|-------|--------|
| TV | 3 days per week |
| Movies | 3 days per week |

Run on alternating days.

Set:

```
MAX_FILES=1
```

to control runtime.

---

# Testing

The project uses **pytest**.

Install dev dependencies:

```bash
pip install -r requirements-dev.txt
```

Run tests:

```bash
pytest -q
```

Tests run **without real media files or FFmpeg**.

---

# Development Workflow

Typical workflow:

```bash
git checkout -b my-change
pytest -q
git add .
git commit -m "Describe change"
```

---

# Stats and Metrics

Chonk Reducer stores operational metrics in **SQLite**.

Database tables:

```
runs
encodes
```

These store:

• run counters  
• per‑file results  
• compression statistics  
• operational metrics  

Default database:

```
/config/chonk.db
```

Future versions may support dashboards or reporting tools using this data.

---

# Environment Variables

These are configured via `compose.yaml`.

| Variable | Default | Description |
|---|---|---|
LIBRARY_NAME | | logical library name |
MEDIA_ROOT | /movies | root scan path |
WORK_ROOT | /work | workspace directory |
MAX_FILES | 2 | files processed per run |
MIN_SIZE_GB | 18 | minimum file size |
MIN_FILE_AGE_MINUTES | 0 | skip recently modified files |
MIN_SAVINGS_PERCENT | 15 | minimum savings |
MAX_SAVINGS_PERCENT | 0 | optional savings cap |
ENCODER | hevc_qsv | encoder profile |
QSV_QUALITY | 21 | encoding quality |
QSV_PRESET | 7 | speed preset |
PROBE_TIMEOUT_SECS | 60 | ffprobe timeout |
RETRY_COUNT | 1 | retry attempts |

Additional settings control validation, logging, stats, and retention.

---

# Outputs / Artifacts

During processing the pipeline creates:

```
Episode.mkv.timestamp.encoded.mkv
Episode.mkv.bak.timestamp
Episode.mkv.optimized
Episode.mkv.failed
```

Logs:

```
/work/logs/transcode_*.log
```

Reports:

```
/work/reports/chonk_weekly_*.txt
```

---

# Project Layout

```
src/chonk_reducer
tests
scripts
work
work/logs
```

---

# Troubleshooting

If ffprobe is unavailable on the host:

```bash
docker compose run --rm --entrypoint ffprobe tv-transcoder <args>
```

---

# Keywords

NAS media optimization  
homelab media stack  
Radarr Sonarr Lidarr  
Intel QSV transcoding  
Docker media automation  
