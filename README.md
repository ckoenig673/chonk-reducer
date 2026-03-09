
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

**Current Version:** v1.32.0

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

# Notifications

Chonk Reducer can send webhook alerts when a run completes or fails.

Supported notification targets:

- Discord webhook
- Generic HTTP webhook

Configure notifications from the **Settings** page:

- `Discord Webhook URL` (optional, encrypted at rest)
- `Generic Webhook URL` (optional, encrypted at rest)
- `Enable Run Complete Notifications`
- `Enable Run Failure Notifications`

Notes:

- If both webhook URLs are empty, notifications are effectively disabled.
- Webhook URL values are encrypted before being written to SQLite (using `CHONK_SECRET_KEY`) and decrypted only at send time.
- The Settings UI does not re-render plaintext secrets after save; it shows masked/hidden status and supports explicit clear + replace behavior.
- Secret-backed webhook values are stored exactly as entered (after trimming leading/trailing whitespace and removing accidental CR/LF), without HTML/entity mutation.
- The Settings page includes a `Send Test Notification` action for operator verification.
- Notification delivery failures are non-fatal and only logged as warnings.
- Notifications are sent once per run completion/failure event in service mode.
- If `CHONK_SECRET_KEY` is changed or lost, existing encrypted webhook values may need to be re-entered.
- Discord webhook URLs are accepted for both `discord.com` and legacy `discordapp.com` hosts; legacy URLs are normalized to `discord.com` at send time.
- Webhook requests send `Content-Type: application/json` and a fixed `User-Agent` (`ChonkReducer/1.x`) to keep transport behavior predictable.
- Webhook delivery ignores ambient proxy environment variables by default; set `CHONK_WEBHOOK_USE_PROXY=1` to opt-in to urllib's proxy behavior.

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
  - `/favicon.ico` (returns `204 No Content` to prevent browser tab spinner hangs)
- Fallback built-in HTTP mode now uses a threaded server so dashboard, favicon, and other small requests stay responsive while background jobs are actively running
- Runtime environment mutation now uses short lock windows (set/restore only), so `/dashboard` and `/runs` remain refreshable while an active run is processing in the background.
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

- Library status cards stacked by enabled library
- Each card shows library name, path, current runtime status (`Idle`, `Queued`, or `Running`), last run, next run (local scheduled timestamp like `2026-03-14 02:00` when available, `Not Scheduled` when schedule is missing/invalid, or `Disabled` when the library is disabled), lifetime totals (`Files Optimized`, `Total Saved`) from SQLite `encodes`, and recent savings from the latest SQLite `runs` entry
- Next-run display uses the library row cron value as the source of truth and computes the upcoming timestamp even when scheduler job metadata is unavailable
- **Run Now** and **Preview Run** controls per enabled library (dashboard forms post to `POST /dashboard/libraries/{library_id}/run` and `POST /dashboard/libraries/{library_id}/preview`, queue work, then redirect to `/dashboard`; JSON API remains available at `POST /libraries/{library_id}/run`)
- Manual Run Now and scheduled triggers now enqueue background jobs instead of running inline
- Single-worker in-memory queue prefers higher-priority libraries first when multiple jobs are queued (higher integer wins), while preserving FIFO behavior for equal priorities
- Legacy compatibility run routes remain available for default Movies/TV libraries
- Recent Runs table (from SQLite `runs`)
- Lifetime savings summary
- Current runtime status block (idle/queued/running, current library, trigger, scheduler status/started time, next global scheduled job/time, mode, queue depth, run id, started timestamp, current file, and lightweight live run snapshot counters)
- Active runs now render a lightweight progress panel with an HTML progress bar. During active ffmpeg encode the panel now also shows encoding percent complete (`out_time_ms / duration_ms`), live speed (`speed`), and ETA, while still showing file counters and current file/library context
- Dashboard runtime status now auto-refreshes every 3 seconds using `GET /api/status` so Current Job Status + Run Progress update live without full page refresh
- Preview Run now submits through the dashboard preview endpoint and keeps the latest preview snapshot visible after completion, labeled with preview library and generated timestamp until the next preview run replaces it
- Active runs show a **Stop Run** button that calls `POST /api/run/cancel`; runtime status transitions through `Cancelling` and then `Cancelled` once the worker stops
- Library cards show inline running progress (`Progress: processed / candidates files`) for the currently active library
- Runtime progress snapshot clears after run completion so idle dashboards return to baseline

- Preview runs (Dry Run mode): scan and candidate selection are unchanged; ffprobe still runs; estimated output size and estimated savings are calculated from current QSV quality/preset settings; decisions are reported as `Encode`, `Skip (below savings threshold)`, `Skip (unsupported codec)`, or `Skip (resolution rules)`
- Preview mode never writes output files, never renames media, and never deletes files
- Dashboard shows a **Preview Results** table (first 25 files) with preview library, generated timestamp, file path, original size, estimated size, estimated savings %, and decision

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
- `retry_backoff_seconds`
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
- per-library `priority` integer (higher runs first when multiple libraries are queued)

Library schedule editing supports two operator modes:

- **Simple schedule builder**: pick weekdays (`Su`, `M`, `T`, `W`, `Th`, `F`, `Sa`) and a time dropdown (15-minute increments), then Chonk generates the cron string for storage using named weekdays (`sun`..`sat`) for unambiguous APScheduler behavior.
- **Advanced raw cron**: edit the raw cron expression directly for complex or custom schedules.

Cron remains the internal scheduler storage format. If a saved cron expression matches the simple weekly pattern (single time + weekday list), the UI opens in simple mode and pre-populates day/time controls. Both legacy numeric weekday values (`0`-`7`) and named weekday values (`sun`..`sat`) are accepted for backward compatibility, and simple schedules are normalized safely at runtime. Unsupported or complex cron expressions automatically fall back to advanced mode and keep the raw cron value editable without rewriting.

Bootstrap model for the new config foundation:

1. Environment/compose values remain bootstrap defaults.
2. Missing service settings keys are initialized from env/default values.
3. If no `libraries` rows exist, default `Movies` and `TV` rows are initialized with safe processing defaults (`min_size_gb=0.0`, `max_files=1`, `priority=100`) and schedule bootstrap from legacy global schedule keys when present.

Retry behavior is automatic for failed encodes: Chonk retries the same source file up to `retry_count`, waits `retry_backoff_seconds` between attempts, cleans up incomplete temp output between attempts, and marks the file failed when retries are exhausted.

For retries, environment/compose values are now bootstrap-only compatibility input (`RETRY_COUNT`, `RETRY_BACKOFF_SECONDS`, legacy `RETRY_BACKOFF_SECS`). After bootstrap, SQLite Global Settings are the source of truth for runtime behavior.

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
- job queued/job started
- manual and scheduled run requests
- queued job start/completion and queue rejections
- run start/completion
- duplicate queued/running rejections

Raw detailed run logs are unchanged and still written to log files. The Activity page is intentionally a small recent-events view, not a full raw log replacement.

System page:

```bash
open http://localhost:8080/system
```

The System page provides lightweight operator visibility into the running service, including:

- service/runtime information (version, host/port, service mode)
- scheduler status
- current background job status (idle/queued/running with queue depth)
- configured schedules for enabled libraries
- next scheduled run times per enabled library and the global next scheduled job/time derived from configured enabled-library schedules
- current queue/worker status (status, current library, trigger, queue depth, current run id, started-at)
- SQLite database path and runtime/work path visibility

It is intentionally minimal and is not a full diagnostics framework.

Settings precedence / bootstrap model:

1. Environment/compose values are bootstrap defaults.
2. On first startup, missing service settings rows are initialized from those environment values.
3. On first startup, missing `libraries` rows are initialized from current Movie/TV media roots, with per-library processing defaults (`min_size_gb=0.0`, `max_files=1`) and per-library encoding defaults (`qsv_quality`, `qsv_preset`, `min_savings_percent`) bootstrapped from current env/compose values.
4. After initialization, library rows in SQLite are the runtime source of truth for per-library processing and encoding behavior.

Scheduler notes:

- Library schedules are owned by DB-backed library rows (`libraries.schedule`).
- Global Settings do not include schedule fields.
- Enabled libraries with schedules are auto-registered with the scheduler.
- Both scheduler and manual run requests share the same queue-backed execution path.
- Enabled libraries with blank schedules are not auto-scheduled and are manual-run only.
- Disabled libraries are excluded from runtime scheduling and manual-run controls.
- Queue model is intentionally minimal for now: single worker, in-memory queue, no cancellation/retries persistence/progress parsing.
- `MOVIE_SCHEDULE` / `TV_SCHEDULE` remain optional legacy/bootstrap env defaults used only during first-time bootstrap.
- If schedules are changed in Libraries, restart the service for new cron schedules to be applied.

Service scheduler environment variables:

- `SERVICE_MODE=true|false` (default: false)
- `SERVICE_HOST` (default: `0.0.0.0`)
- `SERVICE_PORT` (default: `8080`)
Library-specific service overrides (optional):

- `MOVIE_MEDIA_ROOT`, `MOVIE_LOG_PREFIX`, `MOVIE_LIBRARY`
- `TV_MEDIA_ROOT`, `TV_LOG_PREFIX`, `TV_LIBRARY`

If `SERVICE_MODE` is unset/false, Chonk keeps the existing one-shot behavior.

Notification secret key:

- `CHONK_SECRET_KEY` must be set when saving or using encrypted notification webhook settings.
- Use a strong random passphrase value (for example, `openssl rand -base64 32`).

---


# Scheduling Model (Recommended)

Example balanced schedule:

| Library | Frequency |
|-------|--------|
| TV | 3 days per week |
| Movies | 3 days per week |

Run on alternating days.

Set per-library **Max Files Per Run** in **Settings → Libraries → Edit Library** to control runtime for each library independently.

Per-library processing fields in the library create/edit forms:

- **Minimum File Size (GB)** (`min_size_gb`): files smaller than this value are skipped for that library.
- **Max Files Per Run** (`max_files`): a run for that library stops after this many files.

Per-library encoding fields in the library create/edit forms (**Encoding Settings**):

- **QSV Quality** (`qsv_quality`): integer quality value used for that library run.
- **QSV Preset** (`qsv_preset`): integer preset value used for that library run.
- **Minimum Savings Percent** (`min_savings_percent`): numeric threshold used for that library run.

Defaults for existing and new libraries are:

- `min_size_gb = 0.0`
- `max_files = 1`
- `qsv_quality = QSV_QUALITY` env default (fallback `21`)
- `qsv_preset = QSV_PRESET` env default (fallback `7`)
- `min_savings_percent = MIN_SAVINGS_PERCENT` env default (fallback `15`)

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
MIN_FILE_AGE_MINUTES | 0 | skip recently modified files |
MIN_SAVINGS_PERCENT | 15 | minimum savings |
MAX_SAVINGS_PERCENT | 0 | optional savings cap |
ENCODER | hevc_qsv | encoder profile |
QSV_QUALITY | 21 | encoding quality |
QSV_PRESET | 7 | speed preset |
PROBE_TIMEOUT_SECS | 60 | ffprobe timeout |
RETRY_COUNT | 1 | bootstrap default for DB-backed retry count |
RETRY_BACKOFF_SECONDS | 5 | bootstrap default for DB-backed retry delay (seconds) |
PREVIEW | false | dry run preview mode: analyze/probe/estimate only, never transcode |

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
