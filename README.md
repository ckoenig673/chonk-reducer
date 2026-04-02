<p align="center">

![License](https://img.shields.io/badge/license-MIT-green)
![Release](https://img.shields.io/github/v/release/ckoenig673/chonk-reducer)
![CI](https://img.shields.io/github/actions/workflow/status/ckoenig673/chonk-reducer/ci.yml)
![Python](https://img.shields.io/badge/python-3.11+-blue)
![Stars](https://img.shields.io/github/stars/ckoenig673/chonk-reducer?style=social)

</p>

# Chonk Reducer

<p align="center">
  <img src="assets/chonk-reducer-logo.png" width="400">
</p>

**Current Version:** v1.46.17

Chonk Reducer is a Docker-first NAS media optimization service. It scans media libraries, evaluates candidates, runs Intel QSV HEVC transcodes when policy allows, validates output, swaps atomically, and records run/file metrics in SQLite.

It supports both:

- **One-shot mode** (default CLI behavior)
- **Long-running service mode** with built-in scheduler + web UI

---

## Code Layout (Structural Refactor)

This release includes a **structural refactor only**: behavior is intended to remain the same while making modules easier to navigate and maintain.

```
src/chonk_reducer/
  main.py
  core/
  web/
    app.py
    routers/
    templates/
    static/
  services/
  data/
    db.py
    repositories/
  scheduler/
  transcoding/
```

Responsibilities:
- `web/`: FastAPI app bootstrap, route registration, plus UI templates/static assets (`web/templates`, `web/static/css`, `web/static/js`).
- `data/`: SQLite setup and repository-oriented data access modules.
- `scheduler/`: scheduler wiring and listener setup helpers.
- `services/`: service orchestration and runtime workflows.
- `core/`: shared runtime helpers such as display/formatting and text-normalization utilities used by service, data, and web rendering paths.
- `transcoding/`: encoding/runner concerns, separate from web routing.

UI extraction note:
- Dashboard, Runs, Run Detail, Analytics, and selected Settings/Library scaffolds are incrementally moving into dedicated templates/partials under `web/templates/` and page-specific CSS under `web/static/css/base.css` to reduce inline presentation markup in Python service code. Recent passes include reusable Settings global-row/message/housekeeping partials, Library form section/name-path partials, extracted Library schedule/create/ignored-folders partials, reusable preview/system housekeeping summary partials, shared Analytics/key-value/common bordered-message partial wrappers, a dedicated `services/settings_libraries_rendering.py` helper for settings/libraries assembly, and a `services/dashboard_rendering.py` helper for dashboard/runs/activity rendering composition.

## Quick Start

```bash
git clone https://github.com/ckoenig673/chonk-reducer
cd chonk-reducer
```

Run a one-shot transcoding job:

```bash
docker compose run --rm chonk-service run
```

Run healthcheck:

```bash
docker compose run --rm chonk-service healthcheck
```

Start long-running service mode:

```bash
docker compose up -d chonk-service
```

Service URLs:

- Dashboard: `http://localhost:8085/dashboard`
- Health: `http://localhost:8085/health`

---

## Current Architecture (Source of Truth)

### Execution model

- Service mode uses a **single in-memory queue** plus **single worker thread**.
- Manual runs, preview runs, and scheduled runs all enqueue jobs through the same path.
- Queue selection is **priority-first** (higher integer wins), then FIFO for ties.
- Duplicate queueing for a library already queued/running is rejected.

### Scheduler behavior

- Schedules are owned by **library rows in SQLite** (`libraries.schedule`).
- Only **enabled libraries** with a valid schedule are auto-registered.
- A library with blank schedule is manual-run only.
- Dashboard/System show:
  - per-library next run
  - next global scheduled library/time

### Runtime and status

- Live runtime status is available via `GET /api/status` and auto-refreshes on Dashboard.
- Status includes queue depth, current library/trigger, run id, counters, ffmpeg progress hints, scheduler status, and preview snapshot metadata.
- Active runs can be cancelled via `POST /api/run/cancel`.

### Pipeline behavior

Core run pipeline:

1. candidate discovery (`*.mkv`, `.chonkignore`, `.chonkpause`, recency filters)
2. probe and skip policy evaluation (codec/resolution/threshold guards)
3. encode with retry/backoff
4. validate
5. atomic swap + markers (`.optimized`, `.failed`, `.bak`)
6. SQLite metrics write (`runs`, `encodes`, `activity_events`, service `settings`, `libraries`)

---

## UI Surface (Current)

Service UI routes:

- `/dashboard`
- `/analytics`
- `/runs`
- `/runs/{run_id}`
- `/history`
- `/activity`
- `/settings`
- `/system`

### Dashboard

- Per-library cards for enabled libraries
- Run Now + Preview Run actions
- Per-library next run + lifetime/recent savings summaries
- Live runtime status/progress (auto refresh)
- Stop Run action while active
- Preview Results panel (latest snapshot) + **Clear Preview Results**
- Preview Summary block (files evaluated/candidates + estimated totals and savings percent)
- Compact Dashboard Summary widget (total saved, files optimized, saved this week/month, next runs)

### Analytics

- Overall savings summary (total files optimized, total saved, average savings percent, saved this week/month)
- Savings over time tables (daily/weekly/monthly)
- Per-library savings breakdown (files optimized, total saved, average savings percent, recent savings)
- Top savings files and top savings runs
- Best Next Opportunities summary (best next library, reclaimable space, highest potential files, recent effectiveness)

### Runs

- Recent run table backed by `runs`
- Result/mode/duration/counts/savings (including total saved per run)
- Links into Run Detail

### Run Detail

- Sectioned summary: Run Summary, Outcome, Counts, Savings, Related Information
- Savings now include total saved and average saved per encoded file
- Includes trigger type when available from `activity_events`
- Includes file-level rows from `encodes`

### History

- Recent encode-centric view from `encodes`

### Activity

- Recent operator-facing service events from `activity_events`
- Run-linked events hyperlink to Run Detail

### Settings

- Global Settings form (DB-backed)
- Library CRUD (name/path/schedule/enabled + per-library processing/encoding fields)
- Per-library **Ignored Folders** management (create/remove `.chonkignore` files under the library root), including a Browse picker that lists only directories under the selected library root and auto-fills library-relative paths
- Send Test Notification action
- Inline help tooltips (`?`) beside each global and library setting label, with operator-focused descriptions aligned to the settings mapping table

### System

- Service/scheduler summary (app version, scheduler status/start time, next scheduled library job/time, queue depth)
- Housekeeping section (enabled state, schedule, next run, retention, and in-page controls)
- Current job status + runtime paths (DB/work roots)

---

## Preview Mode (What it does / does not do)

Preview mode (`trigger=preview`):

- **Does** run candidate scan + ffprobe + policy evaluation.
- **Does** estimate output size and estimated savings.
- **Does** return per-file decisions (`Encode`, threshold skips, codec/resolution skips).
- **Does not** run ffmpeg encode.
- **Does not** rename, replace, or delete media files.
- **Does not** write `.optimized`/`.failed` artifacts for media.

Preview snapshots are kept in service memory for Dashboard display until replaced or cleared.

## Dry Run Mode

Dry run mode (`dry_run=true`) scans candidate files and logs what would be encoded/swapped without running ffmpeg or modifying media files. It now evaluates each candidate up to `max_files` (instead of stopping after the first dry-run candidate).

---

## Notifications

Supported targets:

- Discord webhook
- Generic webhook

Current behavior:

- URLs are stored in SQLite settings and encrypted at rest.
- `CHONK_SECRET_KEY` is required to save/read encrypted webhook settings.
- Discord accepts `discord.com` and legacy `discordapp.com` URLs (normalized at send time).
- `Send Test Notification` is available from Settings.
- Run complete/failure sends are gated by individual enable toggles.
- Notification delivery failures are logged as warnings and do not fail the transcoding run.
- Proxy env vars are ignored by default for webhooks; set `CHONK_WEBHOOK_USE_PROXY=1` to opt in.

---

## Configuration Model (Where settings live)

### 1) Deployment / Environment (`compose.yaml` / env)

Use env for bootstrap/runtime concerns (service bind, paths, timezone, secret key, DB path).

### 2) Global Settings (SQLite `settings` table)

App-wide operational defaults, editable from **Settings** page.

### 3) Library Settings (SQLite `libraries` table)

Per-library scheduling + processing + encoding + skip-rule behavior.

> Bootstrap note: on first service startup, missing Global Settings and default Movies/TV library rows are initialized from env defaults. Existing libraries are automatically backfilled with new library fields (including skip rules) from legacy defaults when needed. After bootstrap/backfill, SQLite is source of truth for service operations.

---

## Settings Inventory / Mapping

Designed to be operator-friendly and reusable for future UI help/tooltips.

### Global Settings (SQLite: `settings`)

| Setting | Scope | Storage | Description | Default / Behavior |
|---|---|---|---|---|
| `min_file_age_minutes` | Global | SQLite `settings` | Skip very recent files newer than this age. | `10` minutes bootstrap default. |
| `min_savings_percent` | Global | SQLite `settings` | Minimum required savings percent before swap. | `15` bootstrap default. |
| `max_savings_percent` | Global | SQLite `settings` | Optional upper savings guard; above this can be skipped. Policy skips for this reason are cached per file in SQLite and reused until the threshold is raised above the cached savings value. | `0` means disabled. |
| `min_media_free_gb` | Global | SQLite `settings` | Minimum free space safety threshold for media volume. | `0` (disabled unless set). |
| `max_gb_per_run` | Global | SQLite `settings` | Optional cap on total GB processed per run. | `0` means no cap. |
| `fail_fast` | Global | SQLite `settings` | Stop early on failure conditions instead of continuing. | `0` (off). |
| `log_skips` | Global | SQLite `settings` | Emit skip reasons more verbosely in logs/stats. | `0` (off). |
| `top_candidates` | Global | SQLite `settings` | Candidate ranking/display/selection helper limit. | `5`. |
| `retry_count` | Global | SQLite `settings` | Number of retries after initial encode attempt. | `1` retry. |
| `retry_backoff_seconds` | Global | SQLite `settings` | Delay between retry attempts. | `5` seconds. |
| `validate_seconds` | Global | SQLite `settings` | Validation sample duration for post-encode checks. | `10`. |
| `log_retention_days` | Global | SQLite `settings` | Log cleanup retention window. | `30` days. |
| `bak_retention_days` | Global | SQLite `settings` | Backup file cleanup retention window. | `60` days. |
| `discord_webhook_url` | Global | SQLite `settings` (encrypted) | Discord notification endpoint. | Empty = not configured. |
| `generic_webhook_url` | Global | SQLite `settings` (encrypted) | Generic webhook endpoint. | Empty = not configured. |
| `enable_run_complete_notifications` | Global | SQLite `settings` | Enable run-complete notifications. | `0` (off). |
| `enable_run_failure_notifications` | Global | SQLite `settings` | Enable run-failure notifications. | `0` (off). |
| `housekeeping_enabled` | Global | SQLite `settings` | Enable/disable scheduled housekeeping runs. | `1` (enabled). |
| `housekeeping_schedule` | Global | SQLite `settings` | Cron schedule for housekeeping scheduler registration. | `0 2 * * *` (daily 02:00). |

### Library Settings (SQLite: `libraries`)

| Setting | Scope | Storage | Description | Default / Behavior |
|---|---|---|---|---|
| `name` | Per-library | SQLite `libraries` | Operator label for library. | Required, unique. |
| `path` | Per-library | SQLite `libraries` | Media root path for scanning. | Required, unique. |
| `enabled` | Per-library | SQLite `libraries` | Includes library in runtime controls/scheduling. | Enabled by default. |
| `schedule` | Per-library | SQLite `libraries` | Cron expression for scheduler registration. | Blank = manual only. |
| `min_size_gb` | Per-library | SQLite `libraries` | Skip files below this library-specific size floor. | `0.0`. |
| `max_files` | Per-library | SQLite `libraries` | Max files processed in a run for this library. | `1`. |
| `priority` | Per-library | SQLite `libraries` | Queue priority (higher runs first). | `100`. |
| `qsv_quality` | Per-library | SQLite `libraries` | QSV quality for this library. | Bootstrapped from env (`QSV_QUALITY`, fallback `21`). |
| `qsv_preset` | Per-library | SQLite `libraries` | QSV preset for this library. | Bootstrapped from env (`QSV_PRESET`, fallback `7`). |
| `min_savings_percent` | Per-library | SQLite `libraries` | Library-specific minimum savings threshold. | Bootstrapped from env (`MIN_SAVINGS_PERCENT`, fallback `15`). |
| `max_savings_percent` | Per-library | SQLite `libraries` | Optional library-specific maximum savings threshold override. If unset, inherits global `max_savings_percent`. | Nullable; blank/unset means inherit global value. |
| `skip_codecs` | Per-library | SQLite `libraries` | Comma-separated codecs to skip (normalized). | Bootstrapped from legacy `SKIP_CODECS` default when missing. |
| `skip_min_height` | Per-library | SQLite `libraries` | Skip files at or above this vertical resolution. | Bootstrapped from legacy `SKIP_MIN_HEIGHT` default when missing. |
| `skip_resolution_tags` | Per-library | SQLite `libraries` | Comma-separated filename tags to skip (normalized). | Bootstrapped from legacy `SKIP_RESOLUTION_TAGS` default when missing. |
| `ignored folders` | Per-library filesystem | `.chonkignore` marker files | Managed in Settings as library-relative paths; Browse picker is root-restricted to the library path and auto-fills relative folder selections; manual filesystem markers are auto-discovered in UI. | No DB persistence; scanner ignore behavior remains unchanged. |

### Deployment / Environment Settings

| Setting | Scope | Storage | Description |
|---|---|---|---|
| `SERVICE_MODE` | Runtime | env/compose | Enable long-running service when true. |
| `SERVICE_HOST` | Runtime | env/compose | Bind host for HTTP service. |
| `SERVICE_PORT` | Runtime | env/compose | Bind port for HTTP service. |
| `STATS_PATH` | Runtime | env/compose | SQLite DB path (`runs`, `encodes`, service settings/libraries/activity). |
| `WORK_ROOT` | Runtime | env/compose | Writable work/log/report location. |
| `TZ` | Runtime | env/compose | Timezone used for scheduler/display. |
| `CHONK_SECRET_KEY` | Runtime secret | env/compose | Required for encrypted webhook settings. |
| `APP_VERSION` | Runtime metadata | env/compose | Optional runtime version override. |
| `MOVIE_MEDIA_ROOT`, `TV_MEDIA_ROOT` | Bootstrap | env/compose | Used to seed default libraries on first startup. |
| `MOVIE_SCHEDULE`, `TV_SCHEDULE` | Bootstrap | env/compose | Legacy schedule seed values for first startup only. |
| `QSV_QUALITY`, `QSV_PRESET`, `MIN_SAVINGS_PERCENT` | Bootstrap | env/compose | Seed defaults for new/bootstrap library encoding fields. |
| `SKIP_CODECS`, `SKIP_MIN_HEIGHT`, `SKIP_RESOLUTION_TAGS` | Bootstrap compatibility | env/compose | Optional one-time/default seed inputs for library skip fields when DB values are absent. |

---

## Retry and Failure Behavior

- Retries run per file according to `retry_count` and `retry_backoff_seconds`.
- Final hard failures can mark media with `.failed` marker to avoid repeated failures.
- Successful swaps mark `.optimized`.
- Cancelled runs are recorded and surfaced as cancelled status in run summaries.

---

## Daily Housekeeping

- Housekeeping is DB-backed (`housekeeping_enabled`, `housekeeping_schedule`) and configurable from the System/Settings UI with weekday checkboxes + time input.
- Existing installs bootstrap defaults automatically (`enabled`, daily at `02:00`) so upgrade behavior matches prior cleanup behavior without manual setup.
- Housekeeping performs log cleanup under `/work/logs` using `LOG_RETENTION_DAYS`, even if no library run starts that day.
- It does not run encode work, does not modify media files, and skips itself when queue/run activity is in progress.
- Updating housekeeping settings from the UI refreshes the live scheduler job without service restart.
- Activity entries include `housekeeping_started` and `housekeeping_completed`.

---

## Data Storage

Default DB path:

```text
/config/chonk.db
```

SQLite tables used by current service/app behavior:

- `runs`
- `encodes`
- `activity_events`
- `settings`
- `libraries`

---

## Testing

```bash
pip install -r requirements-dev.txt
pytest -q
```

See `docs/TESTING.md` for environment-specific test notes.
