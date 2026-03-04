![CI](https://github.com/ckoenig673/chonk-reducer/actions/workflows/ci.yml/badge.svg)
![License](https://img.shields.io/github/license/ckoenig673/chonk-reducer)
![Release](https://img.shields.io/github/v/release/ckoenig673/chonk-reducer)
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
Current Version: v1.6.0

Chonk Reducer is a **policy-driven media size reduction system** built for NAS environments (Synology + Docker).

**Goal:** safely reclaim space by re-encoding oversized media files (Intel QSV → HEVC), validating output, and swapping in-place with backups and markers so scheduled runs are safe and repeatable.

---

## What Chonk Does

- Scans a media root for candidates (default: `*.mkv`)
- Skips folders with `.chonkignore`
- Can pause instantly with `.chonkpause` (+ optional `.chonkpause.reason`)
- Encodes **in the same folder** as the source (avoids cross-device rename errors / EXDEV)
- Validates output (decode test)
- Enforces a **savings window** (min + optional max) before swapping
- Backs up original with timestamped `.bak.YYYYMMDD_HHMMSS`
- Atomic replace swap + writes `.optimized` marker
- Retries transient failures with backoff
- Quarantines repeated failures with `.failed` marker
- Logs metrics and per-show rollups

### Run Summary Counters

At the end of each run, the runner prints stage-oriented counters:

- `Candidates found`
- `Pre-filtered` (marker/backup)
- `Evaluated`
- `Processed (encode)`
- `Succeeded`
- `Skipped (policy)`
- `Failed`


---

## High-Level Architecture

```
DSM Task Scheduler
    → scripts/chonkreducer_task.sh
        → docker compose run --rm --no-deps <service>
            → python -m chonk_reducer
                → runner
                    → cleanup
                    → discovery
                    → probe (ffprobe)
                    → encode (ffmpeg QSV)
                    → validate
                    → swap + backup + marker
                    → metrics + summary
```

No long-running daemons. Each scheduled execution is isolated and self-contained.

---

## Folder Markers

### `.chonkignore`
Place in a folder to exclude it and all subfolders from processing:

```
/tv_shows/Sports/.chonkignore
```

### `.chonkpause`
Place at the media root to exit immediately without processing:

```
/tv_shows/.chonkpause
```

Optional pause reason:

```
/tv_shows/.chonkpause.reason
```

### `.optimized`
Written after a successful swap. Prevents re-processing.

### `.failed`
Created when a file fails after all retries. Skipped on future runs.

---

## Docker Usage
Healthcheck run:

```bash
docker compose run --rm tv-transcoder healthcheck
docker compose run --rm movie-transcoder healthcheck

Manual run:

```bash
docker compose run --rm tv-transcoder
docker compose run --rm movie-transcoder
```

**Healthcheck strict mode**

- `HEALTHCHECK_STRICT=true` (default): exits non-zero if any check fails
- `HEALTHCHECK_STRICT=false`: always exits 0 but still prints `[FAIL]` lines (useful for smoke tests)


This project is designed to be run via Synology DSM Task Scheduler using the wrapper script:
- `scripts/chonkreducer_task.sh`

---

## Scheduling Model (Recommended)

- TV: 3 days/week
- Movies: 3 days/week
- Alternate days so they never overlap
- Keep `MAX_FILES=1` to bound runtime per scheduled job

## Add Wrapper Commands Section (Nice to Have)

```markdown
Example DSM task using wrapper:

```bash
/bin/sh scripts/chonkreducer_task.sh tv-transcoder
/bin/sh scripts/chonkreducer_task.sh movie-transcoder

---

## Environment Variables

These are passed to the **container** via `compose.yaml` (`environment:`). Movie and TV services should use the same keys.

**Run modes:** `DRY_RUN=true` (no swaps/changes) and `PREVIEW=true` (lightweight preview) are supported; otherwise the run is **LIVE**.

| Variable | Default | What it does |
|---|---:|---|
| `LIBRARY_NAME` | `` | Logical library name written into stats/weekly reports (e.g., movies, tv). |
| `MEDIA_ROOT` | `/movies` | Root folder to scan for media files (container path). |
| `WORK_ROOT` | `/work` | Work directory for temp files, logs, and reports (container path). |
| `EXCLUDE_PATH_PARTS` | `#recycle,@eaDir` | Comma-separated path fragments to ignore during scanning (Synology recycle bin, @eaDir, etc). |
| `DRY_RUN` | `False` | If true, prints what would happen but does not encode/swap. |
| `PREVIEW` | `False` | If true, behaves like a lightweight preview run (no swaps). |
| `MAX_FILES` | `2` | Max number of files to attempt per run. |
| `MIN_SIZE_GB` | `18.0` | Only consider files >= this size (GB). |
| `MAX_GB_PER_RUN` | `0.0` | Optional cap on total input size processed per run (0 disables). |
| `FAIL_FAST` | `False` | Stop the run on the first hard failure (vs continue). |
| `MIN_SAVINGS_PERCENT` | `15.0` | Minimum savings required to keep an encode result; otherwise treated as skip. |
| `MAX_SAVINGS_PERCENT` | `0.0` | Optional maximum savings; if savings exceed this, treat as skip (0 disables). |
| `SKIP_CODECS` | `hevc,av1` | Comma-separated codecs to skip (already encoded). |
| `SKIP_MIN_HEIGHT` | `2160` | Skip if source height >= this (e.g., skip 4K). |
| `SKIP_RESOLUTION_TAGS` | `2160p,4k,uhd` | Comma-separated tags; if in path/name then skip as 4K/uhd, etc. |
| `LOG_SKIPS` | `False` | Log skip decisions per-file. |
| `ENCODER` | `hevc_qsv` | Encoder profile to use (default QSV HEVC). |
| `QSV_QUALITY` | `21` | QSV quality (ICQ). Lower = higher quality. |
| `QSV_PRESET` | `7` | QSV preset (higher is faster; depends on driver). |
| `EXTRA_HW_FRAMES` | `64` | Extra hardware frames for QSV (helps throughput on some systems). |
| `FFMPEG_PATH` | `ffmpeg` | Path to ffmpeg inside the container. |
| `FFPROBE_PATH` | `ffprobe` | Path to ffprobe inside the container. |
| `PROBE_TIMEOUT_SECS` | `60` | Timeout for ffprobe on a single file. |
| `TOP_CANDIDATES` | `5` | How many largest candidates to print before processing. |
| `POST_ENCODE_VALIDATE` | `True` | Enable post-encode validation. |
| `VALIDATE_MODE` | `decode` | Validation mode (e.g., decode). |
| `VALIDATE_SECONDS` | `10` | How many seconds of output to validate. |
| `RETRY_COUNT` | `1` | Encode retry attempts on transient failure. |
| `RETRY_BACKOFF_SECS` | `5` | Seconds to wait between retries. |
| `OUT_UID` | `1028` | UID to chown output files to (0 leaves as-is if running as root). |
| `OUT_GID` | `100` | GID to chown output files to (0 leaves as-is if running as root). |
| `OUT_MODE` | `"664"` | Octal file mode for output files. |
| `OUT_DIR_MODE` | `"775"` | Octal dir mode for created directories. |
| `WORK_CLEANUP_HOURS` | `0` | Delete temp files under WORK_ROOT older than this many hours. |
| `LOG_RETENTION_DAYS` | `30` | Delete old transcode logs under /work/logs older than this many days. |
| `BAK_RETENTION_DAYS` | `60` | Delete old *.bak.* files under MEDIA_ROOT older than this many days. |
| `STATS_ENABLED` | `True` | Write per-file NDJSON stats to STATS_PATH. |
| `STATS_PATH` | `/movies/.chonkstats.ndjson` | Where NDJSON stats are written (container path). |
| `WEEKLY_REPORT_DAYS` | `7` | Lookback window (days) for the weekly report command. |
| `REPORT_RETENTION_DAYS` | `0` | Delete weekly report files older than this many days (0 disables). |
| `LOCK_STALE_HOURS` | `12` | Consider a lock stale after this many hours. |
| `LOCK_SELF_HEAL` | `True` | If true, automatically remove stale locks; if false, skip the run when lock is stale. |
| `MIN_MEDIA_FREE_GB` | `0.0` | Abort run if MEDIA_ROOT volume has less free space than this (0 disables). |

### Notes
- Values are read from env and parsed as bool/int/float where applicable.
- `STATS_PATH` should live on the media volume if you want one stats file per library.
- `REPORT_RETENTION_DAYS` is applied by the `weekly-report` command to prune old `weekly_*.md` files.

## Outputs / Artifacts

- Encoded temp file (same folder):  
  `Episode.mkv.<stamp>.encoded.mkv`
- Backup (same folder):  
  `Episode.mkv.bak.<stamp>`
- Marker (same folder):  
  `Episode.mkv.optimized`
- Failure marker (same folder):  
  `Episode.mkv.failed`
- Logs (work root):  
  `/work/logs/<prefix>_transcode_<stamp>.log`
- Weekly reports:
  `/work/reports/chonk_weekly_<date>.txt`
---

## Project Structure

```
scripts/
  chonkreducer_task.sh

src/chonk_reducer/
  __main__.py
  cli.py
  runner.py
  discovery.py
  encode.py
  validation.py
  swap.py
  cleanup.py
  config.py
  ffmpeg_utils.py
  lock.py
  logging_utils.py
```

---

## Notes / Troubleshooting

### “ffprobe: command not found” on NAS host
ffprobe is inside the container. Use:

```bash
docker compose run --rm --entrypoint ffprobe tv-transcoder <args...>
```

### EXDEV / cross-device rename errors
Chonk encodes in-place specifically to avoid this Synology bind-mount pain.

---

## Status

Stable for scheduled NAS usage.  
Actively evolving.

Author: Cory Koenig

---


---

## 🔔 Discord Notifications (Optional)

Chonk Reducer can send a summary notification after each run using a Discord webhook.

### 1️⃣ Create a Webhook
- Go to **Discord → Server Settings → Integrations → Webhooks**
- Create a webhook
- Copy the webhook URL

### 2️⃣ (Optional) Get Your User ID (for mentions)
- Enable **Developer Mode** in Discord
- Right-click your username → **Copy User ID**

---

## 🔧 Configuration Options

You can configure Discord in **two ways**.

### ✅ Option A — Recommended (DSM Task Scheduler)

This keeps secrets out of git and keeps the wrapper script generic.

In DSM:

**Control Panel → Task Scheduler → Edit Task → User-defined script**

Add environment variables BEFORE calling the wrapper:

```bash
export DISCORD_WEBHOOK_URL="https://discord.com/api/webhooks/XXXXXXXX/XXXXXXXX"
export DISCORD_USER_ID="123456789012345678"   # optional

export DISCORD_ONLY_IF_WORK_DONE=true
export DISCORD_PING_ON_FAILURE=true
export DISCORD_PING_ON_SUCCESS=false
export DISCORD_DEBUG=false

# Escalated failure handling (wrapper-level)
export ESCALATE_FAILURES=true
export ESCALATE_FAILURE_THRESHOLD=3

/volume1/docker/projects/nas-transcoder/scripts/chonkreducer_task.sh tv-transcoder
```

**Recommended for production use.**

---

### Option B — Directly Inside `scripts/chonkreducer_task.sh`

For simple setups (or non-DSM environments), you can define:

```bash
DISCORD_WEBHOOK_URL="https://discord.com/api/webhooks/XXXXXXXX/XXXXXXXX"
DISCORD_USER_ID="123456789012345678"
```

⚠ Not recommended if the repository is public.

---

## 📬 What Gets Sent

Notifications include:

- Service name (tv or movie)
- Exit code
- Run ID
- Processed / Failed counts
- Savings summary
- Log file location

---

## ⚙ Environment Variables (compose.yaml)

Keep TV and Movie containers in the **same order** for consistency.

| Setting | Description |
|----------|-------------|
| MEDIA_ROOT | Root folder to scan (`/tv_shows` or `/movies`) |
| WORK_ROOT | Temporary work/log directory |
| MIN_SIZE_GB | Minimum file size threshold |
| MAX_FILES | Max successful swaps per run |
| MIN_SAVINGS_PERCENT | Minimum % reduction required to keep encode |
| QSV_QUALITY | Intel QSV quality target |
| QSV_PRESET | QSV speed/quality preset |
| POST_ENCODE_VALIDATE | Enable validation step |
| VALIDATE_MODE | Validation type (`decode`) |
| VALIDATE_SECONDS | Seconds to validate |
| OUT_UID | Output file owner UID |
| OUT_GID | Output file group GID |
| OUT_MODE | File permissions |
| OUT_DIR_MODE | Directory permissions |
| EXCLUDE_PATH_PARTS | Skip folders like `#recycle`, `@eaDir` |
| FAIL_FAST | Stop immediately on error |
| DRY_RUN | Simulate encode + swap |
| LOG_SKIPS | Log skipped candidates |
| TOP_CANDIDATES | Show largest candidates preview |
| DISCORD_* | Configured in wrapper script (not compose) |

---

---

## 🚀 v1.1.0 – Stats + Run Totals Release

### What’s New
- NDJSON stats export per library (`.chonkstats.ndjson`)
- Per-run totals in logs (before/after/saved/%/time/rate)
- Discord summary now includes run totals
- Enforced LF line endings via `.gitattributes` for cross-platform stability

---

## 📊 Stats Output (NDJSON)

Each processed file appends a single JSON line to:

Version stamping uses `APP_VERSION` if set; otherwise it falls back to the package `__version__` (or `unknown`).

- `/tv_shows/.chonkstats.ndjson`
- `/movies/.chonkstats.ndjson`

### Example Fields

- `ts`
- `run_id`
- `version`
- `library`
- `mode`
- `encoder`
- `quality`
- `preset`
- `status`
- `stage`
- `skip_reason` (when `status=skipped`)
- `fail_stage` (when `status=failed`)
- `path`
- `filename`
- `size_before_bytes`
- `size_after_bytes`
- `saved_bytes`
- `saved_pct`
- `codec_from`
- `codec_to`
- `duration_seconds`
- `bak_path`

### Status Semantics

- `status` is one of: `success`, `skipped`, `failed`
- If `status=skipped`, `skip_reason` is recorded (e.g., `codec`, `resolution`, `min_savings`, `dry_run`)
- If `status=failed`, `fail_stage` is recorded (e.g., `probe`, `encode`, `validate`, `swap`)
- Older NDJSON rows may not include `skip_reason`/`fail_stage`; reports treat these as `unknown`.

This file is append-only and intended as the source of truth for reporting and aggregation (e.g., weekly reports).

---

## 📣 Discord Summary (Run Totals)

When enabled via the wrapper script, Discord notifications now include:

- `TOTAL BEFORE (run)`
- `TOTAL AFTER (run)`
- `TOTAL SAVED (run)`
- `TOTAL SAVED PCT (run)`
- `TOTAL TIME`
- `TOTAL RATE`

This provides full visibility into space savings and performance per run.

---



---

## 🆕 v1.2.0 – Reporting, Healthcheck & Skip Logic

This release introduces:

### ✅ Healthcheck Mode
Run without processing files:

```bash
docker compose run --rm tv-transcoder healthcheck
docker compose run --rm movie-transcoder healthcheck
```

Verifies:
- Config loads
- MEDIA_ROOT readable
- WORK_ROOT writable
- Logs & reports directories exist
- ffmpeg / ffprobe available
- QSV device present
- Stats file writable

---

### 📊 Weekly Report Generator

Generate 7-day rollup from NDJSON stats files:

```bash
docker compose run --rm tv-transcoder weekly-report
docker compose run --rm movie-transcoder weekly-report
```

Aggregates from:
- `/tv_shows/.chonkstats.ndjson`
- `/movies/.chonkstats.ndjson`

Outputs:
- Human-readable report file under `/work/reports/`
- Console summary (visible in DSM logs)
- Optional Discord summary (if enabled in wrapper)

---

### 🔔 Discord Enhancements

Wrapper now supports:
- Healthcheck notifications
- Weekly report notifications
- Run totals in summary

Controlled via wrapper toggles:

```bash
DISCORD_NOTIFY_HEALTHCHECK="true"
DISCORD_NOTIFY_WEEKLY="true"
```

Webhook + User ID remain configured in the DSM Task Scheduler (not in compose.yaml).

---

### 📈 Stats Export (NDJSON)

Each successful encode appends structured entry:

```
/tv_shows/.chonkstats.ndjson
/movies/.chonkstats.ndjson
```

Includes:
- run_id
- library
- encoder
- before/after bytes
- saved bytes
- saved %
- duration
- codec transition
- status + stage

Designed for:
- Future dashboarding
- Database import
- Long-term reporting

## Weekly Report Totals

Notes on totals:
- `success` rows contribute to savings totals.
- `skipped` rows are counted separately and **do not** count as failures.
- Rows with missing/unknown `status` are counted as `unknown`.

Wrapper detects DRY_RUN by reading latest log in /volume1/data/transcodework/logs and prints a banner.

## DRY_RUN visibility

The wrapper determines whether a run is DRY_RUN by reading the resolved `DRY_RUN` value from `docker compose config` (so it matches the container environment). It will print a clear banner when DRY_RUN is enabled.
