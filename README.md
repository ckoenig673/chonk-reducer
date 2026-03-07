
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

**Current Version:** v1.11.0

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

# Long-Running Service Mode (Transition Foundation)

Chonk Reducer now supports an optional long-running service mode for internal scheduling.

- Existing DSM Task Scheduler + one-shot container runs are still supported.
- Service mode now includes a very small operator page for manual troubleshooting runs.
- The home page provides manual Run Movies/Run TV buttons, lightweight last-run status summaries for Movies and TV, and a Recent Runs table from SQLite when available.
- This is an early operator surface, not a full dashboard.

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

Service operator page:

```bash
open http://localhost:8080/
```

The home page provides:

- **Run Movies** (`POST /run/movies`)
- **Run TV** (`POST /run/tv`)
- Basic last-run status for Movies and TV
- A small Recent Runs view (latest run rows from `runs` in SQLite)

Manual trigger behavior:

- Requests reuse the same service orchestration path used by scheduled runs.
- Library locks prevent overlap (if a library run is active, manual requests return `{"status":"busy"}`).
- Intended for troubleshooting and operational validation, not as a final dashboard.

Service scheduler environment variables:

- `SERVICE_MODE=true|false` (default: false)
- `SERVICE_HOST` (default: `0.0.0.0`)
- `SERVICE_PORT` (default: `8080`)
- `MOVIE_SCHEDULE` (cron expression; blank disables movie schedule)
- `TV_SCHEDULE` (cron expression; blank disables TV schedule)

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
