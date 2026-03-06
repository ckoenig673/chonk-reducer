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

Current Version: v1.6.2

Chonk Reducer is a **policy-driven NAS media optimization pipeline** built for Synology + Docker environments.

**Goal:** safely reclaim space by scanning media libraries, identifying oversized files, re-encoding with Intel QSV (HEVC), validating output, swapping in-place, and recording run stats.

---

## Roadmap

Future improvements and planned features:  
[View the project roadmap](https://github.com/ckoenig673/chonk-reducer/issues?q=is%3Aissue+is%3Aopen+label%3Aroadmap)

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
```

Manual run:

```bash
docker compose run --rm tv-transcoder
docker compose run --rm movie-transcoder
```

**Healthcheck strict mode**

- `HEALTHCHECK_STRICT=true` (default): exits non-zero if any check fails
- `HEALTHCHECK_STRICT=false`: always exits `0` but still prints `[FAIL]` lines

This project is designed to be run via Synology DSM Task Scheduler using:

- `scripts/chonkreducer_task.sh`

Example DSM task using wrapper:

```bash
/bin/sh scripts/chonkreducer_task.sh tv-transcoder
/bin/sh scripts/chonkreducer_task.sh movie-transcoder
```

---

## Scheduling Model (Recommended)

- TV: 3 days/week
- Movies: 3 days/week
- Alternate days so they never overlap
- Keep `MAX_FILES=1` to bound runtime per scheduled job

---

## Testing

The project uses **pytest** for unit testing.

- Test files live in the `/tests` directory.
- Tests are designed to run quickly and without requiring FFmpeg, real NAS storage, or real media files.

Install development tooling first:

```bash
pip install -r requirements-dev.txt
```

Run tests locally from the repository root:

```bash
pytest -q
```

---

## Development Workflow

A simple local workflow:

1. Create a branch for your change.
2. Make your changes.
3. Run tests locally before committing.
4. Commit only after `pytest` passes.

Example:

```bash
git checkout -b my-change
pytest -q
git add .
git commit -m "Describe your change"
```

---


## Legacy NDJSON Migration

Chonk Reducer uses SQLite as the primary stats backend, but can still import legacy NDJSON stats files.

- Migration runs during stats initialization on **every startup**.
- Migration is evaluated per library path (`MEDIA_ROOT`), not by whether the shared DB already exists.
- If `MEDIA_ROOT/.chonkstats.ndjson` exists, rows are streamed line-by-line into SQLite.
- After a successful import, the file is renamed to `.chonkstats.ndjson.migrated`.
- If `.chonkstats.ndjson.migrated` already exists, migration is skipped for that library.

This allows multiple libraries that share `/config/chonk.db` to migrate independently without losing historical data.

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
| `SKIP_RESOLUTION_TAGS` | `2160p,4k,uhd` | Comma-separated tags; if in path/name then skip as 4K/UHD, etc. |
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
| `LOG_RETENTION_DAYS` | `30` | Delete old transcode logs under `/work/logs` older than this many days. |
| `BAK_RETENTION_DAYS` | `60` | Delete old `*.bak.*` files under MEDIA_ROOT older than this many days. |
| `STATS_ENABLED` | `True` | Write per-file metrics into SQLite (`runs` + `encodes`) at STATS_PATH. |
| `STATS_PATH` | `/config/chonk.db` | SQLite database location for metrics and weekly reporting. |
| `WEEKLY_STATS_PATHS` | `/config/chonk.db` | Comma-separated SQLite database paths for `weekly-report`. |
| `WEEKLY_REPORT_DAYS` | `7` | Lookback window (days) for the weekly report command. |
| `REPORT_RETENTION_DAYS` | `0` | Delete weekly report files older than this many days (0 disables). |
| `LOCK_STALE_HOURS` | `12` | Consider a lock stale after this many hours. |
| `LOCK_SELF_HEAL` | `True` | Auto-remove stale locks; if false, skip run when lock is stale. |
| `MIN_MEDIA_FREE_GB` | `0.0` | Abort run if MEDIA_ROOT volume has less free space than this (0 disables). |

### Notes

- Values are read from env and parsed as bool/int/float where applicable.
- `STATS_PATH` points to a shared SQLite DB (default `/config/chonk.db`) that can be used by multiple libraries (for example, movies and TV).
- On every startup, each library checks its own `MEDIA_ROOT/.chonkstats.ndjson`. If present, records are imported into SQLite and the file is renamed to `.chonkstats.ndjson.migrated`.
- Migration is idempotent: already-migrated files are skipped and duplicate legacy records are not inserted again.
- `REPORT_RETENTION_DAYS` is applied by the `weekly-report` command to prune old `weekly_*.md` files.

---

## Outputs / Artifacts

- Encoded temp file (same folder): `Episode.mkv.<stamp>.encoded.mkv`
- Backup (same folder): `Episode.mkv.bak.<stamp>`
- Marker (same folder): `Episode.mkv.optimized`
- Failure marker (same folder): `Episode.mkv.failed`
- Logs (work root): `/work/logs/<prefix>_transcode_<stamp>.log`
- Weekly reports: `/work/reports/chonk_weekly_<date>.txt`

---

## Project Layout

```text
src/chonk_reducer/   # application package
tests/               # pytest unit tests
work/                # runtime workspace in container (temp files, reports)
work/logs/           # runtime logs
```

Additional scripts:

```text
scripts/chonkreducer_task.sh
```

---

## Notes / Troubleshooting

### “ffprobe: command not found” on NAS host

`ffprobe` is inside the container. Use:

```bash
docker compose run --rm --entrypoint ffprobe tv-transcoder <args...>
```
