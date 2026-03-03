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

Chonk Reducer is a **policy-driven media size reduction system** built for NAS environments (Synology + Docker).

**Goal:** safely reclaim space by re-encoding oversized media files (Intel QSV → HEVC), validating output, and swapping in-place with backups and markers so scheduled runs are safe and repeatable.

---

## What Chonk Does

- Scans a media root for candidates (default: `*.mkv`)
- Skips folders with `.chonkignore`
- Can pause instantly with `.chonkpause` (+ optional `.chonkpause.reason`)
- Encodes **in the same folder** as the source (avoids cross-device rename errors / EXDEV)
- Validates output (decode test)
- Enforces **minimum savings %** before swapping
- Backs up original with timestamped `.bak.YYYYMMDD_HHMMSS`
- Atomic replace swap + writes `.optimized` marker
- Retries transient failures with backoff
- Quarantines repeated failures with `.failed` marker
- Logs metrics and per-show rollups

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

Manual run:

```bash
docker compose run --rm tv-transcoder
docker compose run --rm movie-transcoder
```

This project is designed to be run via Synology DSM Task Scheduler using the wrapper script:
- `scripts/chonkreducer_task.sh`

---

## Scheduling Model (Recommended)

- TV: 3 days/week
- Movies: 3 days/week
- Alternate days so they never overlap
- Keep `MAX_FILES=1` to bound runtime per scheduled job

---

## Environment Variables

Yes—keep a table. It makes upgrades and troubleshooting way easier.

> Tip: Use YAML anchors in `compose.yaml` so movie + tv share the same ordered config block.

### Roots / Identity

| Variable | Purpose | Example |
|---|---|---|
| `MEDIA_ROOT` | Root directory to scan | `/tv_shows` |
| `WORK_ROOT` | Workspace root (logs, caches, locks) | `/work` |
| `LOG_PREFIX` | Prefix for log file names | `tv` / `movie` |
| `TZ` | Timezone in logs | `America/Chicago` |

### Run Modes

| Variable | Purpose | Default |
|---|---|---|
| `DRY_RUN` | No encode/swap; log what would happen | `false` |
| `PREVIEW` | Probe-only preview; no encode/swap | `false` |
| `FAIL_FAST` | Exit on first failure | `true` |

### Selection / Discovery

| Variable | Purpose | Default |
|---|---|---|
| `MIN_SIZE_GB` | Minimum file size to consider | (service-specific) |
| `MAX_FILES` | Stop after this many “done” actions | `1` |
| `TOP_CANDIDATES` | Log top N candidates by size | `5` |
| `EXCLUDE_PATH_PARTS` | Comma list of path parts to exclude | `#recycle,@eaDir` |
| `LOG_SKIPS` | Log per-file skip reasons | `false` |

### Encoding

| Variable | Purpose | Default |
|---|---|---|
| `QSV_QUALITY` | QSV quality (lower = higher quality) | `21` |
| `QSV_PRESET` | QSV preset | `7` |
| `EXTRA_HW_FRAMES` | QSV hw frames | `64` |
| `MIN_SAVINGS_PERCENT` | Skip swap if savings below threshold | `15` |

### Validation

| Variable | Purpose | Default |
|---|---|---|
| `POST_ENCODE_VALIDATE` | Enable post-encode validation | `1` |
| `VALIDATE_MODE` | Validation mode | `decode` |
| `VALIDATE_SECONDS` | Seconds to decode-test | `10` |

### Retries / Failure Handling

| Variable | Purpose | Default |
|---|---|---|
| `RETRY_COUNT` | Extra retries after first attempt | `1` |
| `RETRY_BACKOFF_SECS` | Backoff base seconds (multiplied by attempt #) | `5` |

### Ownership / Permissions

| Variable | Purpose | Default |
|---|---|---|
| `OUT_UID` | Output file owner UID | `1028` |
| `OUT_GID` | Output file group GID | `100` |
| `OUT_MODE` | Output file mode | `664` |
| `OUT_DIR_MODE` | Output directory mode | `775` |

### Cleanup / Retention / Locks

| Variable | Purpose | Default |
|---|---|---|
| `WORK_CLEANUP_HOURS` | Cleanup stale work artifacts | `0` |
| `LOG_RETENTION_DAYS` | Log retention | `30` |
| `BAK_RETENTION_DAYS` | Backup retention | `60` |
| `LOCK_STALE_HOURS` | Consider lock stale after N hours | `12` |

### Probe / Timeouts

| Variable | Purpose | Default |
|---|---|---|
| `PROBE_TIMEOUT_SECS` | Timeout for ffprobe | `60` |
| `FFPROBE_ANALYZEDURATION` | ffprobe analyzeduration | `50000000` |
| `FFPROBE_PROBESIZE` | ffprobe probesize | `50000000` |

---

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
