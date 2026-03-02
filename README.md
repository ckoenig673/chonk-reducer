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

---

## Overview

**Chonk Reducer** is a containerized, scheduled media transcoding system designed for NAS environments.

It safely converts large media files to HEVC using Intel Quick Sync (QSV), validates the output, swaps originals with timestamped backups, and enforces retention policies — all without running persistent services.

This project evolved from a simple shell-based ffmpeg loop into a modular Python-based pipeline with clean separation of concerns and deterministic behavior.

---

## Core Capabilities

- Directory scanning with exclusion support
- Size-based filtering (`MIN_SIZE_GB`)
- Intel QSV hardware-accelerated HEVC encoding
- Optional validation pass (`probe` or `decode` mode)
- Atomic swap with timestamped `.bak.YYYYMMDD_HHMMSS`
- Backup retention enforcement
- Work directory cleanup for stale encodes
- `.optimized` marker to prevent reprocessing
- Short-lived container execution (`docker compose run --rm`)
- Designed for DSM Task Scheduler integration

Idempotent by design. Safe for repeated scheduled runs.

---

## High-Level Architecture

```
DSM Task Scheduler
    → scripts/chonkreducer_task.sh
        → docker compose run --rm
            → python -m chonk_reducer
                → runner
                    → discovery
                    → encode
                    → validation
                    → swap
                    → cleanup
```

No long-running daemons. Each scheduled execution is isolated and self-contained.

---

## Project Structure

```
scripts/
    chonkreducer_task.sh
    auto_transcode.monolith.py   (archived original implementation)

src/chonk_reducer/
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

## Build

```bash
docker compose build --no-cache movie-transcoder
```

---

## Manual Run

```bash
docker compose run --rm movie-transcoder
docker compose run --rm tv-transcoder
```

---

## DSM Task Example

```bash
/bin/sh /volume1/docker/projects/nas-transcoder/scripts/chonkreducer_task.sh movie-transcoder
```

Repeat for `tv-transcoder`.

---

## Key Environment Variables

| Variable | Purpose |
|----------|----------|
| MEDIA_ROOT | Root directory to scan |
| WORK_ROOT | Temporary workspace + logs |
| MIN_SIZE_GB | Minimum file size to process |
| MAX_FILES | Maximum files per run |
| QSV_QUALITY | HEVC quality level |
| POST_ENCODE_VALIDATE | Enable validation step |
| VALIDATE_MODE | `probe` or `decode` |
| VALIDATE_SECONDS | Seconds to validate |
| BAK_RETENTION_DAYS | Backup retention window |
| WORK_CLEANUP_HOURS | Stale work artifact cleanup |
| LOG_PREFIX | Distinguish movie vs tv logs |

---

## Design Decisions

- Backups are explicitly “touched” on creation so retention logic reflects backup time, not original file timestamp.
- Transcodes are written to `/work` and only swapped in after successful validation.
- `.optimized` marker prevents re-encoding.
- Cleanup logs explicitly list deleted files for traceability.
- Execution uses `docker compose run --rm` to avoid container buildup.

---

## Evolution

This project began as a simple script experiment and evolved into:

- A modular Python pipeline
- A containerized job execution model
- A Git-managed refactor workflow
- A reproducible NAS automation system

It demonstrates iterative refinement, architectural discipline, and AI-assisted development without forced deadlines.

---

## Status

Stable for scheduled NAS usage.
Modularized and version-controlled.
Actively evolving.

---

Reduce the chonk responsibly.
