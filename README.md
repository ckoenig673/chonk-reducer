# Chonk Reducer

Chonk Reducer is a policy-driven media size reduction system designed
for NAS environments (especially Synology + Docker).

Its mission is simple:

> Safely reduce large video files to reclaim storage space ---
> predictably, observably, and without breaking your media library.

------------------------------------------------------------------------

# What Chonk Is

Chonk is **not** a generic transcoder.

It is a controlled storage optimization system that:

-   Finds oversized media files
-   Re-encodes using Intel QSV (HEVC)
-   Validates output
-   Enforces minimum savings thresholds
-   Swaps safely in-place
-   Tracks metrics and per-show savings
-   Prevents reprocessing
-   Runs safely on a schedule

------------------------------------------------------------------------

# Core Features

## Same-Folder Encoding

All encoded files are written next to the source file.\
This prevents cross-device rename failures (EXDEV) common in Docker +
Synology bind mounts.

## Guard Rails

-   Minimum file size threshold
-   Minimum savings percentage requirement
-   Retry with backoff
-   Failed file quarantine (.failed marker)
-   Fail-fast mode
-   Pause system
-   Folder ignore markers

## Operational Controls

-   `.chonkignore` -- Ignore entire folders
-   `.chonkpause` -- Immediately halt processing
-   `.chonkpause.reason` -- Log pause reason
-   `.optimized` -- Prevent reprocessing
-   `.failed` -- Mark permanent failures

## Observability

-   Top candidate preview logging
-   Per-file metrics (before/after/saved/elapsed/rate)
-   Per-show rollups
-   Total run savings
-   Run ID tagging
-   Optional skip logging

------------------------------------------------------------------------

# Processing Flow

1.  Cleanup temp artifacts
2.  Discover candidates
3.  Apply ignore rules
4.  Apply size threshold
5.  Probe source (ffprobe)
6.  Encode (ffmpeg QSV)
7.  Validate output (decode test)
8.  Enforce minimum savings
9.  Backup original
10. Atomic swap
11. Write `.optimized` marker
12. Log metrics + summary

------------------------------------------------------------------------

# Folder Markers

## .chonkignore

Place in a show folder to exclude it entirely:

    /tv_shows/Sports/.chonkignore

## .chonkpause

Place in media root to halt processing:

    /tv_shows/.chonkpause

Optional reason:

    /tv_shows/.chonkpause.reason

## .failed

Automatically created after retry exhaustion:

    Episode.mkv.failed

Prevents repeated failure loops.

------------------------------------------------------------------------

# Environment Variables

## Run Modes

  Variable    Description
  ----------- -----------------------
  DRY_RUN     Log only, no encoding
  PREVIEW     Probe-only mode
  FAIL_FAST   Stop on first failure

## Retry

  Variable             Description
  -------------------- ---------------------
  RETRY_COUNT          Additional attempts
  RETRY_BACKOFF_SECS   Backoff multiplier

## Selection

  Variable         Description
  ---------------- -----------------------
  MIN_SIZE_GB      Minimum file size
  MAX_FILES        Max processed per run
  TOP_CANDIDATES   Log top N by size

## Encoding

  Variable              Description
  --------------------- --------------------------
  QSV_QUALITY           HEVC quality level
  QSV_PRESET            QSV preset
  MIN_SAVINGS_PERCENT   Minimum savings required

## Validation

  Variable               Description
  ---------------------- -------------------
  POST_ENCODE_VALIDATE   Enable validation
  VALIDATE_MODE          decode mode
  VALIDATE_SECONDS       Seconds to test

## Logging & Retention

  Variable             Description
  -------------------- ------------------
  LOG_SKIPS            Log skip reasons
  LOG_RETENTION_DAYS   Log retention
  BAK_RETENTION_DAYS   Backup retention

------------------------------------------------------------------------

# Docker Usage

Run manually:

    docker compose run --rm tv-transcoder
    docker compose run --rm movie-transcoder

Designed for DSM Task Scheduler via wrapper script.

------------------------------------------------------------------------

# Scheduling Model

Recommended approach:

-   TV runs 3 alternating days
-   Movies run 3 alternating days
-   No overlap
-   MAX_FILES=1 for predictable behavior

------------------------------------------------------------------------

# Philosophy

Chonk is not meant to be clever.

It is meant to be:

-   Safe
-   Predictable
-   Observable
-   Storage-focused

Once configured, it should run for years without supervision.

------------------------------------------------------------------------

# Project Structure

-   Modular Python architecture
-   Runner orchestrates pipeline
-   Separate modules for:
    -   discovery
    -   encoding
    -   validation
    -   swap
    -   cleanup
    -   locking
    -   logging

------------------------------------------------------------------------

# Status

Production-grade storage reducer.

------------------------------------------------------------------------

Author: Cory Koenig
